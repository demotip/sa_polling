# %load features/utils.py
import pandas as pd
from pymongo import MongoClient
from datetime import datetime, timedelta
from pymongo.errors import ConnectionFailure
import pickle
from concurrent.futures import ProcessPoolExecutor
import traceback
import os

"""
This script pulls each user's messages from the databases and calculates some features on the basis of their
conversations with the system.

This document outputs a pickle file per conversation in the features subdirectory.

"""
def mongo_connect():
    """
    A function ryan uses to connect to the database.
    """
    # Try to connect to MongoDB,  exit if not successful.
    try:
        conn = MongoClient('localhost', 27017)
        # print "Connected successfully to MongoDB, instance created!"

    except ConnectionFailure:
        print("Could not connect to MongoDB")

    name = 'test'
    db = conn[name]
    conversations = db.clean_conversation_new
    raw = db.conversations_collection
    features = db.features_collection

    return conn, db, conversations, raw, features


def alpha(conversation_df):
    """
    average number of responses given per open session and total number of sessions.
    The input of this is a dataframe outputted by get_conversation.
    """

    total_number_of_responses = len(conversation_df[conversation_df['session_event'] == 'resume'])
    number_of_sessions_ussd = len(conversation_df[
                                     (conversation_df['session_event'] == 'close') &
                                     (conversation_df['transport_type'] == 'ussd')])

    number_of_sessions_mxit = len(conversation_df[(conversation_df['session_event'] == 'close') &
                                                  (conversation_df['transport_type'] == 'mxit')])

    number_of_sessions = number_of_sessions_ussd + number_of_sessions_mxit

    if float(number_of_sessions) != 0:
        response_per_session = float(total_number_of_responses) / float(number_of_sessions)
        return response_per_session, number_of_sessions_ussd, number_of_sessions_mxit
    else:
        return 0, 0, 0


def get_total_time_in_system(df):
    """
    naming convention for the parameter is inconsistent but this requires a dataframe outputted
    by get_conversation() 
    """
    try:
        # start is the first instance we observe a new_connection
        start = df.index[0]
        # finish is the very last report of the system
        finish = df.index[-1]
        delta = finish - start
        return delta.total_seconds()
    except:
        return 0


def beta(conversation_df):
    """
    Returns total time interacting and total time in system. input needs to be the result of conversation df
    """
    total_time_in_system = get_total_time_in_system(conversation_df)
    total_time_interacting = 0.0
    start_time = 0.0
    finish_time = 0.0
    in_session = False
    for rows in conversation_df.iterrows():

        if rows[1]['session_event'] == 'new':
            start_time = rows[0]
            in_session = True

        if rows[1]['session_event'] == 'close' and in_session is True:
            finish_time = rows[0]
            delta = finish_time - start_time
            time_interacting_in_session = delta.total_seconds()
            total_time_interacting += time_interacting_in_session
            in_session = False

    return total_time_interacting, total_time_in_system


def channels(conversation_df):
    """
    documents within USSD if you moved between the free lottery. SHould not apply to mxit users.
    """
    channel_list = conversation_df[(conversation_df['to_addr'] == '*120*7692*2#') |
                                   (conversation_df['to_addr'] == '*120*7692*3#') |
                                   (conversation_df['to_addr'] == '*120*4729#')]['to_addr'].tolist()
    previous = None
    ch1_to_ch2 = False
    ch2_to_ch1 = False
    ch2_to_ch3 = False
    ch3_to_ch2 = False
    ch1_to_ch3 = False
    ch3_to_ch1 = False
    for x in channel_list[1:]:
        next_one = x
        if next_one == previous:
            previous = next_one
            pass
        else:
            # ch1 = '*120*7692*2#' ch2 = '*120*7692*3#' ch3 = '*120*4729#'
            if (previous == '*120*7692*2#' and next_one == '*120*7692*3#'):
                ch1_to_ch2 = True
            if(next_one == '*120*7692*2#' and previous == '*120*7692*3#'):
                ch2_to_ch1 = True
            if (previous == '*120*7692*3#' and next_one == '*120*4729#'):
                ch2_to_ch3 = True
            if (next_one == '*120*7692*3#' and previous == '*120*4729#'):
                ch3_to_ch2 = True
            if (previous == '*120*7692*2#' and next_one == '*120*4729#'):
                ch3_to_ch1 = True
            if (next_one == '*120*7692*2#' and previous == '*120*4729#'):
                ch1_to_ch3 = True
            previous = next_one

    return ch1_to_ch2, ch2_to_ch1, ch2_to_ch3, ch3_to_ch2, ch1_to_ch3, ch3_to_ch1


def get_average_response_time(conversation_df):
    """
    Returns the time a user takes to respond on average.

    modified by Etienne to get more information with regards to the responses in the response data object.
    :param conversation_df:
    :return:
    """
    questions = conversation_df[conversation_df['from_addr'] != conversation_df['poi']]
    questions_loc = [conversation_df.index.get_loc(loc) for loc in questions.index.tolist()]
    potential_answers_loc = [conversation_df.index.get_loc(loc) + 1 for loc in questions.index.tolist()]
    cleaned_questions = list(set(questions_loc) - set(potential_answers_loc))

    if len(conversation_df) - 1 in cleaned_questions:
        cleaned_questions.remove(len(conversation_df) - 1)

    cleaned_answers = [q + 1 for q in cleaned_questions]
    times = []

    for q, a in zip(cleaned_questions, cleaned_answers):
        r1 = conversation_df.iloc[q]
        r2 = conversation_df.iloc[a]
        r1_date = r1.name
        r2_date = r2.name
        times.append([(r2_date - r1_date).total_seconds(), r1.content, r2.content, r2.session_event, r1.transport_type])

    df = pd.DataFrame(times)
    if len(times) == 0:
        return 0, df
    else:

        df.columns = ['times', 'question', 'answer', 'session_event', 'transport_type']
        return df.times.mean(), df


def addresses_and_types(conversation_df):
    """
    identifies which addresses and transport types are found in the conversation. I am unclear as to what exception the
    global except clause tries to prevent but am leaving it there.
    :param conversation_df:
    :return:
    """
    addresses = conversation_df['to_addr'].unique().tolist()
    invalid_to_addresses = [conversation_df.poi.iloc[0], '', ' ', 'None']
    for invalid in invalid_to_addresses:
        try:
            addresses.remove(invalid)
        except:
            pass
    return addresses, conversation_df['transport_type'].unique().tolist()


def find_transport_type(conversation_df):
    """
    finds whether the user sent messages with ussd, sms or mxit.
    :param conversation_df:
    :return:
    """
    transport_type = conversation_df.transport_type
    mxit = any(transport_type == 'mxit')
    sms = any(transport_type == 'sms')
    ussd = any(transport_type == 'ussd')

    return mxit, sms, ussd


def find_number_of_responses(conversation_df, poi):
    """
    finds the number of responses written by
    """

    conversation_df = conversation_df.dropna(subset=['from_addr'])
    print(len(conversation_df))
    user_msg = conversation_df[conversation_df.from_addr.str.contains(str(poi), regex=False)]
    user_msg = user_msg[~user_msg.content.isnull()]
    user_msg = user_msg[~user_msg.content.str.contains('timeout')]

    mxit_msg = user_msg[user_msg.transport_type == 'mxit']
    sms_msg = user_msg[user_msg.transport_type == 'sms']
    ussd_msg = user_msg[user_msg.transport_type == 'ussd']

    total_number_of_responses = len(user_msg)
    number_of_mxit_msg = len(mxit_msg)
    number_of_sms_msg = len(sms_msg)
    number_of_ussd_msg = len(ussd_msg)

    return total_number_of_responses, number_of_mxit_msg, number_of_sms_msg, number_of_ussd_msg


def make_payload(conversation_df):
    """
    creates some of the required feature from a get_conversation output. returns a line per output
    Modified by Etienne to add the session_event information.

    This function is missing some of the features that are present in the cleaned_features_v3 file. We believe this is
    due to Ryan not pushing some of his code.
    Etienne made some extra functions that generate these missing features (see make_pickle_one_conversation function)
    """
    address = conversation_df['poi'][0]
    average_response, number_of_session_ussd, number_of_session_mxit = alpha(conversation_df)
    total_time_interacting, total_time_in_system = beta(conversation_df)
    ch1_to_ch2, ch2_to_ch1, ch2_to_ch3, ch3_to_ch2, ch1_to_ch3, ch3_to_ch1 = channels(conversation_df)
    average_response_time, response_df = get_average_response_time(conversation_df)
    addresses, message_types = addresses_and_types(conversation_df)
    mxit, sms, ussd = find_transport_type(conversation_df)

    nb_responses, nb_mxit_responses, nb_sms_responses, nb_ussd_responses = \
        find_number_of_responses(conversation_df, address)

    payload = {
        'poi': address,
        'average_response_count': average_response,
        'total_sessions_mxit': number_of_session_mxit,
        'total_sessions_ussd': number_of_session_ussd,
        'total_time_interacting': total_time_interacting,
        'total_time_in_system': total_time_in_system,
        'ch1_to_ch2': ch1_to_ch2,
        'ch2_to_ch1': ch2_to_ch1,
        'ch2_to_ch3': ch2_to_ch3,
        'ch3_to_ch2': ch3_to_ch2,
        'ch1_to_ch3': ch1_to_ch3,
        'ch3_to_ch1': ch3_to_ch1,
        'to_address_values': addresses,
        'transport_types': message_types,
        'average_response_time': average_response_time,
        'is_mxit': mxit,
        'is_ussd': ussd,
        'is_sms': sms,
        'total_number_responses': nb_responses,
        'total_responses_sms': nb_sms_responses,
        'total_responses_ussd': nb_ussd_responses,
        'total_responses_mxit': nb_mxit_responses,
        'response_data': response_df.to_dict(orient='records'),
    }

    return payload


def setup_conversation(conversation_df):
    """
    Sets the index of a df outputted by get_conversation() to the timestamp of messages and removes duplicates.
    """
    header = ['poi', 'content', 'from_addr', 'to_addr', 'transport_type', 'transport_name', 'session_event', '_id']
    conversation_df = conversation_df.set_index('timestamp')
    # we drop duplicates of timestamp which is the index
    df = conversation_df[~conversation_df.index.duplicated(keep='first')]
    return df[header]


def get_addresses():
    """
    returns the list of distinct poi found in the mongo database
    :return:
    """
    conn, db, conversations, raw, features = mongo_connect()
    try:
        c = conversations.distinct('poi')
    finally:
        conn.close()

    return list(c)


def get_conversation(poi):
    """
    creates a dataframe for the messages of one user found in the database. Applied iteratively to all unique addresses
    to obtain full converstations.
    :param poi:
    :return:
    """
    conn, db, conversations, raw, features = mongo_connect()
    try:
        c = conversations.find({'poi': poi})
    finally:
        conn.close()
    df = pd.DataFrame(list(c))
    header = ['poi', 'content', 'from_addr', 'to_addr', 'transport_type', 'transport_name', 'session_event', '_id',
              'helper_metadata']
    df = df.set_index('timestamp')
    # we drop duplicates of timestamp which is the index
    df = df[~df.index.duplicated(keep='first')]
    return df[header]


def fix_mxit_session(conversation_df):
    """
    function made by Etienne to restore the session info for mxit conversations, which is not registered by mxit.
    heuristically mimics the session info from the ussd conversations.
    A session is considered to be "new" if it is the first interaction between the user and the system in 2 minutes.
    The first message is always taken to be a new session.
    iterrows is slow but conversation_dfs are small.

    :param conversation_df: needs to be sorted by timestamp
    :return:
    """
    def generate_session_timeout(conversation_df_row):
        """
        adds a session_timeout interaction to the mxit conversations when the user goes inactive. This is to standardize
        mxit conversations w.r.t the ussd conversations
        :param conversation_df_row:
        :return:
        """
        session_timeout = conversation_df_row.copy()
        new_timestamp = conversation_df_row.name + timedelta(seconds=1)
        session_timeout.name = new_timestamp
        session_timeout['content'] = 'Session timeout.'
        session_timeout['session_event'] = 'close'
        session_timeout['_id'] = 'artificial_message'
        session_timeout['transport_type'] = 'mxit'

        return session_timeout

    try:
        first_row = conversation_df.iloc[0, :]

        for message in conversation_df.loc[:, ['session_event', 'from_addr', 'poi', 'content']].iterrows():
            if (message[1]['content'] is None) & (message[1]['from_addr'] == message[1]['poi']):
                conversation_df.loc[message[0], 'session_event'] = 'new'
            elif (message[1]['content'] is not None) & (message[1]['from_addr'] == message[1]['poi']):
                conversation_df.loc[message[0], 'session_event'] = 'resume'
            else:
                conversation_df.loc[message[0], 'session_event'] = 'None'

        added_new_sessions = conversation_df[conversation_df.loc[:, 'session_event'] == 'new']
        added_new_sessions = added_new_sessions.drop(added_new_sessions.iloc[0, :].name)
        replace_timestamp = conversation_df[
            (conversation_df.loc[:, 'session_event'] == 'new').shift(-1).fillna(False)].index
        added_new_sessions.index = replace_timestamp
        # This timestamp will generate observations before the last message of a session.

        session_timeout_message = [generate_session_timeout(new_session[1]) for new_session in added_new_sessions.iterrows()]

        for timeout_message in session_timeout_message:
            conversation_df = conversation_df.append(timeout_message)

        conversation_df.sort_index(inplace=True)
        conversation_df.loc[first_row.name, 'session_event'] = 'new'

        final_event = generate_session_timeout(conversation_df.iloc[-1, :])
        final_event.name = conversation_df.iloc[-1, :].name + timedelta(seconds=1)
        conversation_df = conversation_df.append(final_event)
    except:
        print(traceback.format_exc())

    return conversation_df


def index_to_date(conversation_df_index):
    """
    turns the index from a unicode string to datetime format
    :param conversation_df:
    :return:
    """
    datetime_index = [
        datetime.strptime(index_value, TIME_STAMP_FORMAT) for index_value in conversation_df_index]
    return datetime_index


def find_mxit_specific_variables(conversation_df):
    """
    function written by Etienne to find the mxit specific variables found in the cleaned_features_v3 csv file.
    inelegant but will get the job done.
    returns empty lists for ussd users or users with no mxit metadata to be consistent with ryan's files.
    :param poi:
    :return:
    """
    user_info_available = False
    for info in conversation_df.helper_metadata:
        try:
            user_info = info['mxit_info']
            user_info_available = True
            break
        except KeyError:
            pass

    if not user_info_available:
        mxit_variables = {'mxit_cities': [],
                          'mxit_countries': [],
                          'mxit_dobs': [],
                          'mxit_genders': [],
                          'mxit_langs': [],
                          'mxit_tariff_plans': [],
                          'most_pinged_city': []}

    else:
        mxit_variables = {'mxit_cities': user_info[u'X-Mxit-Location'][u'city'],
                          'mxit_countries': user_info[u'X-Mxit-Location'][u'country_code'],
                          'mxit_dobs': user_info[u'X-Mxit-Profile'][u'date_of_birth'],
                          'mxit_genders': user_info[u'X-Mxit-Profile'][u'gender'],
                          'mxit_langs': user_info[u'X-Mxit-Profile'][u'language_code'],
                          'mxit_tariff_plans': user_info[u'X-Mxit-Profile'][u'tariff_plan'],
                          'most_pinged_city': []}

    return mxit_variables


def make_pickle_one_conversation(poi):
    """
    made by Etienne
    feed into multiprocessed executor to generate features
    :param poi:
    :return:
    """
    try:
        conv = get_conversation(poi)
        try:
            conv.index = index_to_date(conv.index)
        except TypeError:
            pass
        conv.sort_index(inplace=True)

        is_mexit = any(conv.transport_type == 'mxit')
        print(is_mexit)

        try:
            if is_mexit:
                conv = fix_mxit_session(conv)

            ryan_payload = make_payload(conv)
            ryan_payload.update(find_mxit_specific_variables(conv))
            pickle.dump(ryan_payload, open("./features/data/out_{}.pkl".format(poi), "wb"))
            print('one conversation')
        except AttributeError:
            print(traceback.format_exc())
            print('empty conversation')
            pass
    except:
        print(traceback.format_exc())


def remove_already_processed_addresses(address_list):
    """
    use this function if there has been a crash and you want to relaunch without reprocessing conversations
    :return:
    """
    to_pickles = './features/data/'
    files = os.listdir(to_pickles)
    files = [file for file in files if '.pkl' in file]
    files = [file.replace('out_', '') for file in files]
    files = [file.replace('.pkl', '') for file in files]

    all_pois = set(address_list)
    processed_pois = set(files)
    remaining_pois = all_pois - processed_pois

    print(f'there are {len(all_pois)} total users')
    print(f'there hare {len(remaining_pois)} users that have not been processed yet')

    return list(remaining_pois)


if __name__ == "__main__":
    conn, db, conversations, raw, features = mongo_connect()

    TIME_STAMP_FORMAT = "%Y-%m-%d %H:%M:%S.%f"

    addresses = get_addresses()
    # uncomment the following function if there has been a crash while this script was running.
    # addresses = remove_already_processed_addresses(addresses)

    print('found addresses')
    print(len(addresses))

    with ProcessPoolExecutor() as executor:
        executor.map(make_pickle_one_conversation, addresses)

    print('conversations processed, ready to load into csv')



