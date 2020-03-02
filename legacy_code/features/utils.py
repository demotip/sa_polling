import pandas as pd
from datetime import datetime
from features.settings import TIME_STAMP_FORMAT
import pickle


def get_conversation(conversations, poi):
    c = conversations.find({'poi': poi})
    df = pd.DataFrame(list(c))
    header = ['poi', 'content', 'from_addr', 'to_addr', 'transport_type', 'transport_name', 'session_event', '_id']
    df = df.set_index('timestamp')
    # we drop duplicates of timestamp which is the index
    df = df[~df.index.duplicated(keep='first')]
    return df[header]


def count_mxit_responses(conversation_df):
    try:
        count = len(conversation_df[(conversation_df['transport_type'] == 'mxit') & (conversation_df['content'] != None) & (conversation_df['to_addr'] == 'vipvoice2014')])
    except:
        count = 0
    return count


def number_of_mxit_sessions(conversation_df):
    if 'mxit' in conversation_df['transport_type'].unique():
        return 1
    else:
        return 0


def alpha(conversation_df):
    """average number of responses given per open session and total number of sessions"""
    total_number_of_responses = len(conversation_df[conversation_df['session_event']=='resume'])
    number_of_sessions_sms = len(conversation_df[conversation_df['session_event']=='close'])
    total_number_of_responses += count_mxit_responses(conversation_df)
    number_of_sessions = number_of_sessions_sms + number_of_mxit_sessions(conversation_df)
    if float(number_of_sessions) != 0:
        return float(total_number_of_responses)/float(number_of_sessions), float(number_of_sessions)
    else:
        return 0, 0


def get_total_time_in_system(df):
    try:
        # start is the first instance we observe a new_connection
        start = datetime.strptime(df[df['session_event'] == 'new'].index[0], TIME_STAMP_FORMAT)
        # finish is the very last report of the system
        finish = datetime.strptime(df.index[-1], TIME_STAMP_FORMAT)
        delta = finish - start
        return delta.seconds
    except:
        return 0


def beta(conversation_df):
    """Returns total time interacting and total time in system """
    total_time_in_system = get_total_time_in_system(conversation_df)
    total_time_interacting = 0.0
    start_time = 0.0
    finish_time = 0.0
    in_session = False
    for rows in conversation_df.iterrows():

        if rows[1]['session_event'] == 'new':
            start_time = datetime.strptime(rows[0], TIME_STAMP_FORMAT)
            in_session = True

        if rows[1]['session_event'] == 'close' and in_session is True:
            finish_time = datetime.strptime(rows[0], TIME_STAMP_FORMAT)
            delta = finish_time - start_time
            time_interacting_in_session = delta.seconds
            total_time_interacting += delta.seconds
            in_session = False

    return total_time_interacting, total_time_in_system


def channels(conversation_df):
    channel_list = conversation_df[(conversation_df['to_addr'] == '*120*7692*2#') | (conversation_df['to_addr'] == '*120*7692*3#') | (conversation_df['to_addr'] == '*120*4729#')]['to_addr'].tolist()
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
    questions = conversation_df[conversation_df['from_addr'] != conversation_df['poi']]
    questions_loc = [conversation_df.index.get_loc(loc) for loc in questions.index.tolist()]
    potential_answers_loc = [conversation_df.index.get_loc(loc) + 1 for loc in questions.index.tolist()]
    cleaned_questions = list(set(questions_loc) - set(potential_answers_loc))
    cleaned_answers = [q + 1 for q in cleaned_questions]

    if len(conversation_df) - 1 in cleaned_questions:
        cleaned_questions.remove(len(conversation_df) - 1)

    cleaned_answers = [q + 1 for q in cleaned_questions]
    times = []

    for q, a in zip(cleaned_questions, cleaned_answers):
        r1 = conversation_df.iloc[q]
        r2 = conversation_df.iloc[a]
        r1_date = datetime.strptime(r1.name, TIME_STAMP_FORMAT)
        r2_date = datetime.strptime(r2.name, TIME_STAMP_FORMAT)
        times.append([(r2_date - r1_date).total_seconds(), r1.content, r2.content])

    df = pd.DataFrame(times)
    if len(times) == 0:
        return 0, df
    else:

        df.columns = ['times', 'question', 'answer']
        df.times.mean()
        return df.times.mean(), df


def addresses_and_types(conversation_df):
    addresses = conversation_df['to_addr'].unique().tolist()
    invalid_to_addresses = [conversation_df.poi.iloc[0], '', ' ', 'None']
    for invalid in invalid_to_addresses:
        try:
            addresses.remove(invalid)
        except:
            pass
    return addresses, conversation_df['transport_type'].unique().tolist()


def make_payload(conversation_df):
    address = conversation_df['poi'][0]
    average_response, total_number_of_sessions = alpha(conversation_df)
    total_time_interacting, total_time_in_system = beta(conversation_df)
    ch1_to_ch2, ch2_to_ch1, ch2_to_ch3, ch3_to_ch2, ch1_to_ch3, ch3_to_ch1 = channels(conversation_df)
    average_response_time, response_df = get_average_response_time(conversation_df)
    addresses, message_types = addresses_and_types(conversation_df)

    payload = {
        'poi': address,
        'average_response_count': average_response,
        'total_number_of_sessions': total_number_of_sessions,
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
        'response_data': response_df.to_dict(orient='records'),
    }
    pickle.dump(payload, open("./features/data/out_{}.pkl".format(address), "wb"))
    return payload


def setup_conversation(conversation_df):
    header = ['poi', 'content', 'from_addr', 'to_addr', 'transport_type', 'transport_name', 'session_event', '_id']
    conversation_df = conversation_df.set_index('timestamp')
    # we drop duplicates of timestamp which is the index
    df = conversation_df[~conversation_df.index.duplicated(keep='first')]
    return df[header]
