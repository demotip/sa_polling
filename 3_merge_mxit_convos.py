import pymongo
from pymongo import MongoClient
import csv
import os
import pandas as pd

"""
Some users used both Mxit and ussd when interacting with the system. This script merges the conversation so that they 
are assigned to the same user. Need the list of conversations to merge that is contained in the Merge subdirectory. 
"""

def get_merge_list():
    """
    reads in the csv files that are part of the merge subfolder
    :return:
    """
    outputs = []
    doubles = []
    for afile in os.listdir('./Merge/'):
        x = os.path.basename(afile)
        if x.split('.')[0].split('_')[0] == 'mxits':
            my_file = os.path.join('Merge', afile)
            with open(my_file, 'r') as my_csv:
                lines = csv.reader(my_csv, delimiter=',')
                count = 0
                for line in lines:
                    count += 1
                    outputs.append([line[-1], x.split('.')[0].split('_')[-1]])
                if count == 2:
                    doubles.append(my_file)

    # GET List To merge
    merge_list = pd.DataFrame(outputs)
    merge_list.columns = ['mxit', 'phone_number']
    print("Number of times in merge_list before clean: {}".format(len(merge_list)))

    # clean merge_list of duplicates
    for k, v in merge_list.mxit.iteritems():
        if len(merge_list[merge_list['mxit'] == v]) == 2:
            merge_list = merge_list.drop(k)
    print("Number of times in merge_list before clean: {}".format(len(merge_list)))
    return merge_list


def get_addresses():
    """
    finds the poi of the users whose conversation need sto be merged in the merge directory.
    This directory is generated in the merge sms mxit notebook.
    :return: dataframe with one column addresses (unclear as to why this is not a series)
    """
    output = []
    for afile in os.listdir('./Merge/'):
        x = os.path.basename(afile)
        if x.split('.')[0].split('_')[0] == 'mxits':
            output.append(x.split('.')[0].split('_')[-1])
    df = pd.DataFrame(output)
    df.columns = ['addresses']
    return df


def get_mxit_conversation(address):
    """
    loads the mxit conversations that need to be merged with some ussd counterpart
    :param address: poi of user
    :return: conversation as dataframe
    """
    df = pd.read_csv('./Merge/mxit_convo_{}.csv'.format(address))
    return df[['_id', 'poi']]


def get_ussd_conversation(address):
    """
    loads the conversations from ussd that need to be merged with mxit conversations.
    :param address: poi of user
    :return: conversation under dataframe format
    """
    df = pd.read_csv('./Merge/non_mxit_convo_{}.csv'.format(address))
    return df[['_id', 'poi']]


def deletePreviousMxitConversation(address):
    return None


def merge(merge_list):
    """
    merges the conversations from different platforms together. This function both modifies the mongodb by updating the
    existing conversations and saves a conversation in the old/results
    :param merge_list:
    :return:
    """
    rs = []
    for row in merge_list.iterrows():
        mxit_number = row[1]['mxit']
        phone = row[1]['phone_number']
        print('merging: {} {}'.format(mxit_number, phone))

        # save old
        try:
            c = clean_conversation.find({'poi': mxit_number})
        finally:
            client.close()

        old_mxit_convo = pd.DataFrame(list(c))
        old_mxit_convo.to_csv('./Merge/result/csv/old_mxit_convo_{}.csv'.format(mxit_number))
        old_mxit_convo.to_pickle('./Merge/result/pickles/old_mxit_convo_{}.pkl'.format(mxit_number))

        # update to new
        try:
            results = clean_conversation.update_many({'poi': mxit_number}, {'$set': {'poi': phone}})
        finally:
            client.close()
        print(results.raw_result)
        results_df = pd.DataFrame(results.raw_result, index=[mxit_number])
        results_df.to_pickle('./Merges/results/mxit_result_{}_{}.pkl'.format(mxit_number, phone))

        rs.append(results_df)
    return rs


def main():
    merge_list = get_merge_list()
    rs = merge(merge_list)


client = MongoClient('localhost', 27017)  # local ip and mongo port on remote server
db = client['test']
message_collection = db.get_collection('message_collection')
clean_conversation = db.get_collection('cleaned_conversations')


if __name__ == "__main__":
    main()









