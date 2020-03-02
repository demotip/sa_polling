import json
import os, os.path
import sys
import re
#from queue import queue as queue
#import pdb


"""
This module contains functions to analyze the data. The parse function is called from within structure data. The other
functions are mostly called in the analysis.ipynb notebook. 
"""

def parse(file_path):
    """
    This function loads the json line by line into a python list
    :param file_path:
    :return:
    """
    json_arr = []
    with open(file_path, 'rb') as f:
        lines = f.readlines()

        for line in lines:
            json_arr.append(json.loads(line))

    return json_arr


## returns fields in a line of json  --> this was used to validate the data we received
def get_fields(data):

    fields = []
    #print("DATA: ", data)

    ##load json into dict
    data = json.loads(data)

    # return fields as an array  
    return data.keys()



## passing in master array of arrays filled with the fields from each file
def check_fields(array_data):
    check1=True
    check2=True

    ##check if json files contain the same number of fields
    
    # for file in array_data:
    #     if len(array_data[0]) != len(file):
    #         check1 == False
    
    check1 = all([len(file) for file in array_data])
    print("Check 1 was %s"%str(check1))

    ## following only useful to check if each document contains the same number of fields
    if check1:
        master_bool = True

        #check if all fields in every document are the same
        field_comparison = array_data[0]  # first json file

        check2 = all(( array_data[0][ind] in file for file in array_data for ind in range(len(array_data[0]))))
        print("Check 2 was %s"%str(check1))

        # for f in array_data:
        #     for ind in range(len(array_data[0])):
        #         if array_data[0][ind] not in f:
        #             check2=False

    return check1 and check2

def compare_fields(array_data,file_names):

    field_comparison = array_data[0]

    for f in array_data:
        for ind in range(len(array_data[0])):
            if array_data[0][ind] not in f:
                print("Field in question: ", array_data[0][ind])
                print("\n file: ", f)

                #print "The field %s in file %s is not in the file %s. The index in question is %i \n" %(array_data[0][ind],file_names[0],file_names[ind],ind)


def check_time(array_data):
    for root, dirs, files in os.walk(dir_unzipped):
        for file in files:


            print("Reading file %s"%file)
            file_path = ''.join((root,"\\",file))

            if ".gz" in file:
                with gzip.open(file_path,'rb') as f:
                    content = f.read().decode('ascii')

                    ##write to appropriate file
                    #unzip_path = unzip_path+"\\"
                    new_file = ''.join((unzip_path, "\\", file.strip(".gz")))
                    fd = os.open(new_file,os.O_WRONLY|os.O_CREAT)
                    os.write(fd,content)
                    os.close(fd)


