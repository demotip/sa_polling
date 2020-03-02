import json
import os
import gzip
import shutil
import pdb

"""
This module contains functions that read json.
"""


## returns dictionary of field-value pairs


def read_json(file):
    if 'inbound' in file:
        direction = "inbound"
    else:
        direction = "outbound"

    content=[]
    with open(file,'rb') as f:
        for line in f.readlines():
            content.append(json.loads(line))
            # returns a tuple with the file name and its content, both strings
    return content, direction


def write_gjson_into_file(directory):
    count = 1
    unzip_path = ''.join((directory,"\unzipped"))
    zip_path = ''.join((directory,"\zipped"))

    if os.path.isdir(unzip_path):
        #pdb.set_trace()
        # shutil.rmtree(unzip_path)
        # print("removed existing unzipped directory")

        os.mkdir(unzip_path)
        for root, dirs, files in os.walk(zip_path):
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

                        print("%d file(s) unzipped and written successfully\n"%count)
                        count+=1

                else:
                    new_file = ''.join((unzip_path, "\\", file))
                    shutil.copy(file_path,new_file)


                    print("%d file(s) unzipped and written successfully\n"%count)
                    count+=1


        print("%d files written total"%(count-1))
