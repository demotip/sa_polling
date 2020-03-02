# South Africa Polling Analysis

This repository contains code used to create a set of variables used in Aaron's book project on digital participation in South Africa. The variables are informative with regards to the amount of time spent by people on Aaron's platform and how often they interact with it. 

Part of the code in this project was written by Ryan Sampana in 2017. Several issues discovered while reproducing the files forced Etienne Gagnon (etgagnon@icloud.com) to rewrite most of the code found in this repository in 2019-2020. The original version of this repository can be found under the branch "ryan version". Contact Etienne Gagnon if having any issue running this. 

Amongst the most important issues in the original files: 

- The code used to load the data only loaded 1/3rd of the data. 
- The function used to calculate time variables in the original code did not give the appropriate results. 
- The code was written in stream of consciousness jupyter notebooks which made them difficult to understand. 

One should keep these issues in mind when looking at the original files. The notebooks are kept in the legacy_code subfolder. The output of this project is analyzed by scripts contained in the pk_main repository on Aaron's github. 

## Explanation of scripts

Here is an explanation of the various scripts:

- 1_database_loader: This file replaces a set of scripts that were contained on the usb key where Aaron keeps the original data. The file takes the in the data in JSON format and loads them into a mongo database. Running this file requires a computer with AT LEAST 16gb of ram otherwise it will fail to run. Each mongo object loaded into the database is an individual message from the user to the system or the system to the user. 

- 2_clean_conversation: Most of Ryan's code relies on using a user's POI to retrieve their message. A POI is a user's phone number, which is treated as a unique id in this project. The POI is not included as a field in the original JSON. It comes from the values found in the "from_addr" field from the original JSON. This file reads in all of the messages in the database, cleans up the phone number so that they appear in a regular format, adds the phone number as a POI and reloads them into a new database on mongo titled "clean_conversation_new". This script runs in roughly 2 days when multiprocessed on Aaron's compute canada cloud computer. 

- 3_merge_mxit_convos: Some users used both Mxit and ussd when interacting with the system. This script merges the conversation so that they are assigned to the same user. To run this script ones needs the list of conversations to merge that is contained in the Merge subdirectory. The database on Mongo is updated and the merged files are saved in the Merge subfolder.

- 4_final_feature_creation: This is the workhorse script that generates the variables that Aaron is interested in. The script reads in the messages found in the database, groups all messages from one user into a conversation and computes variables related to the number of messages and the time at which they were sent. The output of this script is saved in the features/data subdirectory. This script takes over a week to run when multi-processed on Aaron's compute Canada cloud. 

- 5_make_feature_csv: This file reads in the data saved in the previous script and writes it as a csv. 

## Mxit vs USSD

In the study some users used a platform called Mxit and other users a platform called USSD when interacting with the system. Mxit is a social media platform that was popular in South Africa at the time while USSD is a text messaging service. Mxit users have extra meta-data associated with their accounts but our informations are less precise with regards to the timing of their logins when compared to USSD users.  

## Using Mongo and running this project

To be able to properly use mongo one needs to have it running as a background process when running the scripts. On Aaron's compute Canada cluster this can be done like this: 

```bash
screen
Mongod
# press ctrl a-d to escape the screen.
```

Screen is a command that allows to run processes on detachable interface. When pressing ctrl a-d the user escapes the screen and is not locked down by the interface processing information. 

The python scripts can then be run like this: 

```bash
screen
python <script_to_run.py>
# press crtl a-d
```

again the use of screen allows the user to logout from Aaron's compute canada cloud without interrupting the script. Since running this whole project can take up to 2 weeks I highly recommend the use of screen when running the scripts. 

To access the mongo database directly one can use the following commands: 

```bash
screen 
mongod
# press ctrl a-d
mongo 
```

The mongo database is interacted with using a specific syntax. I suggest reading the documentation found on this site: https://docs.mongodb.com/manual/tutorial/query-documents/

