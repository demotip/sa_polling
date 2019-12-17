import pandas as pd
import csv
import os

TO_PICKLES = './features/data/'
files = os.listdir(TO_PICKLES)
files = [file for file in files if '.pkl' in file]

example_dictionary = pd.read_pickle(os.path.join(TO_PICKLES, files[0]))

fields = [
    'is_ussd',
    'is_sms',
    'is_mxit',
    'poi',
    'average_response_time',
    'average_response_count',
    'ch1_to_ch2',
    'ch1_to_ch3',
    'ch2_to_ch1',
    'ch2_to_ch3',
    'ch3_to_ch1',
    'ch3_to_ch2',
    'to_address_values',
    'total_number_responses',
    'total_responses_sms',
    'total_responses_ussd',
    'total_sessions_mxit',
    'total_sessions_ussd',
    'total_time_in_system',
    'total_time_interacting',
    'mxit_cities',
    'mxit_countries',
    'mxit_dobs',
    'mxit_genders',
    'mxit_langs',
    'mxit_tariff_plans',
    'most_pinged_city',
    'response_data',
    'transport_types'
]

with open('cleaned_features_nov22th.csv', 'wb') as f:
    writer = csv.DictWriter(f, fieldnames=fields)
    writer.writeheader()
    for file in files:
        conversation = pd.read_pickle(os.path.join(TO_PICKLES, file))
        writer.writerow(conversation)


