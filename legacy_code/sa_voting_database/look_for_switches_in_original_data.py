import pandas as pd


input_file = pd.read_json(r'C:\Users\Etienne Gagnon\Documents\Demotip\sa_polling\SA_Voting_Database\AllData\di_ussd_7692_1_live_app-2014-04-04T15_06_46.656804-stopped-inbound.json', lines=True)
input_file.to_dict(orient='records')