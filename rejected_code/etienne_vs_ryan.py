import pandas as pd

etienne = pd.read_csv(r'C:\Users\Etienne Gagnon\Downloads\cleaned_features_nov22th.csv')
ryan = pd.read_csv(r'C:\Users\Etienne Gagnon\Downloads\cleaned_features_v3.csv')

len(ryan)
len(etienne)

poi_different = list(set(ryan.poi) ^ set(etienne.poi))

ryan_poi = ryan.poi.to_list()
etienne_poi = etienne.poi.to_list()

etienne_not_ryan = set(etienne_poi) - set(ryan_poi)
ryan_not_etienne = set(ryan_poi) - set(etienne_poi)

ussd_etienne_nryan = [poi for poi in etienne_not_ryan if poi[0].isdigit()]
ussd_etienne_nryan = [long(poi) for poi in ussd_etienne_nryan]

new_set = ryan_not_etienne - set(ussd_etienne_nryan)
len(new_set)
type(list(ryan_not_etienne)[-1])
