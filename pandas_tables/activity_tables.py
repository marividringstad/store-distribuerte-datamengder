import pandas as pd


user_ids = [f'{i:03}' for i in range(182)] 

for id in user_ids: #gjør om så den leser rader i user_pandas-tabellen 
    path = f"/Users/tineaas-jakobsen/Desktop/Skrivebord – Tines MacBook Pro/NTNU/TDT4225 Store Distribuerte Datamengder/Assignment-2/store-distribuerte-datamengder/dataset/dataset/Data/{id}/Trajectory"



activity_pandas = pd.DataFrame({
    'id': xx,
    'user_id': xx,
    'transportation_mode': xx,
    'start_date_time': xx,
    'end_date_time': xx,
})