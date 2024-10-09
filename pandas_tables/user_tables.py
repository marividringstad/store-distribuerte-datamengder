import pandas as pd

def get_base_path(username):

    #get correct path
    if username.lower() == 'erik':
        path = "/Users/eriksundstrom/store-distribuerte-datamengder/"
    if username.lower() == 'mari':
        path = "/Users/marividringstad/store-distribuerte-datamengder/" #TODO: mari sjekk at denne er riktig
    if username.lower() == 'tine':
        path = "/Users/tineaas-jakobsen/Desktop/Skrivebord – Tines MacBook Pro/NTNU/TDT4225 Store Distribuerte Datamengder/Assignment-2/store-distribuerte-datamengder/"

    return path

username = input("Who is running this code?")

#file path for .txt with ids for users with labels
base_path = get_base_path(username)

file_path = f"{base_path}dataset/dataset/labeled_ids.txt"

#open file and create set of users with labels
with open(file_path, 'r') as file:
    labeled_ids = set(line.strip() for line in file)

#all ids of users as string on the format 'XXX'
ids = [f'{i:03}' for i in range(182)]
has_labels = [f'{i:03}' in labeled_ids for i in range(182)]  #true if id in labeled_ids, else false

#create dataframe
users_pandas = pd.DataFrame({
    'id': ids,
    'has_labels': has_labels
})

#path for csv file
csv_output_path = f"{base_path}cleaned_tables/users_data.csv"


#write dataframe to csv
users_pandas.to_csv(csv_output_path, index=False)

print(f"Data successfully saved to {csv_output_path}")
