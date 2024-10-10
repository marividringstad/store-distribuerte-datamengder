import pandas as pd

#file path for .txt with ids for users with labels
file_path = '/Users/marividringstad/Desktop/Høst 2024/Store, distribuerte datamengder/store-distribuerte-datamengder/dataset/dataset/labeled_ids.txt'

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
csv_output_path = "/Users/marividringstad/Desktop/Høst 2024/Store, distribuerte datamengder/store-distribuerte-datamengder/cleaned_tables.csv"


#write dataframe to csv
users_pandas.to_csv(csv_output_path, index=False)

print(f"Data successfully saved to {csv_output_path}")
print('hei')
