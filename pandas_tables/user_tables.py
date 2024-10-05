import pandas as pd

# Step 1: Read the contents of labeled_id.txt from your provided file path
file_path = '/Users/tineaas-jakobsen/Desktop/Skrivebord – Tines MacBook Pro/NTNU/TDT4225 Store Distribuerte Datamengder/Assignment-2/store-distribuerte-datamengder/dataset/dataset/labeled_ids.txt'

with open(file_path, 'r') as file:
    labeled_ids = set(line.strip() for line in file)

# Step 2: Create a DataFrame
numbers = [f'{i:03}' for i in range(182)]  # Strings from 0 to 181
boolean_values = [f'{i:03}' in labeled_ids for i in range(182)]  # True if number in labeled_ids, else False

# Step 3: Create a pandas DataFrame
users_pandas = pd.DataFrame({
    'id': numbers,
    'has_labels': boolean_values
})

# Display the DataFrame
print(users_pandas)

true_count = users_pandas['has_labels'].sum()

# Display the count
print(f"Number of True values: {true_count}")