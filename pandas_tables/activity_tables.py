import pandas as pd
import os
from tabulate import tabulate
from datetime import datetime

# Import your DataFrame containing user info
from user_tables import users_pandas

# Initialize a list to store the activity data
activity_list = []
# Create the list of user IDs from 000 to 181
user_ids = [f'{i:03}' for i in range(182)]

# Define the base path to the dataset (adjust this to your actual dataset path)
base_path = "/Users/eriksundstrom/store-distribuerte-datamengder/dataset/dataset/Data"

# Initialize a unique id counter for activity_pandas 'id' field
unique_id_counter = 1

# Helper function to parse date and time from the activity files into a consistent format
def parse_datetime_from_activity(date_str, time_str):
    return datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M:%S")

# Helper function to parse date and time from the labels (YYYY/MM/DD format)
def parse_datetime_from_label(date_str, time_str):
    formatted_date_str = date_str.replace('/', '-')
    return datetime.strptime(f"{formatted_date_str} {time_str}", "%Y-%m-%d %H:%M:%S")

print("Starting to process activities...")

# Iterate over the user_ids
for user_id in user_ids:
    print(f"Processing user {user_id}...")
    
    # Determine if the user has labels or not
    user_has_labels = not users_pandas[(users_pandas['id'] == user_id) & (users_pandas['has_labels'] == False)].empty
    
    # Build the path to the user's Trajectory folder
    user_trajectory_path = os.path.join(base_path, user_id, "Trajectory")
    
    # Ensure the folder exists
    if os.path.exists(user_trajectory_path):
        plt_files = sorted([f for f in os.listdir(user_trajectory_path) if f.endswith('.plt')])

        print(f"Found {len(plt_files)} .plt files for user {user_id}")

        for plt_file in plt_files:
            plt_path = os.path.join(user_trajectory_path, plt_file)

            print(f"Processing file: {plt_file}")
            
            with open(plt_path, 'r') as file:
                lines = file.readlines()
                valid_lines = lines[6:]

                if len(valid_lines) <= 2500:
                    if valid_lines:
                        first_line = valid_lines[0].strip().split(',')
                        last_line = valid_lines[-1].strip().split(',')

                        start_date_time = f"{first_line[5]} {first_line[6]}"
                        end_date_time = f"{last_line[5]} {last_line[6]}"

                        # Append the activity data to the list
                        activity_list.append({
                            'id': unique_id_counter,
                            'user_id': user_id,
                            'transportation_mode': 'NULL',
                            'start_date_time': start_date_time,
                            'end_date_time': end_date_time,
                            'plt_file_name': plt_file 
                        })

                        print(f"Added activity for user {user_id} from {start_date_time} to {end_date_time}")
                        
                        unique_id_counter += 1
                else:
                    print(f"Skipped file {plt_file} for user {user_id} as it has more than 2500 valid lines")

# Convert the list to a DataFrame
activity_pandas = pd.DataFrame(activity_list)

# Now, update transportation_mode for users with has_labels == True
for user_id in users_pandas[users_pandas['has_labels'] == True]['id']:
    user_labels_path = os.path.join(base_path, user_id, "labels.txt")
    
    if os.path.exists(user_labels_path):
        print(f"Processing labels.txt for user {user_id}...")
        
        with open(user_labels_path, 'r') as label_file:
            labels = label_file.readlines()

        for label in labels[1:]:
            label_data = label.strip().split()
            label_start_time = parse_datetime_from_label(label_data[0], label_data[1])
            label_end_time = parse_datetime_from_label(label_data[2], label_data[3])
            transportation_mode = label_data[4]

            for i, row in activity_pandas[activity_pandas['user_id'] == user_id].iterrows():
                row_start_time = parse_datetime_from_activity(row['start_date_time'].split()[0], row['start_date_time'].split()[1])
                row_end_time = parse_datetime_from_activity(row['end_date_time'].split()[0], row['end_date_time'].split()[1])
                
                if row_start_time == label_start_time and row_end_time == label_end_time:
                    activity_pandas.at[i, 'transportation_mode'] = transportation_mode
                    print(f"Updated transportation mode for user {user_id} activity from {label_start_time} to {label_end_time} to {transportation_mode}")

# Ensure that all dates are in the correct format
activity_pandas['start_date_time'] = pd.to_datetime(activity_pandas['start_date_time'])
activity_pandas['end_date_time'] = pd.to_datetime(activity_pandas['end_date_time'])

print(f"Total number of rows in activity_pandas: {len(activity_pandas)}")
print(tabulate(activity_pandas.head(20), headers='keys', tablefmt='psql'))

# Save the DataFrame to a CSV file
csv_output_path = "/Users/eriksundstrom/store-distribuerte-datamengder/cleaned_tables/activity_data.csv"
activity_pandas.to_csv(csv_output_path, index=False)

print(f"Data successfully saved to {csv_output_path}")



