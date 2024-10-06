import pandas as pd
import os
from tabulate import tabulate
from datetime import datetime


"table updated to only include activitites with less than 2500 tracpoints in .plt files"

# Import your DataFrame containing user info
from user_tables import users_pandas

# Initialize an empty DataFrame to store the activity data
activity_pandas = pd.DataFrame(columns=['id', 'user_id', 'transportation_mode', 'start_date_time', 'end_date_time'])

# Create the list of user IDs from 000 to 181
user_ids = [f'{i:03}' for i in range(182)]

# Define the base path to the dataset (you should adjust this to your actual dataset path)
base_path = "/Users/marividringstad/Desktop/Høst 2024/Store, distribuerte datamengder/store-distribuerte-datamengder/dataset/dataset/Data"

# Initialize a unique id counter for activity_pandas 'id' field
unique_id_counter = 1

# Helper function to parse date and time into a standard format for comparison
def parse_datetime(date_str, time_str):
    return datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M:%S")

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
        # List all .plt files in the Trajectory folder and sort them
        plt_files = sorted([f for f in os.listdir(user_trajectory_path) if f.endswith('.plt')])

        print(f"Found {len(plt_files)} .plt files for user {user_id}")

        # Iterate over the sorted .plt files
        for plt_file in plt_files:
            plt_path = os.path.join(user_trajectory_path, plt_file)

            print(f"Processing file: {plt_file}")
            
            # Open the .plt file and read its contents
            with open(plt_path, 'r') as file:
                lines = file.readlines()

                # Ignore the first 6 lines (based on the PLT format specification)
                valid_lines = lines[6:]

                # Only process the file if it has 2500 or fewer valid lines
                if len(valid_lines) <= 2500:
                    if valid_lines:
                        # Extract the first and last valid data lines
                        first_line = valid_lines[0].strip().split(',')
                        last_line = valid_lines[-1].strip().split(',')

                        # Extract start date and time from the first valid line
                        start_date = first_line[5]  # Date as a string
                        start_time = first_line[6]  # Time as a string
                        start_date_time = f"{start_date} {start_time}"

                        # Extract end date and time from the last valid line
                        end_date = last_line[5]  # Date as a string
                        end_time = last_line[6]  # Time as a string
                        end_date_time = f"{end_date} {end_time}"

                        # Create a new row as a DataFrame
                        new_row = pd.DataFrame({
                            'id': [unique_id_counter],
                            'user_id': [user_id],
                            'transportation_mode': [None],  # Set to None initially; we'll update this later
                            'start_date_time': [start_date_time],
                            'end_date_time': [end_date_time]
                        })

                        # Append the new row to activity_pandas using pd.concat()
                        activity_pandas = pd.concat([activity_pandas, new_row], ignore_index=True)

                        print(f"Added activity for user {user_id} from {start_date_time} to {end_date_time}")
                        
                        # Increment the unique ID counter for the next activity
                        unique_id_counter += 1
                else:
                    print(f"Skipped file {plt_file} for user {user_id} as it has more than 2500 valid lines")

# Now, update transportation_mode for users with has_labels == True
for user_id in users_pandas[users_pandas['has_labels'] == True]['id']:
    user_labels_path = os.path.join(base_path, user_id, "labels.txt")
    
    if os.path.exists(user_labels_path):
        print(f"Processing labels.txt for user {user_id}...")
        
        # Read the labels.txt file
        with open(user_labels_path, 'r') as label_file:
            labels = label_file.readlines()
        
        for label in labels:
            # Split the label into start time, end time, and transportation mode
            label_data = label.strip().split()
            label_start_time = f"{label_data[0]} {label_data[1]}"
            label_end_time = f"{label_data[2]} {label_data[3]}"
            transportation_mode = label_data[4]

            # Match this label with activity in activity_pandas
            for i, row in activity_pandas[activity_pandas['user_id'] == user_id].iterrows():
                if row['start_date_time'] == label_start_time and row['end_date_time'] == label_end_time:
                    # Update the transportation_mode
                    activity_pandas.at[i, 'transportation_mode'] = transportation_mode
                    print(f"Updated transportation mode for user {user_id} activity from {label_start_time} to {label_end_time} to {transportation_mode}")

# Print how many rows have been added to the table
print(f"Total number of rows in activity_pandas: {len(activity_pandas)}")

print("Finished processing. First 20 rows of activity_pandas:")
# Print the first 20 rows using tabulate
print(tabulate(activity_pandas.head(20), headers='keys', tablefmt='psql'))
print('yolo')




