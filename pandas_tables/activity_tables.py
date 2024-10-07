import pandas as pd
import os
from tabulate import tabulate
from datetime import datetime

# Import your DataFrame containing user info
from user_tables import users_pandas

# Initialize an empty DataFrame to store the activity data
activity_pandas = pd.DataFrame(columns=['id', 'user_id', 'transportation_mode', 'start_date_time', 'end_date_time', 'plt_file_name'])

# Create the list of user IDs from 000 to 181
user_ids = [f'{i:03}' for i in range(2)]

# Define the base path to the dataset (you should adjust this to your actual dataset path)
base_path = "/Users/eriksundstrom/store-distribuerte-datamengder/dataset/dataset/Data"

# Initialize a unique id counter for activity_pandas 'id' field
unique_id_counter = 1

# Helper function to parse date and time into a standard format for comparison
def parse_datetime(date_str, time_str):
    return datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M:%S")

# Iterate over the user_ids
for user_id in user_ids:
    # Determine if the user has labels or not
    user_has_labels = not users_pandas[(users_pandas['id'] == user_id) & (users_pandas['has_labels'] == False)].empty

    # Build the path to the user's Trajectory folder
    user_trajectory_path = os.path.join(base_path, user_id, "Trajectory") #inni trajectory-mappen for hver braker
    
    # Ensure the folder exists
    if os.path.exists(user_trajectory_path):
        # List all .plt files in the Trajectory folder and sort them
        plt_files = sorted([f for f in os.listdir(user_trajectory_path) if f.endswith('.plt')]) #sortert liste som ser slik ut: ['3333.plt1', '34532521.plt', ...]

        # Iterate over the sorted .plt files
        for plt_file in plt_files:
            plt_path = os.path.join(user_trajectory_path, plt_file) #nå er vi inni en .plt fil

            # Open the .plt file and read its contents
            with open(plt_path, 'r') as file:
                lines = file.readlines()

                # Ignore the first 6 lines (based on the PLT format specification)
                valid_lines = lines[6:]

                if valid_lines and len(valid_lines) <= 2500:
                    # Extract the first and last valid data lines
                    first_line = valid_lines[0].strip().split(',')
                    last_line = valid_lines[-1].strip().split(',')

                    # Extract start date and time from the first valid line
                    start_date = first_line[5]  # Date as a string
                    start_time = first_line[6]  # Time as a string
                    start_date_time = pd.to_datetime(f"{start_date} {start_time}")

                    # Extract end date and time from the last valid line
                    end_date = last_line[5]  # Date as a string
                    end_time = last_line[6]  # Time as a string
                    end_date_time = pd.to_datetime(f"{end_date} {end_time}")
                    
                    #extracting the plt-file name
                    plt_file_name = plt_file

                    # Create a new row as a DataFrame
                    new_row = pd.DataFrame({
                        'id': [unique_id_counter],
                        'user_id': [user_id],
                        'transportation_mode': [None],  # Set to None initially; we'll update this later
                        'start_date_time': [start_date_time],
                        'end_date_time': [end_date_time],
                        'plt_file_name': [plt_file_name]
                    })

                    # Append the new row to activity_pandas using pd.concat()
                    activity_pandas = pd.concat([activity_pandas, new_row], ignore_index=True)

                    # Increment the unique ID counter for the next activity
                    unique_id_counter += 1

# Now, update transportation_mode for users with has_labels == True
for user_id in users_pandas[users_pandas['has_labels'] == True]['id']:
    user_labels_path = os.path.join(base_path, user_id, "labels.txt")
    
    if os.path.exists(user_labels_path):
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

# Print the first 20 rows using tabulate
#print('testing true and false for labels')
print(tabulate(activity_pandas.tail(20), headers='keys', tablefmt='psql'))
#print('testing rows where id = "20"')
#filtered_rows = activity_pandas[activity_pandas['user_id'] == "020"]  # Filtering the DataFrame
#print(tabulate(filtered_rows, headers='keys', tablefmt='psql'))
