import pandas as pd
import os
from tabulate import tabulate
from datetime import datetime
import sys

"table updated to only include activities with less than 2500 tracpoints in .plt files"

# Import your DataFrame containing user info
from user_tables import users_pandas

# Initialize an empty DataFrame to store the activity data
activity_pandas = pd.DataFrame(columns=['id', 'user_id', 'transportation_mode', 'start_date_time', 'end_date_time', 'plt_file_name'])

# Create the list of user IDs from 000 to 181
user_ids = [f'{i:03}' for i in range(1)]

# Define the base path to the dataset (adjust this to your actual dataset path)
base_path = "/Users/eriksundstrom/store-distribuerte-datamengder/dataset/dataset/Data"

# Initialize a unique id counter for activity_pandas 'id' field
unique_id_counter = 1

# Helper function to parse date and time from the activity files into a consistent format
def parse_datetime_from_activity(date_str, time_str):
    return datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M:%S")

# Helper function to parse date and time from the labels (YYYY/MM/DD format)
def parse_datetime_from_label(date_str, time_str):
    # Convert the date format from 'YYYY/MM/DD' to 'YYYY-MM-DD'
    formatted_date_str = date_str.replace('/', '-')
    # Convert to datetime object
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
                            'transportation_mode': ['NULL'],  # Set to NULL initially; we'll update this later
                            'start_date_time': [start_date_time],
                            'end_date_time': [end_date_time],
                            'plt_file_name': [plt_file] 
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

        # Skip the header line (assuming the first line is the header)
        for label in labels[1:]:  # Start from the second line to skip the header
            # Split the label into start time, end time, and transportation mode
            label_data = label.strip().split()

            # Parse start and end time from the label (with / in the date format)
            label_start_time = parse_datetime_from_label(label_data[0], label_data[1])
            label_end_time = parse_datetime_from_label(label_data[2], label_data[3])
            transportation_mode = label_data[4]

            # Match this label with activity in activity_pandas
            for i, row in activity_pandas[activity_pandas['user_id'] == user_id].iterrows():
                # Parse start and end time from the activity table (with - in the date format)
                row_start_time = parse_datetime_from_activity(row['start_date_time'].split()[0], row['start_date_time'].split()[1])
                row_end_time = parse_datetime_from_activity(row['end_date_time'].split()[0], row['end_date_time'].split()[1])
                
                # Check if the times match exactly
                if row_start_time == label_start_time and row_end_time == label_end_time:
                    # Update the transportation_mode
                    activity_pandas.at[i, 'transportation_mode'] = transportation_mode
                    print(f"Updated transportation mode for user {user_id} activity from {label_start_time} to {label_end_time} to {transportation_mode}")

# Ensure that all dates in the final DataFrame are in the correct format (with -)
activity_pandas['start_date_time'] = pd.to_datetime(activity_pandas['start_date_time']).dt.strftime('%Y-%m-%d %H:%M:%S')
activity_pandas['end_date_time'] = pd.to_datetime(activity_pandas['end_date_time']).dt.strftime('%Y-%m-%d %H:%M:%S')

# Print how many rows have been added to the table
print(f"Total number of rows in activity_pandas: {len(activity_pandas)}")

print("Finished processing. First 20 rows of activity_pandas:")
# Print the first 20 rows using tabulate
print(tabulate(activity_pandas.head(20), headers='keys', tablefmt='psql'))

# Define the path where you want to save the CSV file
csv_output_path = "/Users/eriksundstrom/store-distribuerte-datamengder/cleaned_tables/activity_data.csv"

# Save the DataFrame to a CSV file
activity_pandas.to_csv(csv_output_path, index=False)

print(f"Data successfully saved to {csv_output_path}")


