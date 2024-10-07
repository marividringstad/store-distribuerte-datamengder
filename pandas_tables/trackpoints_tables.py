import pandas as pd
import os
from datetime import datetime
# Initialize an empty DataFrame for trackpoints
trackpoint_pandas = pd.DataFrame(columns=['id', 'activity_id', 'lat', 'lon', 'altitude', 'date_days', 'date_time'])
# Initialize an ID counter for the trackpoints and activity IDs
trackpoint_id_counter = 1
activity_id_counter = 1
# Iterate over the user IDs (assuming user IDs are the folders from 000 to 181)
user_ids = [f'{i:03}' for i in range(182)]
# Define the base path to the dataset (you should adjust this to your actual dataset path)
base_path = "/Users/marividringstad/Desktop/Høst 2024/Store, distribuerte datamengder/store-distribuerte-datamengder/dataset/dataset/Data"
print("Starting to process trackpoints...")
# Iterate over each user folder
for user_id in user_ids:
    user_trajectory_path = os.path.join(base_path, user_id, "Trajectory")
    
    # Ensure the Trajectory folder exists
    if os.path.exists(user_trajectory_path):
        # List all .plt files in the Trajectory folder
        plt_files = sorted([f for f in os.listdir(user_trajectory_path) if f.endswith('.plt')])
        # Iterate over each .plt file
        for plt_file in plt_files:
            plt_path = os.path.join(user_trajectory_path, plt_file)
            # Open the .plt file and read its contents
            with open(plt_path, 'r') as file:
                lines = file.readlines()
                # Ignore the first 6 lines (based on the PLT format specification)
                valid_lines = lines[6:]
                # Only process the .plt file if it has 2500 or fewer valid lines
                if len(valid_lines) <= 2500:
                    # Iterate over each valid line (each line corresponds to a trackpoint)
                    for line in valid_lines:
                        trackpoint_data = line.strip().split(',')
                        lat = float(trackpoint_data[0])
                        lon = float(trackpoint_data[1])
                        altitude = int(trackpoint_data[3])
                        date_days = float(trackpoint_data[4])
                        date_str = trackpoint_data[5]  # Date as a string
                        time_str = trackpoint_data[6]  # Time as a string
                        date_time = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M:%S")
                        # Create a new row for the trackpoint
                        new_trackpoint = pd.DataFrame({
                            'id': [trackpoint_id_counter],
                            'activity_id': [activity_id_counter],  # Use the same activity_id for all trackpoints in this file
                            'lat': [lat],
                            'lon': [lon],
                            'altitude': [altitude],
                            'date_days': [date_days],
                            'date_time': [date_time]
                        })
                        # Append the new trackpoint to trackpoint_pandas DataFrame
                        trackpoint_pandas = pd.concat([trackpoint_pandas, new_trackpoint], ignore_index=True)
                        # Increment the trackpoint ID counter for the next trackpoint
                        trackpoint_id_counter += 1
                    print(f"Added trackpoints for activity {activity_id_counter} from file: {plt_file}")
                    # Increment the activity_id_counter for the next activity (next .plt file)
                    activity_id_counter += 1
                else:
                    print(f"Skipped file {plt_file} for user {user_id} as it has more than 2500 valid lines")
print(f"Total number of trackpoints: {len(trackpoint_pandas)}")
# Print the first 10 rows of the trackpoint_pandas DataFrame
print(trackpoint_pandas.head(10))


