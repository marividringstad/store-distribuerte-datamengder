import pandas as pd
import os
from tabulate import tabulate
from datetime import datetime

# Import your DataFrame containing activity info 
from activity_tables import activity_pandas

# Initialize an empty DataFrame to store the activity data
trackpoint_pandas = pd.DataFrame(columns=['id', 'activity_id', 'lat', 'lon', 'altitude', 'date_days', 'date_time'])

# Define the base path to the dataset (you should adjust this to your actual dataset path)
base_path = "/Users/eriksundstrom/store-distribuerte-datamengder/dataset/dataset/Data"

# Initialize a unique id counter for activity_pandas 'id' field
unique_id_counter = 1

# Helper function to parse date and time into a standard format for comparison
def parse_datetime(date_str, time_str):
    return datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M:%S")

# Iterate over each activity
for index, activity_row in activity_pandas.iterrows():
    activity_id = activity_row['id']
    user_id = activity_row['user_id']
    plt_file = activity_row['plt_file_name']  # Get the .plt file name for the current activity
    
    # Build the path to the .plt file
    plt_path = os.path.join(base_path, user_id, "Trajectory", plt_file)
    
    #get start and end date time for activity
    start_time = pd.to_datetime(activity_row['start_date_time'])
    end_time = pd.to_datetime(activity_row['end_date_time'])
    
    if os.path.exists(plt_path):
        # Open the .plt file and read the trackpoint data
        with open(plt_path, 'r') as file:
            
            lines = file.readlines()
            
            # Skip the first 6 lines (header information)
            valid_lines = lines[6:]
            
            # Iterate through each valid line (each trackpoint)
            for line in valid_lines:
                trackpoint_data = line.strip().split(',')
                lat = float(trackpoint_data[0])
                lon = float(trackpoint_data[1])
                altitude = int(trackpoint_data[3])
                date_days = float(trackpoint_data[4])  # Date in days since some reference point
                date_time_str = f"{trackpoint_data[5]} {trackpoint_data[6]}"
                date_time = pd.to_datetime(date_time_str)
                
                if start_time <= date_time <= end_time:
                    new_trackpoint = pd.DataFrame({
                    'id': [unique_id_counter],
                    'activity_id': [activity_id],
                    'lat': [lat],
                    'lon': [lon],
                    'altitude': [altitude],
                    'date_days': [date_days],
                    'date_time': [date_time]
                })
                
                # Append the new trackpoint row to trackpoint_pandas
                trackpoint_pandas = pd.concat([trackpoint_pandas, new_trackpoint], ignore_index=True)
                
                # Increment the unique ID counter for the next trackpoint
                unique_id_counter += 1
# Optional: Print the first few rows of the trackpoint DataFrame for verification
print(trackpoint_pandas)