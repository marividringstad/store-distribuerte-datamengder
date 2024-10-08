##OBS: Brukes ikke lenger!!!

import pandas as pd
import os
from datetime import datetime

# Import your DataFrame containing activity info 
from activity_tables import activity_pandas

# Define the base path to the dataset (you should adjust this to your actual dataset path)
base_path = "/Users/eriksundstrom/store-distribuerte-datamengder/dataset/dataset/Data"

# Initialize a unique id counter for activity_pandas 'id' field
unique_id_counter = 1

# Convert date columns to datetime before the loop (avoid repetitive parsing)
activity_pandas['start_date_time'] = pd.to_datetime(activity_pandas['start_date_time'])
activity_pandas['end_date_time'] = pd.to_datetime(activity_pandas['end_date_time'])

# Prepare a list to accumulate trackpoints instead of appending to a DataFrame inside the loop
trackpoints_list = []

# Iterate over each activity
for index, activity_row in activity_pandas.iterrows():
    activity_id = activity_row['id']
    user_id = activity_row['user_id']
    plt_file = activity_row['plt_file_name']  # Get the .plt file name for the current activity

    # Build the path to the .plt file
    plt_path = os.path.join(base_path, user_id, "Trajectory", plt_file)

    # Print the current activity details for logging purposes (can be removed to speed up)
    print(f"\nProcessing activity {activity_id} for user {user_id}")

    # Get start and end date time for the activity
    start_time = activity_row['start_date_time']
    end_time = activity_row['end_date_time']

    # Check if the PLT file exists
    if os.path.exists(plt_path):
        print(f"File exists: {plt_path}")

        # Open the .plt file and read the trackpoint data
        with open(plt_path, 'r') as file:
            lines = file.readlines()

            # Skip the first 6 lines (header information)
            valid_lines = lines[6:]

            # Iterate through each valid line (each trackpoint)
            for line in valid_lines:
                trackpoint_data = line.strip().split(',')

                # Skip invalid trackpoints (missing lat/lon)
                try:
                    lat = float(trackpoint_data[0])
                    lon = float(trackpoint_data[1])
                except ValueError:
                    continue  # Skip to the next line if lat or lon is invalid

                altitude = int(trackpoint_data[3])
                date_days = float(trackpoint_data[4])  # Date in days since some reference point
                date_time_str = f"{trackpoint_data[5]} {trackpoint_data[6]}"
                date_time = pd.to_datetime(date_time_str)

                if altitude <= -777:
                    altitude = None

                # If the trackpoint falls within the activity time window, add it to the list
                if start_time <= date_time <= end_time:
                    trackpoints_list.append({
                        'id': unique_id_counter,
                        'activity_id': activity_id,
                        'lat': lat,
                        'lon': lon,
                        'altitude': altitude,
                        'date_days': date_days,
                        'date_time': date_time
                    })

                    # Increment the unique ID counter for the next trackpoint
                    unique_id_counter += 1
    else:
        print(f"File does not exist: {plt_path}")

# After collecting all trackpoints, convert the list to a DataFrame
trackpoint_pandas = pd.DataFrame(trackpoints_list)

# Define the path where you want to save the CSV file
csv_output_path = "/Users/eriksundstrom/store-distribuerte-datamengder/cleaned_tables/trackpoints_data.csv"

# Save the DataFrame to a CSV file
trackpoint_pandas.to_csv(csv_output_path, index=False)

# Optional: Print the first few rows of the trackpoint DataFrame for verification
print("\nFinal trackpoint DataFrame:")
print(trackpoint_pandas.head(100))
