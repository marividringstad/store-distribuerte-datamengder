import pandas as pd
import os
from tabulate import tabulate
from datetime import datetime


#Most revent trackpoint table 
#TODO: add trackpoints_list = [] outside the user loop to make a general list that is written into a pandas fram
#TODO: do the same for activities

# Import your DataFrame containing user info
from user_tables import users_pandas

# Initialize an empty DataFrame to store the activity data


# Create the list of user IDs from 000 to 181
user_ids = [f'{i:03}' for i in range(182)]
#user_ids = ['045','046']

# Define the base path to the dataset (adjust this to your actual dataset path)
base_path = "/Users/marividringstad/Desktop/Høst 2024/Store, distribuerte datamengder/store-distribuerte-datamengder/dataset/dataset/Data"

# Initialize a unique id counter for activity_pandas 'id' field
unique_id_counter = 1
unique_id_trackpoints = 1

# Helper function to parse date and time from the labels (YYYY/MM/DD format)
def parse_datetime_from_label(date_str, time_str):
    formatted_date_str = date_str.replace('/', '-')  # Convert to 'YYYY-MM-DD'
    return datetime.strptime(f"{formatted_date_str} {time_str}", "%Y-%m-%d %H:%M:%S")


def get_all_times_plt(trackpoints): #tar in valid_lines
    all_date_times =[]
    for i in range(len(trackpoints)):
        trackpoint = trackpoints[i].strip().split(',')
        date = trackpoint[5]
        time = trackpoint[6]
        date_time = f"{date} {time}"
        trackpoint_start_datetime = datetime.strptime(date_time, '%Y-%m-%d %H:%M:%S')
        all_date_times.append(trackpoint_start_datetime)
    return all_date_times

def get_trackponit_info(trackpoint):
    lat = float(trackpoint[0])
    lon = float(trackpoint[1])
    altitude = trackpoint[3]
    date_days = trackpoint[4]
    date = trackpoint[5]
    time = trackpoint[6]
    #format now looks like this: [datetime.datetime(2009, 1, 15, 22, 1, 45)], needs to be changed
    date_time_trackpoint = parse_datetime_from_label(date, time)
    return lat, lon, altitude, date_days, date_time_trackpoint

print("Starting to process activities...")

activity_list_all = []
trackpoints_list_all =[]

# Iterate over the user_ids
for user_id in user_ids:
    trackpoints_list =[]
    print(f"Processing user {user_id}...")
    
    # Build the path to the user's Trajectory folder
    user_trajectory_path = os.path.join(base_path, user_id, "Trajectory")
    # Path to the user's labels.txt file
    
    labels =[]

    #hent labels hvis bruker har labels
    if not users_pandas[(users_pandas['id'] == user_id) & (users_pandas['has_labels'] == True)].empty:  
        user_labels_path = os.path.join(base_path, user_id, "labels.txt")           
        if os.path.exists(user_labels_path):
            print(f"Processing labels.txt for user {user_id}...")

            # Read the labels.txt file
            with open(user_labels_path, 'r') as label_file:
                labels = label_file.readlines()

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
                        start_date = first_line[5]
                        start_time = first_line[6]
                        start_date_time = f"{start_date} {start_time}"

                        # Extract end date and time from the last valid line
                        end_date = last_line[5]
                        end_time = last_line[6]
                        end_date_time = f"{end_date} {end_time}"

                        transportation_mode = 'NULL'  # Default

                        # Check if the user has labels
                        if len(labels) >= 0.5:
                            all_date_times = get_all_times_plt(valid_lines) #legg valid lines rett inn

                            # Process each label entry (skip the header)
                            for label in labels[1:]:
                                label_data = label.strip().split()

                                # Parse start and end times from the label
                                label_start_time = parse_datetime_from_label(label_data[0], label_data[1])
                                label_end_time = parse_datetime_from_label(label_data[2], label_data[3])
                                label_transportation_mode = label_data[4]


                                # Check if times match exactly or overlap
                                if label_start_time in all_date_times  and label_end_time in all_date_times:
                                    transportation_mode = label_transportation_mode
                                    start_date_time = label_start_time
                                    end_date_time = label_end_time

                                    #oppdatere valid lines
                                    start_time_index= all_date_times.index(label_start_time)
                                    end_time_index= all_date_times.index(label_end_time)
                                    valid_lines = valid_lines[start_time_index : end_time_index]
                                    break  # Use the first matching label

                        # Create a new row as a DataFrame
                        new_activity = {
                            'id': unique_id_counter,
                            'user_id': user_id,
                            'transportation_mode': transportation_mode,
                            'start_date_time': start_date_time,
                            'end_date_time': end_date_time
                        }
                        # Append the new row to activity_pandas using pd.concat()
                        activity_list_all.append(new_activity)
                        # Increment the unique ID counter for the next activity
                        
                        

                        print(f"Added activity for user {user_id} from {start_date_time} to {end_date_time}")   

                        #Legge til trajectory
                        #oppdatere valid lines slik at det bare er linjene som er innenfor start og slutt som er med 
                        for i in range(len(valid_lines)):
                            trackpoint = valid_lines[i].strip().split(',')
                            lat, lon, altitude, date_days, date_time_trackpoint = get_trackponit_info(trackpoint)

                            new_trackpoint = {
                                'id': int(unique_id_trackpoints),
                                'activity_id': int(unique_id_counter),
                                'lat': float(lat),
                                'lon': float(lon),
                                'altitude':float(altitude),
                                'date_days': date_days, #sjekk denne
                                'date_time': date_time_trackpoint
                            }   
                            
                            trackpoints_list_all.append(new_trackpoint)
                            
                        
                            unique_id_trackpoints +=1
                            
                        unique_id_counter += 1     
                else:
                    print(f"Skipped file {plt_file} for user {user_id} as it has more than 2500 valid lines")

                
    
    print(f"Added all trackpoint for user {user_id} for all activities") 
    
#activity_pandas = pd.DataFrame(columns=['id', 'user_id', 'transportation_mode', 'start_date_time', 'end_date_time', 'plt_file_name'])
#trackpoint_pandas = pd.DataFrame(columns=['id', 'activity_id', 'lat', 'lon', 'altitude', 'date_days', 'date_time'])

activity_pandas = pd.DataFrame(activity_list_all)
#activity_pandas = pd.concat([activity_pandas, activity_list], ignore_index=True)

trackpoint_pandas = pd.DataFrame(trackpoints_list_all)
#trackpoint_pandas = pd.concat([trackpoint_pandas, trackpoint_pandas_list], ignore_index=True)


# Ensure that all dates in the final DataFrame are in the correct format (with -)
activity_pandas['start_date_time'] = pd.to_datetime(activity_pandas['start_date_time']).dt.strftime('%Y-%m-%d %H:%M:%S')
activity_pandas['end_date_time'] = pd.to_datetime(activity_pandas['end_date_time']).dt.strftime('%Y-%m-%d %H:%M:%S')
trackpoint_pandas['date_time'] = pd.to_datetime(trackpoint_pandas['date_time']).dt.strftime('%Y-%m-%d %H:%M:%S')

# Print how many rows have been added to the table
print(f"Total number of rows in activity_pandas: {len(activity_pandas)}")

print("Finished processing. First 20 rows of activity_pandas:")
# Print the first 20 rows using tabulate
print(tabulate(activity_pandas.head(20), headers='keys', tablefmt='psql'))
print(tabulate(trackpoint_pandas.head(20), headers='keys', tablefmt='psql'))

# Define the path where you want to save the CSV file
csv_output_path = "/Users/marividringstad/Desktop/Høst 2024/Store, distribuerte datamengder/store-distribuerte-datamengder/cleaned_tables/activity_final.csv"

# Save the DataFrame to a CSV file
activity_pandas.to_csv(csv_output_path, index=False)

# Define the path where you want to save the CSV file
csv_output_path_track = "/Users/marividringstad/Desktop/Høst 2024/Store, distribuerte datamengder/store-distribuerte-datamengder/cleaned_tables/trackpoints_final.csv"

# Save the DataFrame to a CSV file
trackpoint_pandas.to_csv(csv_output_path_track, index=False)

print(f"Data successfully saved to {csv_output_path}")
print(f"Data successfully saved to {csv_output_path_track}")
