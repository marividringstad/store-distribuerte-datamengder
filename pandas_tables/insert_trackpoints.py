import pandas as pd
import mysql.connector
import sys
import os

# Add the parent directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from DbConnector import DbConnector

# Define the path to your CSV file
csv_file_path = "/Users/eriksundstrom/store-distribuerte-datamengder/cleaned_tables/trackpoints_data.csv"

# Read the CSV file into a pandas DataFrame
trackpoint_pandas = pd.read_csv(csv_file_path)

# Convert 'start_date_time' and 'end_date_time' to datetime objects
trackpoint_pandas['date_time'] = pd.to_datetime(trackpoint_pandas['date_time'])

def insert_trackpoints(trackpoint_pandas):
    db = DbConnector()  # Establish a connection to the database
    cursor = db.cursor
    
    # Prepare the SQL insert query
    query = """
    INSERT INTO Activity (id, activity_id, lat, lon, altitude, date_days, date_time)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """  # Using ON DUPLICATE KEY to avoid inserting duplicate users
    
    # Iterate over the users DataFrame and insert each user into the database
    for index, row in trackpoint_pandas.iterrows():
        id = row['id']
        activity_id = row['activity_id']
        lat = row['lat']
        lon = row['lon']
        date_days = row['date_days']
        date_time = row['date_time'].to_pydatetime()
        cursor.execute(query, (id, activity_id, lat, lon, date_days, date_time))
    
    # Commit the transaction
    db.connection.commit()
    
    print(f"Inserted {cursor.rowcount} activities.")
    
    db.close()  # Close the connection

# Call the function to insert users
insert_trackpoints(trackpoint_pandas)