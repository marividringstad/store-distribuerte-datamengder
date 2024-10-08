import pandas as pd
import mysql.connector
import sys
import os

# Add the parent directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from DbConnector import DbConnector

# Define the path to your CSV file
csv_file_path = "/Users/eriksundstrom/store-distribuerte-datamengder/cleaned_tables/activity_data.csv"

# Read the CSV file into a pandas DataFrame
activity_pandas = pd.read_csv(csv_file_path)

# Convert 'start_date_time' and 'end_date_time' to datetime objects
activity_pandas['start_date_time'] = pd.to_datetime(activity_pandas['start_date_time'])
activity_pandas['end_date_time'] = pd.to_datetime(activity_pandas['end_date_time'])


def insert_activities(activity_pandas):
    db = DbConnector()  # Establish a connection to the database
    cursor = db.cursor
    
    # Prepare the SQL insert query
    query = """
    INSERT INTO Activity (id, user_id, transportation_mode, start_date_time, end_date_time)
    VALUES (%s, %s, %s, %s, %s)
    """  # Using ON DUPLICATE KEY to avoid inserting duplicate users
    
    # Iterate over the users DataFrame and insert each user into the database
    for index, row in activity_pandas.iterrows():
        id = row['id']
        user_id = row['user_id']
        transportation_mode = row['transportation_mode']
        start_date_time = row['start_date_time'].to_pydatetime()
        end_date_time = row['end_date_time'].to_pydatetime()
        cursor.execute(query, (id, user_id, transportation_mode, start_date_time, end_date_time))
    
    # Commit the transaction
    db.connection.commit()
    
    print(f"Inserted {cursor.rowcount} activities.")
    
    db.close()  # Close the connection

# Call the function to insert users
insert_activities(activity_pandas)