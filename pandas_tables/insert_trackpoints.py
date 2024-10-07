import pandas as pd
from DbConnector import DbConnector
import mysql.connector

from pandas_tables.trackpoints_tables import trackpoint_pandas

# Define the path to your CSV file
csv_file_path = "/Users/eriksundstrom/store-distribuerte-datamengder/cleaned_tables/trackpoints_data.csv"

# Read the CSV file into a pandas DataFrame
trackpoint_pandas = pd.read_csv(csv_file_path)

def insert_users(trackpoint_pandas):
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
insert_users(trackpoint_pandas)