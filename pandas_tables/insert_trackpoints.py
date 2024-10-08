import logging
import pandas as pd
import mysql.connector
import sys
import os

# Add the parent directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from DbConnector import DbConnector
from pandas_tables.activity_and_trackpoints import trackpoint_pandas
# Define the path to your CSV file
#csv_file_path = "/Users/eriksundstrom/store-distribuerte-datamengder/cleaned_tables/trackpoints_data.csv"
#logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Read the CSV file into a pandas DataFrame
#trackpoint_pandas = pd.read_csv(csv_file_path)

# Convert 'start_date_time' and 'end_date_time' to datetime objects
#trackpoint_pandas['date_time'] = pd.to_datetime(trackpoint_pandas['date_time'])
start_trackpoints = trackpoint_pandas[:200000]
def insert_trackpoints(start_trackpoints, batch_size):
    db = None
    try:
        db = DbConnector()  # Establish a connection to the database
        cursor = db.cursor
        
        # Prepare the SQL insert query
        query = """
        INSERT INTO TrackPoint (activity_id, lat, lon, altitude, date_days, date_time)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        data = []
        total_inserted = 0
        
        # Iterate over the trackpoints DataFrame and insert each trackpoint into the database
        for index, row in start_trackpoints.iterrows():
            print(row)
            data.append((
                #row['id'],
                row['activity_id'],
                row['lat'],
                row['lon'],
                row['altitude'],
                row['date_days'],
                row['date_time']
            ))
            
            if len(data) == batch_size:
                cursor.executemany(query, data)
                db.connection.commit()
                total_inserted += len(data)
                logging.info(f"Inserted batch of {len(data)} trackpoints, total inserted: {total_inserted}")
                data = []  # Clear the list for the next batch

        # Insert remaining data (less than batch_size)
        if data:
            cursor.executemany(query, data)
            db.connection.commit()
            total_inserted += len(data)
            logging.info(f"Inserted final batch of {len(data)} trackpoints, total inserted: {total_inserted}")

    except Exception as e:
        if db:
            db.connection.rollback()  # Rollback the transaction in case of error
        logging.error(f"Error occurred: {e}")
        raise  # Re-raise the exception after rollback to make sure it's caught
        
    finally:    
        if db:
            db.close()  # Close the database connection
    print(f"Total trackpoints inserted: {total_inserted}")

insert_trackpoints(start_trackpoints, batch_size=1000)