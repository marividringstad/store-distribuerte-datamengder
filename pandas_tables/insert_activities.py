import logging
import pandas as pd
import mysql.connector
import sys
import os

# Add the parent directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from DbConnector import DbConnector
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
#from pandas_tables.activity_and_trackpoints import activity_pandas

activity_pandas = pd.read_csv("/Users/marividringstad/Desktop/Høst 2024/Store, distribuerte datamengder/store-distribuerte-datamengder/cleaned_tables/activity_final.csv")
# Define the path to your CSV file
#csv_file_path = "/Users/eriksundstrom/store-distribuerte-datamengder/cleaned_tables/activity_data.csv"

# Read the CSV file into a pandas DataFrame
#activity_pandas = pd.read_csv(csv_file_path)

# Convert 'start_date_time' and 'end_date_time' columns to string format for MySQL
#activity_pandas['start_date_time'] = activity_pandas['start_date_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
#activity_pandas['end_date_time'] = activity_pandas['end_date_time'].dt.strftime('%Y-%m-%d %H:%M:%S')

def insert_activities(activity_pandas, batch_size):
    db = None
    try:
        db = DbConnector()  # Establish a connection to the database
        cursor = db.cursor
        
        # Prepare the SQL insert query
        query = """
        INSERT INTO Activity (id, user_id, transportation_mode, start_date_time, end_date_time)
        VALUES (%s, %s, %s, %s, %s)
        """  # Using ON DUPLICATE KEY to avoid inserting duplicate users
        
        data = []
        total_inserted = 0
        
        # Iterate over the users DataFrame and insert each user into the database
        for index, row in activity_pandas.iterrows():
            data.append((
                row['id'],
                row['user_id'],
                row['transportation_mode'],
                row['start_date_time'],
                row['end_date_time']))
            
            if len(data) == batch_size:
                cursor.executemany(query, data)
                db.connection.commit()
                total_inserted += len(data)
                logging.info(f"Inserted batch of {len(data)} users, total inserted: {total_inserted}")
                data = []  # Clear the list for the next batch
        if data:
            cursor.executemany(query, data)
            db.connection.commit()
            total_inserted += len(data)
        db.connection.commit()
            
    except Exception as e:
        if db:
            db.connection.rollback()  # Rollback the transaction in case of error
        logging.error(f"Error occurred: {e}")
        raise  # Re-raise the exception after rollback to make sure it's caught
        
    finally:    
        if db:
            db.close() 
    print(f"Inserted {cursor.rowcount} activities.")

# Call the function to insert users
insert_activities(activity_pandas, batch_size = 1000)