import logging
import pandas as pd
import sys
import os

# Add the parent directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from DbConnector import DbConnector
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
#from pandas_tables.activity_and_trackpoints import activity_pandas

def get_table_from_csv(username):

    #get correct path
    if username.lower() == 'erik':
        csv_file_path = "/Users/eriksundstrom/store-distribuerte-datamengder/cleaned_tables/activity_data.csv"
    if username.lower() == 'mari':
        csv_file_path = "/Users/marividringstad/Desktop/Høst 2024/Store, distribuerte datamengder/store-distribuerte-datamengder/cleaned_tables/activity_data.csv"
    if username.lower() == 'tine':
        csv_file_path = "/Users/tineaas-jakobsen/Desktop/Skrivebord – Tines MacBook Pro/NTNU/TDT4225 Store Distribuerte Datamengder/Assignment-2/store-distribuerte-datamengder/cleaned_tables/activity_data.csv"
    
    #store table in variabel
    pandas_table = pd.read_csv(csv_file_path)

    #convert 'start_date_time' and 'end_date_time' columns to string format for MySQL
    pandas_table['start_date_time'] = pandas_table['start_date_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
    pandas_table['end_date_time'] = pandas_table['end_date_time'].dt.strftime('%Y-%m-%d %H:%M:%S')

    return pandas_table

username = input("Who is running this code?")

activity_pandas = get_table_from_csv(username)

def insert_activities(activity_pandas, batch_size):
    db = None
    try:
        #connect to database
        db = DbConnector() 
        cursor = db.cursor
        
        #SQL insert query
        query = """
        INSERT INTO Activity (user_id, transportation_mode, start_date_time, end_date_time)
        VALUES (%s, %s, %s, %s)
        """  # Using ON DUPLICATE KEY to avoid inserting duplicate users -- TODO: stemmer dette?
        #uses autoincrement on activities
        
        data = []
        total_inserted = 0
        
        # Iterate over the users DataFrame and insert each user into the database
        for row in activity_pandas.iterrows():
            data.append((
                row['user_id'],
                row['transportation_mode'],
                row['start_date_time'],
                row['end_date_time']))
            
            #write in batches of batch_size - length
            if len(data) == batch_size:
                cursor.executemany(query, data)
                db.connection.commit()
                total_inserted += len(data)
                logging.info(f"Inserted final batch of {len(data)} users, total inserted: {total_inserted}")
                data = []  # Clear the list for the next batch
        
        #last write will not fill batch
        if data:
            cursor.executemany(query, data)
            db.connection.commit()
            total_inserted += len(data)
            logging.info(f"Inserted batch of {len(data)} users, total inserted: {total_inserted}")
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