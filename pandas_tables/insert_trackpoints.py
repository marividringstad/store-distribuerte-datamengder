import logging
import pandas as pd
import sys
import os

# Add the parent directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from DbConnector import DbConnector
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

#from pandas_tables.activity_and_trackpoints import trackpoint_pandas


def get_table_from_csv(username):

    #get correct path
    if username.lower() == 'erik':
        csv_file_path = "/Users/eriksundstrom/store-distribuerte-datamengder/cleaned_tables/trackpoints_final.csv"
    if username.lower() == 'mari':
        csv_file_path = "/Users/marividringstad/Desktop/Høst 2024/Store, distribuerte datamengder/store-distribuerte-datamengder/cleaned_tables/trackpoints_final.csv"
    if username.lower() == 'tine':
        csv_file_path = "/Users/tineaas-jakobsen/Desktop/Skrivebord – Tines MacBook Pro/NTNU/TDT4225 Store Distribuerte Datamengder/Assignment-2/store-distribuerte-datamengder/cleaned_tables/trackpoints_final.csv"
    
    #store table in variable
    pandas_table = pd.read_csv(csv_file_path)

    #convert 'date_time'  column to string format for MySQL
    #pandas_table['date_time'] = pandas_table['date_time'].dt.strftime('%Y-%m-%d %H:%M:%S')

    return pandas_table

username = input("Who is running this code?")

trackpoint_pandas = get_table_from_csv(username)

def insert_trackpoints(trackpoint_pandas, batch_size):
    db = None
    try:
        #connect to database
        db = DbConnector()  
        cursor = db.cursor
        
        #SQL insert query
        query = """
        INSERT INTO TrackPoint (activity_id, lat, lon, altitude, date_days, date_time)
        VALUES (%s, %s, %s, %s, %s, %s)
        """ #used autoincrement id
        data = []
        total_inserted = 0
        
        # Iterate over the trackpoints DataFrame and insert each trackpoint into the database
        for index, row in trackpoint_pandas.iterrows():
            data.append((
                row['activity_id'],
                row['lat'],
                row['lon'],
                row['altitude'],
                row['date_days'],
                row['date_time']
            ))
            
            #write in batched
            if len(data) == batch_size:
                cursor.executemany(query, data)
                db.connection.commit()
                total_inserted += len(data)
                logging.info(f"Inserted batch of {len(data)} trackpoints, total inserted: {total_inserted}")
                data = []  # Clear the list for the next batch

        #last write will not fill batch
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

insert_trackpoints(trackpoint_pandas, batch_size=1000)