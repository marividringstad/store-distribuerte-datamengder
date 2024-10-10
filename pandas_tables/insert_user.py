import pandas as pd
import mysql.connector
import sys
import os

# Add the parent directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from DbConnector import DbConnector
from pandas_tables.user_tables import users_pandas
csv_file_path = "/Users/marividringstad/Desktop/Høst 2024/Store, distribuerte datamengder/store-distribuerte-datamengder/cleaned_tables/users_data.csv"

# Read the CSV file into a pandas DataFrame
users_pandas = pd.read_csv(csv_file_path)

def insert_users(users_pandas):
    db = DbConnector()  # Establish a connection to the database
    cursor = db.cursor
    
    # Prepare the SQL insert query
    query = """
    INSERT INTO User (id, has_labels)
    VALUES (%s, %s)
    ON DUPLICATE KEY UPDATE has_labels=VALUES(has_labels);
    """  # Using ON DUPLICATE KEY to avoid inserting duplicate users
    
    # Iterate over the users DataFrame and insert each user into the database
    for index, row in users_pandas.iterrows():
        user_id = row['id']
        has_labels = row['has_labels']
        cursor.execute(query, (user_id, has_labels))
    
    # Commit the transaction
    db.connection.commit()
    
    print(f"Inserted {cursor.rowcount} users.")
    
    db.close()  # Close the connection

# Call the function to insert users
insert_users(users_pandas)
