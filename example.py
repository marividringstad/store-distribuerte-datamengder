from DbConnector import DbConnector
import mysql.connector

def test_connection():
    print("Starting the database connection test...")  # Log start
    try:
        print("Attempting to create a connection object...")  # Log connection attempt
        db = DbConnector()  # Try to establish a connection
        print("Connected to the database!")  # Log success
        db.close()  # Log closing the connection
        print("Connection closed.")  # Log connection closure
    except mysql.connector.Error as err:
        print(f"Error: {err}")  # Log any error

if __name__ == "__main__":
    print("Running example.py...")  # Log entry to the script
    test_connection()
