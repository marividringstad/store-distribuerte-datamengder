from DbConnector import DbConnector
import mysql.connector  # Import the correct MySQL connector module

def test_connection():
    try:
        db = DbConnector()
        print("Connected to the database!")
        db.close()
    except mysql.connector.Error as err:  # Catch the correct error
        print(f"Error: {err}")

if __name__ == "__main__":
    test_connection()
