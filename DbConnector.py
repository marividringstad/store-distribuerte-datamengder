import mysql.connector

class DbConnector:
    def __init__(self):
        self.connection = mysql.connector.connect(
        host="10.22.78.239",  # Your computer's IP address
        port="3306",
        user="new_user",       # The new user you've created
        password="new_password",  # The password for the new user
        database="your_db_name"   # The name of your database
        )
        self.cursor = self.connection.cursor()

    def close(self):
        self.connection.close()

