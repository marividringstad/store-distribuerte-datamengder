import mysql.connector

class DbConnector:
    def __init__(self):
        self.connection = mysql.connector.connect(
            host="localhost",  # since Docker is running locally
            port="3306",  # default MySQL port
            user="root",  # MySQL root user
            password="your_password",  # replace with your MySQL root password
            database="your_db_name"  # replace with your database name
        )
        self.cursor = self.connection.cursor()

    def close(self):
        self.connection.close()
