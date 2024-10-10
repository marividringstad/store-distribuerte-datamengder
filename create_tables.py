from DbConnector import DbConnector

def create_tables():
    db = DbConnector()
    cursor = db.cursor

    #create user table
    create_user_table = """
    CREATE TABLE IF NOT EXISTS User (
        id VARCHAR(50) PRIMARY KEY,
        has_labels BOOLEAN
    );
    """
    
    #create activity table
    create_activity_table = """
    CREATE TABLE IF NOT EXISTS Activity (
        id INT PRIMARY KEY AUTO_INCREMENT,
        user_id VARCHAR(50),                    
        transportation_mode VARCHAR(50),
        start_date_time DATETIME,
        end_date_time DATETIME,
        FOREIGN KEY (user_id) REFERENCES User(id)
    );
    """
    
    #create trackpoints table
    create_trackpoint_table = """
    CREATE TABLE IF NOT EXISTS TrackPoint (
        id INT PRIMARY KEY AUTO_INCREMENT,
        activity_id INT,
        lat DOUBLE,
        lon DOUBLE,
        altitude INT,
        date_days DOUBLE,
        date_time DATETIME,
        FOREIGN KEY (activity_id) REFERENCES Activity(id)
    );
    """

    # Execute the table creation queries
    cursor.execute(create_user_table)
    cursor.execute(create_activity_table)
    cursor.execute(create_trackpoint_table)

    # Commit the changes
    db.connection.commit()

    print("Tables created successfully!")

    # Close the connection
    db.close()

if __name__ == "__main__":
    create_tables()
