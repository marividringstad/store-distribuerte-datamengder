# store-distribuerte-datamengder
# Todo-list for TDT4225 Assignment 2
## Setup and Preparation
### Setup MySQL Database:

1. Connect to the virtual machine via SSH.
2. Create a MySQL user with admin privileges.
3. Create a new database for your assignment.

### Setup Python Environment:

1. Install necessary Python packages using pip install -r requirements.txt.
2. Update DbConnector.py with database connection settings (host, database name, user, password).

## Part 1: Cleaning and Inserting Data
### Data Cleaning and Integration:

1. Read and understand the structure of the Geolife dataset.
2. Implement a Python program to clean and organize the dataset.
3. Ensure all data matches the schema defined for User, Activity, and TrackPoint tables.
4. Consider data size limitations (e.g., activities with <= 2500 trackpoints).

### Database Interaction:

1. Connect to MySQL using DbConnector.py.
2. Create tables User, Activity, and TrackPoint if they do not exist.
3. Insert cleaned data from the dataset into respective tables.
4. Handle foreign key constraints and ensure data integrity.

## Part 2: Querying the Database
### Writing SQL Queries:
1. Write Python scripts to execute SQL queries for the following tasks:
2. Count users, activities, and trackpoints.
3. Calculate average activities per user and find top 20 users by activity count.
4. Retrieve users who have used specific transportation modes.
5. Analyze data for years with the most activities and most recorded hours.
6. Calculate total distance walked by a specific user in a given year.
7. Identify top users gaining the most altitude meters.
8. Detect users with invalid activities based on timestamp deviations.
9. Find users who tracked activities in specific geographical locations.
10. Determine users' most frequently used transportation modes.

## Part 3: Report Writing
### Report Generation:
1. Create a report summarizing your findings and analysis.
2. Include screenshots of the top 10 rows from all relevant tables (User, Activity, TrackPoint).
3. Document SQL queries used and their results.
4. Discuss any deviations from the assignment guidelines and justify your approach.
5. Prepare the report in PDF format for submission.

## General Tips
### Documentation and Testing:
1. Document your code thoroughly with comments and docstrings.
2. Test SQL queries in the MySQL terminal before integrating them into Python scripts.
### Collaboration:
1. Coordinate tasks and code integration with your group members using Git.
2. Use Git branches for individual tasks and merge changes after review.