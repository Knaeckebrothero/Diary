import os
import dotenv
import mysql.connector
import pandas as pd

# Load environment variables
dotenv.load_dotenv(dotenv.find_dotenv())

# Establish a database connection
conn = mysql.connector.connect(
    host=os.getenv('DATABASE_HOST'),
    user=os.getenv('DATABASE_USER'),
    password=os.getenv('DATABASE_PASSWORD'),
    database=os.getenv('DATABASE_NAME')
)

# Create a cursor
cursor = conn.cursor()

# Load the JSON data
data = pd.read_json(os.getenv('JSON_PATH'), orient='records')

# Transform the data (if necessary)
# This step will depend on the structure of your JSON data and your database schema

# Load the data into MySQL
for index, row in data.iterrows():
    # Construct the SQL query
    query = """
        INSERT INTO diary_day (today_date, fell_asleep, woke_up, focus, start_mood, end_mood, satisfaction)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    # Execute the query with the data from the current row
    cursor.execute(query, tuple(row))

# Commit the transactions
conn.commit()

# Close the database connection
cursor.close()
conn.close()
