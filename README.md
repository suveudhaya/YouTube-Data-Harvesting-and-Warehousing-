# Capstone-1
YouTube Data Harvesting and Warehousing 

# Establishing Connection to MySQL Database:
The script connects to a MySQL database using the mysql.connector library. It contains functions to establish and close connections to the database.

# Creating Database Tables:
Functions are provided to create necessary tables (channel, playlist, videos, comments) in the MySQL database if they do not already exist.

# Data Collection from YouTube API:
The script fetches data from the YouTube Data API using a provided API key.
It retrieves channel details, playlist details, video details, and comments details from YouTube.

# Data Insertion into MySQL Database:
Functions are provided to insert fetched data into the corresponding tables in the MySQL database.

# Querying MySQL Database:
The script includes predefined SQL queries to extract useful insights from the collected data.
These queries are implemented as functions and include tasks such as retrieving video names, channel names, view counts, comment counts, etc.
Streamlit UI is provided to select and execute these predefined queries.
