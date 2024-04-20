import mysql.connector
import streamlit as st
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from datetime import datetime
import pandas as pd

# Function to establish connection to MySQL
def connect_to_mysql():
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="tech@123",
            database="youtube"
        )
        if connection.is_connected():
            print("Connected to MySQL database")
            return connection
    except mysql.connector.Error as e:
        print("Error connecting to MySQL database:", e)

# Function to close the database connection
def close_connection(connection):
    connection.close()
    print("MySQL connection closed")

# Function to create MySQL table for channels
def create_channel_table(connection):
    cursor = connection.cursor()
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS channel (
                channel_id VARCHAR(255) PRIMARY KEY,
                channel_name VARCHAR(255),
                channel_type VARCHAR(255),       
                channel_views INT,
                channel_description TEXT,
                channel_status VARCHAR(255)
            )
        """)
        connection.commit()
        print("Channel table created successfully.")
    except mysql.connector.Error as e:
        print("Error creating channel table:", e)
    finally:
        cursor.close()

# Function to create MySQL table for playlists
def create_playlist_table(connection):
    cursor = connection.cursor()
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS playlist (
                playlist_id VARCHAR(255) PRIMARY KEY,
                channel_id VARCHAR(255),
                playlist_name VARCHAR(255),
                FOREIGN KEY (channel_id) REFERENCES channel(channel_id)
            )
        """)
        connection.commit()
        print("Playlist table created successfully.")
    except mysql.connector.Error as e:
        print("Error creating playlist table:", e)
    finally:
        cursor.close()

# Function to create MySQL table for comments
def create_comments_table(connection):
    cursor = connection.cursor()
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS comments (
                comment_id VARCHAR(255) PRIMARY KEY,
                video_id VARCHAR(255),
                comment_text TEXT,       
                comment_author VARCHAR(255),
                comment_published_date DATETIME,
                FOREIGN KEY (video_id) REFERENCES videos(video_id)
            )
        """)
        connection.commit()
        print("Comments table created successfully.")
    except mysql.connector.Error as e:
        print("Error creating comments table:", e)
    finally:
        cursor.close()

# Function to create MySQL table for videos
def create_videos_table(connection):
    cursor = connection.cursor()
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS videos (
                video_id VARCHAR(255) PRIMARY KEY,
                playlist_id VARCHAR(255),
                video_name VARCHAR(255),
                video_description TEXT,
                published_date DATETIME,
                view_count INT,
                like_count INT,
                dislike_count INT, 
                favorite_count INT, 
                comment_count INT,                                
                duration INT,
                thumbnail VARCHAR(255),
                caption_status VARCHAR(255),
                FOREIGN KEY (playlist_id) REFERENCES playlist(playlist_id)
            )
        """)
        connection.commit()
        print("Videos table created successfully.")
    except mysql.connector.Error as e:
        print("Error creating videos table:", e)
    finally:
        cursor.close()

# Function to check if the channel exists in the MySQL database
def channel_exists(channel_id, connection):
    cursor = connection.cursor()
    try:
        query = "SELECT channel_id FROM channel WHERE channel_id = %s"
        cursor.execute(query, (channel_id,))
        result = cursor.fetchone()  # Fetch one record
        return result is not None
    except mysql.connector.Error as e:
        print("Error checking channel existence:", e)
    finally:
        cursor.close()

# Function to check if a video exists in the MySQL database
def video_exists(video_id, connection):
    cursor = connection.cursor()
    try:
        query = "SELECT COUNT(*) FROM videos WHERE video_id = %s"
        cursor.execute(query, (video_id,))
        result = cursor.fetchone()[0]
        return result > 0
    except mysql.connector.Error as e:
        print("Error checking if video exists:", e)
        return False
    finally:
        cursor.close()

# Function to insert channel details into the MySQL database
def insert_channel_details(channel_details, connection):
    cursor = connection.cursor()
    try:
        insert_query = """
            INSERT INTO channel (channel_id, channel_name, channel_type, channel_views, channel_description, channel_status)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (
            channel_details['id'],
            channel_details['snippet']['title'],
            channel_details['snippet'].get('channelType'),  # Assuming 'channelType' exists in the 'snippet' dictionary
            channel_details['statistics']['viewCount'],
            channel_details['snippet']['description'],
            'active'
        ))
        connection.commit()
        print("Channel details inserted successfully")
    except mysql.connector.Error as e:
        connection.rollback()
        print("Error inserting channel details:", e)
    finally:
        cursor.close()

# Function to insert playlist details into the MySQL database
def insert_playlist_details(playlist_details, connection):
    cursor = connection.cursor()
    try:
        insert_query = """
            INSERT INTO playlist (playlist_id, channel_id, playlist_name)
            VALUES (%s, %s, %s)
        """
        cursor.execute(insert_query, (
            playlist_details['id'],
            playlist_details['snippet']['channelId'],
            playlist_details['snippet']['title']
        ))
        connection.commit()
        print("Playlist details inserted successfully")
    except mysql.connector.Error as e:
        connection.rollback()
        print("Error inserting playlist details:", e)
    finally:
        cursor.close()    

# Function to insert video details into the MySQL database
def insert_video_details(video_details, connection, playlist_id):
    cursor = connection.cursor()
    try:
        insert_query = """
            INSERT INTO videos (video_id, playlist_id, video_name, video_description, published_date, view_count, like_count, dislike_count, favorite_count, comment_count, duration, thumbnail, caption_status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        # Correcting KeyError handling for other keys
        category_id = video_details['snippet'].get('categoryId', None)
        published_date_iso8601 = video_details['snippet'].get('publishedAt', None)
        published_date_mysql = convert_iso8601_to_mysql_datetime(published_date_iso8601)
        thumbnail_url = video_details['snippet']['thumbnails'].get('default', {}).get('url', None)

        # Check if 'contentDetails' key exists
        content_details = video_details.get('contentDetails', {})
        duration_iso8601 = content_details.get('duration', None)
        duration_seconds = convert_duration_iso8601_to_seconds(duration_iso8601) if duration_iso8601 else None

        # Check if 'statistics' key exists
        statistics = video_details.get('statistics', {})
        dislike_count = statistics.get('dislikeCount', 0)

        cursor.execute(insert_query, (
            video_details['id'],
            playlist_id,
            video_details['snippet'].get('title', None),
            video_details['snippet'].get('description', None),
            published_date_mysql,
            statistics.get('viewCount', None),
            statistics.get('likeCount', None),
            dislike_count,
            statistics.get('favoriteCount', None),
            statistics.get('commentCount', None),
            duration_seconds,
            thumbnail_url,
            'active'  # Assuming 'active' is the default value for caption status
        ))

        connection.commit()
        print("Video details inserted successfully")
    except mysql.connector.Error as e:
        connection.rollback()
        print("Error inserting video details:", e)
    finally:
        cursor.close()

# Function to convert ISO 8601 duration format to seconds
def convert_duration_iso8601_to_seconds(duration_iso8601):
    if duration_iso8601 is None:
        return None

    # Initialize variables for hours, minutes, and seconds
    hours = 0
    minutes = 0
    seconds = 0

    # Extracting hours, minutes, and seconds from ISO 8601 duration
    duration_iso8601 = duration_iso8601[2:]  # Remove 'PT' prefix
    if 'H' in duration_iso8601:
        hours_index = duration_iso8601.index('H')
        hours = int(duration_iso8601[:hours_index])
        duration_iso8601 = duration_iso8601[hours_index + 1:]
    if 'M' in duration_iso8601:
        minutes_index = duration_iso8601.index('M')
        minutes = int(duration_iso8601[:minutes_index])
        duration_iso8601 = duration_iso8601[minutes_index + 1:]
    if 'S' in duration_iso8601:
        seconds_index = duration_iso8601.index('S')
        seconds = int(duration_iso8601[:seconds_index])

    # Calculate total duration in seconds
    total_seconds = hours * 3600 + minutes * 60 + seconds
    return total_seconds

# Function to convert ISO 8601 date to MySQL datetime format
def convert_iso8601_to_mysql_datetime(iso8601_date):
    # Parse ISO 8601 date string
    parsed_date = datetime.strptime(iso8601_date, '%Y-%m-%dT%H:%M:%SZ')
    # Convert to MySQL datetime format
    mysql_datetime = parsed_date.strftime('%Y-%m-%d %H:%M:%S')
    return mysql_datetime

# Function to insert comments details into the MySQL database
def insert_comment_details(comment_details, connection):
    cursor = connection.cursor()
    try:
        # Check if the video exists before inserting the comment details
        if video_exists(comment_details['video_id'], connection):
            insert_query = """
                INSERT INTO comments (comment_id, video_id, comment_text, comment_author, comment_published_date)
                VALUES (%s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, (
                comment_details['id'],
                comment_details['video_id'],
                comment_details['comment_text'],
                comment_details['author_name'],
                convert_iso8601_to_mysql_datetime(comment_details['comment_published_date'])  # Convert ISO 8601 to MySQL datetime
            ))
            connection.commit()
            print("Comment details inserted successfully")
        else:
            print("Error: The video identified by the video_id parameter could not be found.")
    except mysql.connector.Error as e:
        connection.rollback()
        print("Error inserting comment details:", e)
    finally:
        cursor.close()

# Function to get channel details from YouTube API
def get_channel_details(youtube, channel_id):
    request = youtube.channels().list(
        part="snippet,statistics",
        id=channel_id
    )
    response = request.execute()
    if 'items' in response and len(response['items']) > 0:
        return response['items'][0]
    else:
        return None

# Function to get videos IDs from a channel
def get_videos_ids(channel_id, youtube):
    video_ids = []
    response = youtube.channels().list(
        id=channel_id,
        part='contentDetails'
    ).execute()
    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token = None

    while True:
        response1 = youtube.playlistItems().list(
            part='snippet',
            playlistId=playlist_id,
            maxResults=100,
            pageToken=next_page_token
        ).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = response1.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids

# Function to get video details from YouTube API
def get_video_details_from_playlist(youtube, playlist_id):
    videos = []
    request = youtube.playlistItems().list(
        part="snippet",
        playlistId=playlist_id,
        maxResults=100  # Adjust maxResults if needed
    )
    while request:
        response = request.execute()
        video_ids = [item["snippet"]["resourceId"]["videoId"] for item in response.get("items", [])]
        
        # Get video details including contentDetails and statistics
        video_details_request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=",".join(video_ids)
        )
        video_details_response = video_details_request.execute()
        
        for video_item in video_details_response.get("items", []):
            video_details = {
                "id": video_item["id"],
                "snippet": video_item["snippet"],
                "contentDetails": video_item.get("contentDetails", {}),
                "statistics": video_item.get("statistics", {})
            }
            videos.append(video_details)
            
        request = youtube.playlistItems().list_next(request, response)
    return videos

# Function to get playlist details from YouTube API
def get_playlist_details(youtube, channel_id):
    playlists = []
    request = youtube.playlists().list(
        part="snippet,contentDetails",
        channelId=channel_id,
        maxResults=100  # Adjust maxResults if needed
    )
    while request:
        response = request.execute()
        for playlist in response.get("items", []):
            playlists.append(playlist)
        request = youtube.playlists().list_next(request, response)
    return playlists

# Function to get comments details from YouTube API
def get_comments_details(youtube, video_ids):
    comments = []
    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=600
            )
            response = request.execute()

            if 'items' in response:  # Check if comments are found for the video ID
                for item in response['items']:
                    try:
                        comment_details = {
                            "id": item['snippet']['topLevelComment']['id'],
                            "video_id": item['snippet']['topLevelComment']['snippet']['videoId'],
                            "author_name": item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            "comment_text": item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            "comment_published_date": item['snippet']['topLevelComment']['snippet']['publishedAt']  # Include comment published date
                        }
                        comments.append(comment_details)
                    except KeyError:
                        # Skip comments with missing data
                        pass
            else:
                print(f"No comments found for the video with ID: {video_id}")
    except HttpError as e:
        if e.resp.status == 404:
            print(f"No comments found for the video with ID: {video_id}")
        else:
            print("An error occurred while fetching comments:", e)
    return comments

# Main function to run the Streamlit app
def main():
    # Connect to MySQL database
    connection = connect_to_mysql()
    if connection:
        # Create channel table if not exists
        create_channel_table(connection)

        # Create comments table if not exists
        create_comments_table(connection)

        # Create videos table if not exists
        create_videos_table(connection)

        # Create playlist table if not exists
        create_playlist_table(connection)

        # API key connection
        global youtube
        api_key = "AIzaSyDaCs6xKEF83rWDioipZAF8uwK9UgrtDtE"  # Replace with your actual API key
        youtube = build("youtube", "v3", developerKey=api_key)

        # Streamlit UI
        st.title("YouTube Channel Details Collector")

        # Input for channel ID
        channel_id = st.text_input("Enter YouTube Channel ID:")
        if st.button("Get Channel Details"):
            if channel_id:
                if channel_exists(channel_id, connection):
                    st.error("Channel details already exist in the database.")
                else:
                    channel_details = get_channel_details(youtube, channel_id)
                    if channel_details:
                        insert_channel_details(channel_details, connection)
                        st.success("Channel details inserted successfully.")
                    else:
                        st.error("Channel not found. Please enter a valid channel ID.")
            else:
                st.warning("Please enter a YouTube channel ID.")

        # Input for fetching and storing playlist details
        if st.button("Get and Store Playlist Details"):
            if channel_id:
                playlists = get_playlist_details(youtube, channel_id)
                if playlists:
                    for playlist in playlists:
                        insert_playlist_details(playlist, connection)
                    st.success("Playlist details inserted successfully.")
                else:
                    st.error("No playlists found for the given channel ID.")
            else:
                st.warning("Please enter a YouTube channel ID.")

        # Input for fetching and storing video details
        if st.button("Get and Store Video Details"):
            if channel_id:
                playlists = get_playlist_details(youtube, channel_id)
                if playlists:
                    for playlist in playlists:
                        # Fetch video details for each playlist
                        videos = get_video_details_from_playlist(youtube, playlist['id'])
                        if videos:
                            for video in videos:
                                insert_video_details(video, connection, playlist['id'])  # Pass playlist ID to insert function
                            st.success("Video details inserted successfully.")
                        else:
                            st.error(f"No videos found for the playlist: {playlist['snippet']['title']}")
                else:
                    st.error("No playlists found for the given channel ID.")
            else:
                st.warning("Please enter a YouTube channel ID.")

        # Input for fetching and storing comments details
        if st.button("Get and Store Comments Details"):
            if channel_id:
                video_ids = get_videos_ids(channel_id, youtube)
                if video_ids:
                    comments = get_comments_details(youtube, video_ids)
                    if comments:
                        for comment in comments:
                            insert_comment_details(comment, connection)
                        st.success("Comments details inserted successfully.")
                    else:
                        st.error("No comments found for the given video IDs.")
                else:
                    st.error("No video IDs found for the given channel ID.")
            else:
                st.warning("Please enter a YouTube channel ID.")

        # Close database connection when done
        close_connection(connection)

# Function to execute SQL query and return results as DataFrame
def execute_query(query):
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="tech@123",
        database="youtube"
    )
    cursor = connection.cursor(dictionary=True)
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    connection.close()
    return results

# Function to display query results as table in Streamlit
def display_results_as_table(results):
    if results:
        df = pd.DataFrame(results)
        st.write(df)
    else:
        st.write("No results found.")

# Query 1: What are the names of all the videos and their corresponding channels?
def query1():
    query = """
    SELECT v.video_name, c.channel_name
    FROM videos v
    INNER JOIN playlist p ON v.playlist_id = p.playlist_id
    INNER JOIN channel c ON p.channel_id = c.channel_id
    """
    results = execute_query(query)
    st.write("Query 1: Names of all the videos and their corresponding channels")
    display_results_as_table(results)

# Query 2: Which channels have the most number of videos, and how many videos do they have?
def query2():
    query = """
    SELECT c.channel_name, COUNT(v.video_id) AS num_videos
    FROM channel c
    INNER JOIN playlist p ON c.channel_id = p.channel_id
    INNER JOIN videos v ON p.playlist_id = v.playlist_id
    GROUP BY c.channel_name
    ORDER BY num_videos DESC
    LIMIT 10
    """
    results = execute_query(query)
    st.write("Query 2: Channels with the most number of videos and their corresponding counts")
    display_results_as_table(results)

# Query 3: What are the top 10 most viewed videos and their respective channels?
def query3():
    query = """
    SELECT v.video_name, c.channel_name, v.view_count
    FROM videos v
    INNER JOIN playlist p ON v.playlist_id = p.playlist_id
    INNER JOIN channel c ON p.channel_id = c.channel_id
    ORDER BY v.view_count DESC
    LIMIT 10
    """
    results = execute_query(query)
    st.write("Query 3: Top 10 most viewed videos and their respective channels")
    display_results_as_table(results)

# Query 4: How many comments were made on each video, and what are their corresponding video names?
def query4():
    query = """
    SELECT v.video_name, COUNT(c.comment_id) AS num_comments
    FROM videos v
    LEFT JOIN comments c ON v.video_id = c.video_id
    GROUP BY v.video_name
    """
    results = execute_query(query)
    st.write("Query 4: Number of comments on each video and their corresponding video names")
    display_results_as_table(results)

# Query 5: Which videos have the highest number of likes, and what are their corresponding channel names?
def query5():
    query = """
    SELECT v.video_name, c.channel_name, v.like_count
    FROM videos v
    INNER JOIN playlist p ON v.playlist_id = p.playlist_id
    INNER JOIN channel c ON p.channel_id = c.channel_id
    ORDER BY v.like_count DESC
    LIMIT 10
    """
    results = execute_query(query)
    st.write("Query 5: Videos with the highest number of likes and their corresponding channel names")
    display_results_as_table(results)

# Query 6: What is the total number of likes and dislikes for each video, and what are their corresponding video names?
def query6():
    query = """
    SELECT v.video_name, SUM(v.like_count) AS total_likes
    FROM videos v
    GROUP BY v.video_name
    """
    results = execute_query(query)
    st.write("Query 6: Total number of likes and dislikes for each video and their corresponding video names")
    display_results_as_table(results)

# Query 7: What is the total number of views for each channel, and what are their corresponding channel names?
def query7():
    query = """
    SELECT c.channel_name, SUM(v.view_count) AS total_views
    FROM channel c
    INNER JOIN playlist p ON c.channel_id = p.channel_id
    INNER JOIN videos v ON p.playlist_id = v.playlist_id
    GROUP BY c.channel_name
    """
    results = execute_query(query)
    st.write("Query 7: Total number of views for each channel and their corresponding channel names")
    display_results_as_table(results)

# Query 8: What are the names of all the channels that have published videos in the year 2022?
def query8():
    query = """
    SELECT DISTINCT c.channel_name
    FROM channel c
    INNER JOIN playlist p ON c.channel_id = p.channel_id
    INNER JOIN videos v ON p.playlist_id = v.playlist_id
    WHERE YEAR(v.published_date) = 2022
    """
    results = execute_query(query)
    st.write("Query 8: Names of all channels that have published videos in the year 2022")
    display_results_as_table(results)

# Query 9: What is the average duration of all videos in each channel, and what are their corresponding channel names?
def query9():
    query = """
    SELECT c.channel_name, AVG(v.duration) AS avg_duration
    FROM channel c
    INNER JOIN playlist p ON c.channel_id = p.channel_id
    INNER JOIN videos v ON p.playlist_id = v.playlist_id
    GROUP BY c.channel_name
    """
    results = execute_query(query)
    st.write("Query 9: Average duration(in seconds) of all videos in each channel and their corresponding channel names")
    display_results_as_table(results)

# Query 10: Which videos have the highest number of comments, and what are their corresponding channel names?
def query10():
    query = """
    SELECT v.video_name, c.channel_name, COUNT(cm.comment_id) AS num_comments
    FROM videos v
    INNER JOIN playlist p ON v.playlist_id = p.playlist_id
    INNER JOIN channel c ON p.channel_id = c.channel_id
    LEFT JOIN comments cm ON v.video_id = cm.video_id
    GROUP BY v.video_id, c.channel_name, v.video_name
    ORDER BY num_comments DESC;
    """
    results = execute_query(query)
    st.write("Query 10: Videos with the highest number of comments and their corresponding channel names")
    display_results_as_table(results)


# Function to run the selected query
def run_query(selected_query):
    if selected_query == "1.What are the names of all the videos and their corresponding channels?":
        query1()
    elif selected_query == "2.Which channels have the most number of videos, and how many videos do they have?":
        query2()
    elif selected_query == "3.What are the top 10 most viewed videos and their respective channels?":
        query3()
    elif selected_query == "4.How many comments were made on each video, and what are their corresponding video names?":
        query4()
    elif selected_query == "5.Which videos have the highest number of likes, and what are their corresponding channel names?":
        query5()
    elif selected_query == "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
        query6()
    elif selected_query == "7.What is the total number of views for each channel, and what are their corresponding channel names?":
        query7()
    elif selected_query == "8.What are the names of all the channels that have published videos in the year 2022?":
        query8()
    elif selected_query == "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":
        query9()
    elif selected_query == "10.Which videos have the highest number of comments, and what are their corresponding channel names?":
        query10()

# Streamlit UI for querying the database
def query_ui():
    st.title("Choose a Query")
    selected_query = st.selectbox("", ["1.What are the names of all the videos and their corresponding channels?", 
                                               "2.Which channels have the most number of videos, and how many videos do they have?", 
                                               "3.What are the top 10 most viewed videos and their respective channels?", 
                                               "4.How many comments were made on each video, and what are their corresponding video names?", 
                                               "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
                                               "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                               "7.What is the total number of views for each channel, and what are their corresponding channel names?",
                                               "8.What are the names of all the channels that have published videos in the year 2022?",
                                               "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                               "10.Which videos have the highest number of comments, and what are their corresponding channel names?"])
    if st.button("Run Query"):
        run_query(selected_query)

# Run the main function
if __name__ == "__main__":
    main()
    query_ui()
