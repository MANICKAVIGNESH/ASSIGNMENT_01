from googleapiclient.discovery import build
import pymongo
import mysql.connector
import pandas as pd
import streamlit as st

# connection of API key
def connecting_api():
    api_id = "AIzaSyDg_whzMVY82lrMZtxb28O8ERgCIvpvmWc"

    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name,api_version,developerKey=api_id)
    return youtube
#calling function
youtube = connecting_api()


#get channel details

def get_channel_information(channel_id):
    #parameter to get channel details
    ask = youtube.channels().list(
                    part = "snippet,ContentDetails,statistics",       #get "statistics-->to get no. of subscribe,no. of videos","snippet--> channel id,channel tittle,channel descreption","contentDetails-->video id"
                    id = channel_id                 #youtube channel id
    )
    replay = ask.execute()          #using execute function to execute ask function

    for i in replay['items']:           #we create dictnery bacause mongoDB store data type is always i Json format
        data = dict(Channel_Name=i["snippet"]["title"],   # to get channel name
                    Channel_Id=i["id"],                   # to get channel id
                    Subscribers=i['statistics']['subscriberCount'], # to get subscribers count
                    Views = i['statistics']['viewCount'],
                    Total_videos=i['statistics']['videoCount'],
                    Channel_Description = i['snippet']['description'],
                    Playlist_id = i ['contentDetails']['relatedPlaylists']['uploads']) 
    return data


#get channel videos id
def all_videos_id_in_channel(channel_id):
    Video_ids = []

    ask = youtube.channels().list(id = channel_id,
                                part = 'contentDetails').execute()
    Playlist_id = ask['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token = None
    while True:
        ask1 = youtube.playlistItems().list(
                                            part = 'snippet',
                                            playlistId = Playlist_id,
                                            maxResults = 50,#maxResults parameter is used to get 50 video ids
                                            pageToken = next_page_token).execute()  #pageToken parameter is used for get all video id in channel like more than 50 videos in channel
        for i in range(len(ask1['items'])):
            Video_ids.append(ask1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = ask1.get('nextPageToken')      #get function is used for if the video value is presented mean get that value otherwise it ignore errors

        if next_page_token is None:
            break
    return Video_ids


#get video information
def get_video_information(Videos_Ids):
    Video_data = []

    for video_id in Videos_Ids:
        ask = youtube.videos().list(
            part = 'snippet,ContentDetails,statistics',
            id = video_id
        )
        reply = ask.execute()

        for item in reply['items']:
            data = dict(Channel_Name = item['snippet']['channelTitle'],
                        Channel_Id = item['snippet']['channelId'],
                        Video_id = item['id'],
                        Tittle = item['snippet']['title'],
                        Tags = item['snippet'].get('tags'),
                        Thumbnails = item ['snippet']['thumbnails']['default']['url'],
                        Description = item['snippet'].get('description'),   #['snippet']['description'],
                        PublishedAt = item ['snippet']['publishedAt'],
                        Duration = item ['contentDetails']['duration'],
                        View_Count = item['statistics'].get('viewCount'), #['statistics']['viewCount'],
                        Definition = item ['contentDetails']['definition'],
                        Caption_Status = item ['contentDetails']['caption'],
                        Like_Count = item['statistics'].get('likeCount'),   #['statistics']['likeCount'],
                        Favorite_Count = item['statistics'].get('favoriteCount'), #['statistics']['favoriteCount'],
                        Comment_Count = item['statistics'].get('commentCount'),    #['statistics']['commentCount'],
                        )
            Video_data.append(data)
    return Video_data


#get comment information

def get_comment_information(Videos_Ids):
    Comment_data = []

    try:
        for video_id in Videos_Ids:
            ask = youtube.commentThreads().list(
                    part = 'snippet',
                    videoId = video_id,
                    maxResults = 50
                )
            reply = ask.execute()

            for item in reply['items']:
                        data = dict(Comment_Id_1 = item['snippet']['topLevelComment']['id'],
                                    video_Id = item ['snippet']['topLevelComment']['snippet']['videoId'],
                                    Comment_Id = item ['snippet']['topLevelComment']['snippet']['authorChannelId']['value'],
                                    Comment_Text = item ['snippet']['topLevelComment']['snippet']['textDisplay'],
                                    Comment_Author = item ['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                    Comment_PublishedAt = item ['snippet']['topLevelComment']['snippet']['publishedAt'])
                        
                        Comment_data.append(data)
                        
    except:
        pass
    return Comment_data


#get playlist data

def get_playlist_information(channel_id):

    Next_page_Token = None # this for collect all playlist more than 50 playlists
    Playlist_data = []

    while True:
        ask = youtube.playlists().list(
            part = 'snippet,contentDetails',
            channelId = channel_id,
            maxResults = 50,
            pageToken = Next_page_Token)

        reply = ask.execute()


        for item in reply['items']:
            data = dict(Playlist_Id = item ['id'],
                        Channel_Id = item ['snippet']['channelId'],
                        Playlist_Name = item ['snippet']['title'],
                        Channel_Name = item ['snippet']['channelTitle'],
                        Published_At = item ['snippet']['publishedAt'],
                        Item_Count = item ['contentDetails']['itemCount'])
            Playlist_data.append(data)

        Next_page_Token = reply.get('nextPageToken')
        if Next_page_Token is None:
            break
    return Playlist_data

# Create DataBase on mongoDB

data_transfer_mongodb = pymongo.MongoClient("mongodb://localhost:27017")
db = data_transfer_mongodb["youtube_harvesting_data"]

# upload to mongoDB

def channel_details(channel_id):
    channel_data = get_channel_information(channel_id)
    playlist_data = get_playlist_information(channel_id)
    video_ids_data = all_videos_id_in_channel(channel_id)
    video_data = get_video_information(video_ids_data)
    comment_data = get_comment_information(video_ids_data)


    collection_1 = db["channel_details"]
    collection_1.insert_one({"channel_details":channel_data, "playlist_details":playlist_data, "video_details":video_data, "comment_details":comment_data})

    return "success"


#Table Creation for channels,playlist,videos

def channels_table(): 

    import mysql.connector

    # Replace these values with your own MySQL server details
    host = "localhost"
    user = "root"
    password = "Pass@12345678"

    # Establish a connection to the MySQL server
    conn = mysql.connector.connect(host=host, user=user, password=password)

    # Create a cursor object to execute SQL queries
    cursor = conn.cursor()

    #drop_query = '''drop table if exists channels'''
    #cursor.execute(drop_query)
    #conn.commit()

    # Specify the name of the database you want to create
    database_name = "youtube_database"

    # SQL query to create the database
    create_database_query = f"CREATE DATABASE IF NOT EXISTS {database_name}"

    try:
        # Execute the SQL query to create the database
        cursor.execute(create_database_query)
        print(f"Database '{database_name}' created successfully.")

        # Use the newly created database
        cursor.execute(f"USE {database_name}")

        #drop exixting table

        drop_query = '''drop table if exists channels'''
        cursor.execute(drop_query)
        conn.commit()    

        # SQL query to create the 'channels' table
        create_query = '''create table if not exists channels(Channel_Name varchar(150), Channel_Id varchar(150) primary key, Subscribers bigint, Views bigint, Total_videos bigint, Channel_Description text, Playlist_id varchar(150))'''

        # Execute the SQL query to create the 'channels' table
        cursor.execute(create_query)


        # Commit the changes to the database
        conn.commit()

    except mysql.connector.Error as err:
        # Handle any errors that occur during database creation
        print(f"Error: {err}")
    """
    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()
    """


    #to get data from momgoDB

    #import mongoClient parameter to take values
    from pymongo import MongoClient
    client = MongoClient('localhost', 27017)

    channel_list = []
    db = client["youtube_harvesting_data"]
    collection_1 = db['channel_details']
    for channel_data in collection_1.find({},{'_id' : 0, 'channel_details' : 1}):                   #"{}" is used all channel details mongo db
        channel_list.append(channel_data["channel_details"])
    df = pd.DataFrame(channel_list)


    for index,row in df.iterrows():
        insert_query = '''insert into channels(Channel_Name,Channel_Id, Subscribers, Views, Total_videos, Channel_Description, Playlist_id)
        
        values(%s,%s,%s,%s,%s,%s,%s)'''
        values = (row['Channel_Name'],
                row['Channel_Id'],
                row['Subscribers'],
                row['Views'],
                row['Total_videos'],
                row['Channel_Description'],
                row['Playlist_id'])
        try:
            cursor.execute(insert_query,values)
            conn.commit()
        
        except:
            print('channels value is alredy inserted')


# transport playlist data from mongoDB to mysql

def playlist_table():

    import mysql.connector

    # Replace these values with your own MySQL server details
    host = "localhost"
    user = "root"
    password = "Pass@12345678"

    # Establish a connection to the MySQL server
    conn = mysql.connector.connect(host=host, user=user, password=password)

    # Create a cursor object to execute SQL queries
    cursor = conn.cursor()

    #drop_query = '''drop table if exists channels'''
    #cursor.execute(drop_query)
    #conn.commit()

    # Specify the name of the database you want to create
    database_name = "youtube_database"

    # SQL query to create the database
    create_database_query = f"CREATE DATABASE IF NOT EXISTS {database_name}"

    try:
        # Execute the SQL query to create the database
        cursor.execute(create_database_query)
        print(f"Database '{database_name}' created successfully.")

        # Use the newly created database
        cursor.execute(f"USE {database_name}")

        #drop exixting table

        drop_query = '''drop table if exists playlists'''
        cursor.execute(drop_query)
        conn.commit()    

        # SQL query to create the 'channels' table
        create_query = '''create table if not exists playlists(Playlist_Id varchar(150) primary key, Playlist_Name varchar(150), Channel_Id varchar(150), Channel_Name varchar(150), Published_At timestamp, Item_Count int)'''


        # Execute the SQL query to create the 'channels' table
        cursor.execute(create_query)


        # Commit the changes to the database
        conn.commit()

    except mysql.connector.Error as err:
        # Handle any errors that occur during database creation
        print(f"Error: {err}")


    #to get playlist data from momgoDB

    #import mongoClient parameter to take values
    from pymongo import MongoClient
    client = MongoClient('localhost', 27017)

    play_list = []
    db = client["youtube_harvesting_data"]
    collection_1 = db['channel_details']
    for playlist_data in collection_1.find({},{'_id' : 0, 'playlist_details' : 1}):                   #"{}" is used all channel details mongo db
        for i in range(len(playlist_data['playlist_details'])):
            play_list.append(playlist_data['playlist_details'][i])
    df1 = pd.DataFrame(play_list)

    from datetime import datetime

    for index, row in df1.iterrows():
        # Convert the string to a datetime object
        published_at = datetime.strptime(row['Published_At'], '%Y-%m-%dT%H:%M:%SZ')
        
        # Format the datetime object as a string in the MySQL format
        formatted_published_at = published_at.strftime('%Y-%m-%d %H:%M:%S')

        insert_query = '''insert into playlists(Playlist_Id, Playlist_Name, Channel_Id, Channel_Name, Published_At, Item_Count)
                        values(%s, %s, %s, %s, %s, %s)'''
        values = (row['Playlist_Id'],
                row['Playlist_Name'],
                row['Channel_Id'],
                row['Channel_Name'],
                formatted_published_at,  # Use the formatted datetime value
                row['Item_Count'])
        
        cursor.execute(insert_query, values)
        conn.commit()


# to transfer video details from mongoDb to mysql

def videos_table():
    import mysql.connector

    # Replace these values with your own MySQL server details
    host = "localhost"
    user = "root"
    password = "Pass@12345678"

    # Establish a connection to the MySQL server
    conn = mysql.connector.connect(host=host, user=user, password=password)

    # Create a cursor object to execute SQL queries
    cursor = conn.cursor()

    #drop_query = '''drop table if exists channels'''
    #cursor.execute(drop_query)
    #conn.commit()

    # Specify the name of the database you want to create
    database_name = "youtube_database"

    # SQL query to create the database
    create_database_query = f"CREATE DATABASE IF NOT EXISTS {database_name}"

    try:
        # Execute the SQL query to create the database
        cursor.execute(create_database_query)
        print(f"Database '{database_name}' created successfully.")

        # Use the newly created database
        cursor.execute(f"USE {database_name}")

        #drop exixting table

        drop_query = '''drop table if exists videos'''
        cursor.execute(drop_query)
        conn.commit()    

        # SQL query to create the 'channels' table
        create_query = '''create table if not exists videos(Channel_Name varchar(150), 
                                                            Channel_Id varchar(150), 
                                                            Video_id varchar(200) primary key,
                                                            Tittle varchar(150), 
                                                            Tags text,
                                                            Thumbnails varchar(250),
                                                            Description text,
                                                            PublishedAt timestamp,
                                                            Duration time,
                                                            View_Count bigint,
                                                            Like_Count bigint,
                                                            Comment_Count bigint,
                                                            Favorite_Count bigint,
                                                            Definition varchar(10),
                                                            Caption_Status varchar(60))'''

        # Execute the SQL query to create the 'channels' table
        cursor.execute(create_query)


        # Commit the changes to the database
        conn.commit()

    except mysql.connector.Error as err:
        # Handle any errors that occur during database creation
        print(f"Error: {err}")


    #to get video details from momgoDB

    #import mongoClient parameter to take values
    from pymongo import MongoClient
    client = MongoClient('localhost', 27017)

    video_list = []
    db = client["youtube_harvesting_data"]
    collection_1 = db['channel_details']
    for video_data in collection_1.find({},{'_id' : 0, 'video_details' : 1}):                   #"{}" is used all channel details mongo db
        for i in range(len(video_data['video_details'])):
            video_list.append(video_data['video_details'][i])
    df2 = pd.DataFrame(video_list)



    from datetime import datetime, timedelta
    from isodate import parse_duration

    for index, row in df2.iterrows():
            # Convert the string to a datetime object
            published_at = datetime.strptime(row['PublishedAt'], '%Y-%m-%dT%H:%M:%SZ')
            
            # Format the datetime object as a string in the MySQL format
            formatted_published_at = published_at.strftime('%Y-%m-%d %H:%M:%S')


            # Convert the 'Tags' list to a comma-separated string
            tags_str = ','.join(row['Tags']) if row['Tags'] else None
            
            # Parse duration using ISO 8601 format
            duration_str = row['Duration']
            duration_obj = parse_duration(duration_str)
            
            # Get the total duration in seconds
            total_seconds = int(duration_obj.total_seconds())

            # Convert to timedelta string
            duration = str(timedelta(seconds=total_seconds)) if duration_str else None


            insert_query = '''insert into videos(Channel_Name, Channel_Id, Video_id, Tittle, Tags, Thumbnails, 
                            Description, PublishedAt, Duration, View_Count, Like_Count, Comment_Count, 
                            Favorite_Count, Definition, Caption_Status)
                            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''

            values = (row['Channel_Name'], row['Channel_Id'], row['Video_id'], row['Tittle'], tags_str,
                    row['Thumbnails'], row['Description'], formatted_published_at, duration,
                    row['View_Count'], row['Like_Count'], row['Comment_Count'], row['Favorite_Count'],
                    row['Definition'], row['Caption_Status'])

            cursor.execute(insert_query, values)
            conn.commit()


# to transfer comment details from mongoDb to mysql

def comments_table():

    # to create table on my sql
    import mysql.connector

    # Replace these values with your own MySQL server details
    host = "localhost"
    user = "root"
    password = "Pass@12345678"

    # Establish a connection to the MySQL server
    conn = mysql.connector.connect(host=host, user=user, password=password)

    # Create a cursor object to execute SQL queries
    cursor = conn.cursor()

    #drop_query = '''drop table if exists channels'''
    #cursor.execute(drop_query)
    #conn.commit()

    # Specify the name of the database you want to create
    database_name = "youtube_database"

    # SQL query to create the database
    create_database_query = f"CREATE DATABASE IF NOT EXISTS {database_name}"

    try:
        # Execute the SQL query to create the database
        cursor.execute(create_database_query)
        print(f"Database '{database_name}' created successfully.")

        # Use the newly created database
        cursor.execute(f"USE {database_name}")

        #drop exixting table

        drop_query = '''drop table if exists comments'''
        cursor.execute(drop_query)
        conn.commit()    

        # SQL query to create the 'channels' table
        create_query = '''create table if not exists comments(Comment_Id_1 varchar(200) primary key, video_Id varchar(100), Comment_Text text, Comment_Author varchar(250), Comment_PublishedAt timestamp)'''

        # Execute the SQL query to create the 'channels' table
        cursor.execute(create_query)


        # Commit the changes to the database
        conn.commit()

    except mysql.connector.Error as err:
        # Handle any errors that occur during database creation
        print(f"Error: {err}")


    #to get playlist data from momgoDB

    #import mongoClient parameter to take values
    from pymongo import MongoClient
    client = MongoClient('localhost', 27017)

    comment_list = []
    db = client["youtube_harvesting_data"]
    collection_1 = db['channel_details']
    for comment_data in collection_1.find({},{'_id' : 0, 'comment_details' : 1}):                   #"{}" is used all channel details mongo db
        for i in range(len(comment_data['comment_details'])):
            comment_list.append(comment_data['comment_details'][i])
    df3 = pd.DataFrame(comment_list)


    from datetime import datetime

    for index, row in df3.iterrows():
        # Convert the string to a datetime object
        published_at = datetime.strptime(row['Comment_PublishedAt'], '%Y-%m-%dT%H:%M:%SZ')
        
        # Format the datetime object as a string in the MySQL format
        formatted_published_at = published_at.strftime('%Y-%m-%d %H:%M:%S')

        insert_query = '''insert into comments(Comment_Id_1, video_Id, Comment_Text, Comment_Author, Comment_PublishedAt)
                        values(%s, %s, %s, %s, %s)'''
        values = (row['Comment_Id_1'],
                row['video_Id'],
                row['Comment_Text'],
                row['Comment_Author'],
                formatted_published_at)  # Use the formatted datetime value
        
        cursor.execute(insert_query, values)
        conn.commit()


# transfer all data from momgoDB to mysql table

def tables():
    channels_table()
    playlist_table()
    videos_table()
    comments_table()

    return "table was created in mysql successfully"


def view_of_channel_tables():

    #to get data from momgoDB

    #import mongoClient parameter to take values
    from pymongo import MongoClient
    client = MongoClient('localhost', 27017)

    channel_list = []
    db = client["youtube_harvesting_data"]
    collection_1 = db['channel_details']
    for channel_data in collection_1.find({},{'_id' : 0, 'channel_details' : 1}):                   #"{}" is used all channel details mongo db
        channel_list.append(channel_data["channel_details"])
    df = st.dataframe(channel_list)

    return df


def view_of_playlist_tables():
    #to get playlist data from momgoDB

    #import mongoClient parameter to take values
    from pymongo import MongoClient
    client = MongoClient('localhost', 27017)

    play_list = []
    db = client["youtube_harvesting_data"]
    collection_1 = db['channel_details']
    for playlist_data in collection_1.find({},{'_id' : 0, 'playlist_details' : 1}):                   #"{}" is used all channel details mongo db
        for i in range(len(playlist_data['playlist_details'])):
            play_list.append(playlist_data['playlist_details'][i])
    df1 = st.dataframe(play_list)

    return df1


def view_of_video_tables():

    #to get video details from momgoDB

    #import mongoClient parameter to take values
    from pymongo import MongoClient
    client = MongoClient('localhost', 27017)

    video_list = []
    db = client["youtube_harvesting_data"]
    collection_1 = db['channel_details']
    for video_data in collection_1.find({},{'_id' : 0, 'video_details' : 1}):                   #"{}" is used all channel details mongo db
        for i in range(len(video_data['video_details'])):
            video_list.append(video_data['video_details'][i])
    df2 = st.dataframe(video_list)

    return df2


def view_of_comment_tables():

    #to get comment data from momgoDB

    #import mongoClient parameter to take values
    from pymongo import MongoClient
    client = MongoClient('localhost', 27017)

    comment_list = []
    db = client["youtube_harvesting_data"]
    collection_1 = db['channel_details']
    for comment_data in collection_1.find({},{'_id' : 0, 'comment_details' : 1}):                   #"{}" is used all channel details mongo db
        for i in range(len(comment_data['comment_details'])):
            comment_list.append(comment_data['comment_details'][i])
    df3 = st.dataframe(comment_list)

    return df3


#streamlit code


#import mongoClient parameter to take values
from pymongo import MongoClient
client = MongoClient('localhost', 27017)

with st.sidebar:
    st.title(":orange[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("Take Away Skills")
    st.caption("python Scripting")
    st.caption("Collection Data")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption("Data Management using MongoDB and SQL")


channel_id = st.text_input("Entre the channel ID")


if st.button("collect and store data"):
    channel_ids = []
    db = client["youtube_harvesting_data"]
    collection_1 = db['channel_details']
    for channel_data in collection_1.find({},{'_id' : 0, 'channel_details' : 1}):
        channel_ids.append(channel_data['channel_details']['Channel_Id'])

    # write function to avoid restore data in mongoDB 
    if channel_id in channel_ids:
        st.success("Not Allow!!!, because the channel details are already stored in data base")

    else:
        insert = channel_details(channel_id)
        st.success(insert)

# write function to transfer data from mongoDB to sql
if st.button("Transfer data from mongoDB to sql"):
    Table = tables()
    st.success(Table)

show_table = st.radio("VIEW THE SELECTED TABLE",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if show_table == "CHANNELS":
    view_of_channel_tables()

elif show_table == "PLAYLISTS":
    view_of_playlist_tables()

elif show_table == "VIDEOS":
    view_of_video_tables()

elif show_table == "COMMENTS":
    view_of_comment_tables()


#sql connection 
import mysql.connector

# Replace these values with your own MySQL server details
host = "localhost"
user = "root"
password = "Pass@12345678"
database = "youtube_database"  # Replace with your actual database name

# Establish a connection to the MySQL server with the specified database
conn = mysql.connector.connect(host=host, user=user, password=password, database=database)

# Create a cursor object to execute SQL queries
cursor = conn.cursor()

question = st.selectbox("select your question",("1. Name of all the videos and their corresponding channels",
                                                "2. Which channels have the most number of videos! and count of videos",
                                                "3. The top 10 most-viewed videos and those channels",
                                                "4. The number of comments per video",
                                                "5. The highest number of liked videos, and those channel names",
                                                "6. The highest number of liked videos, and those video names",
                                                "7. Views of each each channel",
                                                "8. published video in the year of 2022",
                                                "9. Average duration of all video in each channel",
                                                "10. Videos with highest number of comments"))


if question == "1. Name of all the videos and their corresponding channels":

    #my sql code for 1st question
    query_1 = '''select Tittle as videos,Channel_Name as channelname from videos'''
    cursor.execute(query_1)
    t1 = cursor.fetchall()      #it collect data from sql and stord in t1 variable
    df = pd.DataFrame(t1,columns=["video tittle","channel name"])
    st.write(df)

    cursor.close()
    conn.close()


elif question == "2. Which channels have the most number of videos! and count of videos":

    #my sql code for 1st question
    query_2 = '''select channel_name as channelname,total_videos as totalvideos from channels order by total_videos desc'''
    cursor.execute(query_2)
    t2 = cursor.fetchall()      #it collect data from sql and stord in t1 variable
    df2 = pd.DataFrame(t2,columns=["Channel Name","Total number of Videos"])
    st.write(df2)

    cursor.close()
    conn.close()



elif question == "3. The top 10 most-viewed videos and those channels":

    #my sql code for 1st question
    query_3 = '''select view_count as views,channel_name as channelname,tittle as videotittle from videos where view_count is not null order by view_count desc limit 10'''
    cursor.execute(query_3)
    t3 = cursor.fetchall()      #it collect data from sql and stord in t1 variable
    df3 = pd.DataFrame(t3,columns=["total view","Channel Name","video tittle"])
    st.write(df3)

    cursor.close()
    conn.close()


elif question == "4. The number of comments per video":

    #my sql code for 1st question
    query_4 = '''select comment_count as numberofcomment,tittle as videotittle from videos where comment_count is not null'''
    cursor.execute(query_4)
    t4 = cursor.fetchall()      #it collect data from sql and stord in t1 variable
    df4 = pd.DataFrame(t4,columns=["number of comment","video tittle"])
    st.write(df4)

    cursor.close()
    conn.close()


elif question == "5. The highest number of liked videos, and those channel names":

    #my sql code for 1st question
    query_5 = '''select tittle as videotittle, channel_name as channelname, like_count as likescount from videos where like_count is not null order by like_count desc'''
    cursor.execute(query_5)
    t5 = cursor.fetchall()      #it collect data from sql and stord in t1 variable
    df5 = pd.DataFrame(t5,columns=["video tittle","Channel name","Like counts"])
    st.write(df5)

    cursor.close()
    conn.close()


elif question == "6. The highest number of liked videos, and those video names":

    #my sql code for 1st question
    query_6 = '''select like_count as likecount, tittle as videotittle from videos'''
    cursor.execute(query_6)
    t6 = cursor.fetchall()      #it collect data from sql and stord in t1 variable
    df6 = pd.DataFrame(t6,columns=["Like counts","video tittle"])
    st.write(df6)

    cursor.close()
    conn.close()


elif question == "7. Views of each each channel":

    #my sql code for 1st question
    query_7 = '''select channel_name as channelname,views as totalviews from channels'''
    cursor.execute(query_7)
    t7 = cursor.fetchall()      #it collect data from sql and stord in t1 variable
    df7 = pd.DataFrame(t7,columns=["Channel Name","Total Views"])
    st.write(df7)

    cursor.close()
    conn.close()


elif question == "8. published video in the year of 2022":

    #my sql code for 1st question
    query_8 = '''select tittle as videotittle,publishedat as videolaunch,channel_name as channelname from videos where extract(year from publishedat)=2022'''
    cursor.execute(query_8)
    t8 = cursor.fetchall()      #it collect data from sql and stord in t1 variable
    df8 = pd.DataFrame(t8,columns=["Video Tittle","video publish at 2022","Channel Name"])
    st.write(df8)

    cursor.close()
    conn.close()


elif question == "9. Average duration of all video in each channel":

    #my sql code for 1st question
    query_9 = '''select channel_name as channelname,AVG(duration) as averageduration from videos group by channel_name'''
    cursor.execute(query_9)
    t9 = cursor.fetchall()      #it collect data from sql and stord in t1 variable
    df9 = pd.DataFrame(t9,columns=["Channel Name","avg duration of videos"])
    #st.write(df9)
    print(df9)

    t9 = []
    for index,row in df9.iterrows():
        channel_tittle = row["Channel Name"]
        avg_duration = row["avg duration of videos"]
        avg_duration_str = str(avg_duration)
        t9.append(dict(channeltittle=channel_tittle,avgduration=avg_duration_str))

    dff9=pd.DataFrame(t9)
    st.write(dff9)

    cursor.close()
    conn.close()


elif question == "10. Videos with highest number of comments":

    #my sql code for 1st question 
    query_10 = '''select tittle as videotittle,channel_name as channelname ,comment_count as commentcount from videos where comment_count is not null order by comment_count desc'''
    cursor.execute(query_10)
    t10 = cursor.fetchall()      #it collect data from sql and stord in t1 variable
    df10 = pd.DataFrame(t10,columns=["Video Title","Channel Name","Comment Count"])
    st.write(df10)

    cursor.close()
    conn.close()
