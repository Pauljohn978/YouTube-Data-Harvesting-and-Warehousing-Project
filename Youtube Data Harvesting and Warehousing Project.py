#!/usr/bin/env python
# coding: utf-8

# In[11]:


#--------------------------Final Version----------------------------------------------------------#
from googleapiclient.discovery import build
from pymongo import MongoClient
import pandas as pd
import mysql.connector
import streamlit as st

# Getting the API KEY
def Api_connect():
    Api_Id = "AIzaSyCxpracUdCQdzp8Zl-YguKUDetbBXWc4Tc"
    api_service_name = "youtube"
    api_version = "v3"
    
    youtube = build(api_service_name, api_version, developerKey=Api_Id)
    return youtube

youtube = Api_connect()

#Getting Channel Info
def get_channel_info(channel_id):
    request = youtube.channels().list(
        part="snippet, contentDetails, statistics",
        id=channel_id
    )
    
    response = request.execute()

    # Initialize an empty dictionary to store channel information
    channel_data = {}

    # Getting Channel statistics
    for i in response["items"]:
        channel_data = {
            "channel_Name": i["snippet"]["title"],
            "Channel_Id": i["id"],
            "subscribers": i["statistics"]["subscriberCount"],
            "Views": i["statistics"]["viewCount"],
            "Total_Videos": i["statistics"]["videoCount"],
            "Channel_Description": i["snippet"]["description"],
            "Playlist_Id": i["contentDetails"]["relatedPlaylists"]["uploads"]
        }

    return channel_data
#-----------------------------------------------------------------------------------------------------------
# get video Ids
def get_video_ids(channel_id):
    video_ids = []
    response = youtube.channels().list(id=channel_id, part="contentDetails").execute()
    playlist_id = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

    next_page_token = None

    while True:
        response1 = youtube.playlistItems().list(part="snippet", playlistId=playlist_id, maxResults=25, pageToken=next_page_token).execute()

        for i in range(len(response1["items"])):
            video_ids.append(response1["items"][i]["snippet"]["resourceId"]["videoId"])

        next_page_token = response1.get("nextPageToken")

        if next_page_token is None:
            break
    return video_ids
#----------------------------------------------------------------------------------------------------------
# Get video Info
def get_video_info(video_ids):
    video_data = []
    for video_id in video_ids:
        request = youtube.videos().list(part="snippet, contentDetails, statistics", id=video_id)
        response = request.execute()

        for item in response["items"]:
            data = {
                "Channel_Name": item["snippet"]["channelTitle"],
                "Channel_Id": item["snippet"]["channelId"],
                "video_Id": item["id"],
                "Title": item["snippet"]["title"],
                "Tags": item["snippet"].get("tags"),
                "Thumbnail": item["snippet"]["thumbnails"]["default"]["url"],
                "description": item["snippet"].get("description"),
                "Published_Date": item["snippet"]["publishedAt"],
                "Duration": item["contentDetails"]["duration"],
                "Views": item["statistics"].get("viewCount"),
                "Likes": item["statistics"].get("likeCount"),
                "Comment": item["statistics"].get("commentCount"),
                "Favorite_Count": item["statistics"]["favoriteCount"],
                "Definition": item["contentDetails"]["definition"],
                "Caption_Status": item["contentDetails"]["caption"]
            }
            video_data.append(data)

    return video_data
#-------------------------------------------------------------------------------------------------------------------
#get comment info
def get_comment_info(video_ids):
    Comment_data=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(part="snippet", videoId=video_id, maxResults=25)
            response=request.execute()
            for item in response["items"]:
                data=dict(Comment_Id=item["snippet"]["topLevelComment"]["id"], Video_Id=item["snippet"]["topLevelComment"]["snippet"]["videoId"], Comment_Text=item["snippet"]["topLevelComment"]["snippet"]["textDisplay"], Comment_Author=item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"], Comment_Published=item["snippet"]["topLevelComment"]["snippet"]["publishedAt"])
                Comment_data.append(data)
    except:
        pass
    return Comment_data
#--------------------------------------------------------------------------------------------------------------------
#Getting playlist Ids
def get_playlist_details(channel_id):
    next_page_token=None
    All_data=[]
    while True:
        request = youtube.playlists().list(part="snippet, contentDetails", channelId=channel_id, maxResults=25, pageToken=next_page_token)
        response=request.execute()
        for item in response["items"]:
            data=dict(Playlist_Id=item["id"], Title=item["snippet"]["title"], Channel_Id=item["snippet"]["channelId"], Channel_Name=item["snippet"]["channelTitle"], PublishedAt=item["snippet"]["publishedAt"], Video_Count=item["contentDetails"]["itemCount"])
            All_data.append(data)

        next_page_token=response.get("nextPageToken")
        if next_page_token is None:
            break
    return All_data   
#----------------------------------------------------------------------------------------------------------------------
#Connecting to mongo

client = MongoClient("mongodb://localhost:27017")
db = client["Youtube_data"]
coll1=db["channel_details"]
#------------------------------------------------------------------------------------------------------------------
#upload to mongo
def channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    pl_details=get_playlist_details(channel_id)
    vi_ids=get_video_ids(channel_id)
    vi_details=get_video_info(vi_ids)
    com_details=get_comment_info(vi_ids)
    
    # Inserting data into MongoDB
    coll1.insert_one({
        "channel_information": ch_details,
        "playlist_information": pl_details,
        "video_information": vi_details,
        "comment_information": com_details,
    })

    return "upload completed successfully"
#------------------------------------------------------------------------------------------------------------------------
#Creating channel table and inserting values
def channels_table():
    mydb = mysql.connector.connect(
        host="127.0.0.1",
        user="root",
        password="MySQL",  
        database="youtubedata",
        port=3306
    )
    cursor = mydb.cursor()
    
    drop_query = """DROP TABLE IF EXISTS channels"""
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = """CREATE TABLE IF NOT EXISTS channels(
            Channel_Name VARCHAR(100), 
            Channel_Id VARCHAR(100) PRIMARY KEY,
            Subscribers BIGINT,
            Views BIGINT,
            Total_Videos INT,
            Channel_Description TEXT,
            Playlist_Id VARCHAR(80)
        )"""
        cursor.execute(create_query)
        mydb.commit()

    except Exception as e:
        print(f"Error creating channels table: {e}")

    ch_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    
    for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
        ch_list.append(ch_data['channel_information'])

    df = pd.DataFrame(ch_list)

    for index, row in df.iterrows():
        insert_query = '''INSERT INTO channels(Channel_Name, Channel_Id, Subscribers, Views, Total_Videos, Channel_Description, Playlist_Id) 
                           VALUES(%s, %s, %s, %s, %s, %s, %s)'''
        values = (row["channel_Name"], row["Channel_Id"], row["subscribers"], row["Views"], 
                  row["Total_Videos"], row["Channel_Description"], row["Playlist_Id"])
        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except Exception as e:
            print(f"Error inserting values into channels table: {e}")

#---------------------------------------------------------------------------------------------------------------------
#Playlist table
def playlist_table():
    mydb = mysql.connector.connect(
            host="127.0.0.1",
            user="root",
            password="MySQL",  
            database="youtubedata",
            port=3306
        )
    cursor=mydb.cursor()

    drop_query="""drop table if exists playlists"""
    cursor.execute(drop_query)
    mydb.commit()


    create_query="""create table if not exists playlists(Playlist_Id varchar(100) primary key, 
                                                                Title varchar(100),
                                                                Channel_Id varchar(100),
                                                                Channel_Name varchar(100),
                                                                PublishedAt timestamp,
                                                                Video_Count int
                                                                )"""

    cursor.execute(create_query)
    mydb.commit()
    
    pl_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for pl_data in coll1.find({},{"_id":0, "playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df=pd.DataFrame(pl_list)
    
    # Inserting Values to playlist table
    cursor=mydb.cursor()
    for index, row in df.iterrows():
            insert_query = '''insert into playlists(Playlist_Id, Title, Channel_Id, Channel_Name, PublishedAt, Video_Count) values(%s, %s, %s, %s, %s, %s)'''
            ['Playlist_Id', 'Title', 'Channel_Id', 'Channel_Name', 'PublishedAt', 'Video_Count']
            values = (row["Playlist_Id"], row["Title"], row["Channel_Id"], row["Channel_Name"], row["PublishedAt"], row["Video_Count"])

            cursor.execute(insert_query, values)
            mydb.commit()
#----------------------------------------------------------------------------------------------------------------
#Crating video table
def videos_table():
    mydb = mysql.connector.connect(
            host="127.0.0.1",
            user="root",
            password="MySQL",  
            database="youtubedata",
            port=3306
        )
    cursor=mydb.cursor()

    drop_query="""drop table if exists videos"""
    cursor.execute(drop_query)
    mydb.commit()


    create_query = """create table if not exists videos(
        Channel_Name varchar(100),
        Channel_Id varchar(100),
        video_Id varchar(100) primary key,
        Title varchar(100),
        Tags text,
        Thumbnail varchar(100),
        description text,
        Published_Date timestamp,
        Duration timestamp,
        Views bigint,
        Likes bigint,
        Comment int,
        Favorite_Count int,
        Definition varchar(10),
        Caption_Status varchar(100)
    )"""

    cursor.execute(create_query)
    mydb.commit()

    vi_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({},{"_id":0, "playlist_information":1}):
        for i in range(len(pl_data["video_information"])):
            vi_list.append(pl_data["video_information"][i])
    df2=pd.DataFrame(vi_list)


    for index, row in df2.iterrows():
        insert_query = """insert into videos(
                            Channel_Name,
                            Channel_Id,
                            video_Id,
                            Title,
                            Tags,
                            Thumbnail varchar,
                            description,
                            Published_Date,
                            Duration,
                            Views,
                            Likes,
                            Comment,
                            Favorite_Count,
                            Definition,
                            Caption_Status
                            )
        values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        values = (row["Channel_Name"], row["Channel_Id"], row["video_Id"], row["Title"], row["Tags"], row["Thumbnail"], row["description"], row["Published_Date"], row["Duration"], row["Views"], row["Likes"], row["Comment"],row["Favorite_Count"],row["Definition"],row["Caption_Status"])

        cursor.execute(insert_query, values)
        mydb.commit()
#-------------------------------------------------------------------------------------------------------------------- 
#Creating Comments table
def comments_table():
    mydb = mysql.connector.connect(
        host="127.0.0.1",
        user="root",
        password="MySQL", 
        database="youtubedata",
        port=3306
    )
    cursor = mydb.cursor()

    drop_query = """drop table if exists comments"""
    cursor.execute(drop_query)
    mydb.commit()

    create_query = """create table if not exists comments(
        Comment_Id varchar(100) primary key,
        Video_Id varchar(100),
        Comment_Text text,
        Comment_Author varchar(100),
        Comments_Published timestamp
    )"""

    cursor.execute(create_query)
    mydb.commit()

    com_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for com_data in coll1.find({}, {"_id": 0, "comment_information": 1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3 = pd.DataFrame(com_list)

    for index, row in df3.iterrows():
        insert_query = """insert into comments(
                            Comment_Id,
                            Video_Id,
                            Comment_Text,
                            Comment_Author,
                            Comments_Published
                        )
                        values(%s, %s, %s, %s, %s)"""

        values = (row["Comment_Id"], row["Video_Id"], row["Comment_Text"], row["Comment_Author"], row["Comments_Published"])

        cursor.execute(insert_query, values)
        mydb.commit()
#---------------------------------------------------------------------------------------------------------------------
#Creating all tables
def tables():
    channels_table()
    playlist_table()
    videos_table()
    comments_table()
    
    return "Tables Created Successfully"
#----------------------------------------------------------------------------------------------------------------------
def show_channels_table():
    ch_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0, "channel_information":1}):
        ch_list.append(ch_data['channel_information'])
    df=st.dataframe(ch_list)    
    return df
#------------------------------------------------------------------------------------------------------------------------
def show_playlists_table():
    pl_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for pl_data in coll1.find({},{"_id":0, "playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1=st.dataframe(pl_list)
    return df1
#------------------------------------------------------------------------------------------------------------------------
def show_videos_table():
    vi_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({},{"_id":0, "playlist_information":1}):
        for i in range(len(pl_data["video_information"])):
            vi_list.append(pl_data["video_information"][i])
    df2= st.dataframe(vi_list)        
    return df2
#----------------------------------------------------------------------------------------------------------------------
def show_comments_table():
    com_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for com_data in coll1.find({}, {"_id": 0, "comment_information": 1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3 = st.dataframe(com_list)
    return df3
#-----------------------------------------------------------------------------------------------------------------------
#Streamlit
with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("Skill Take Away")
    st.caption("Python Scripting")
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption("Data Management using MongoDB and SQL")
    
channel_id=st.text_input("Enter Channel ID:")

if st.button("Collect and store data"):
    ch_ids=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0, "channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["Channel_Id"])
        
    if channel_id in ch_ids:
        st.success("Channel Details of the given channel id already exists")
    else:
        insert=channel_details(channel_id)
        st.success(insert)
        
if st.button("Migrate to SQL"):
    Table=tables(Table)
    
show_table=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","PLAYLISTS","VIDEOS", "COMMENTS"))

if show_table=="CHANNEL":
    show_channels_table()
elif show_table=="PLAYLISTS":
    show_playlists_table()
elif show_table=="VIDEOS":
    show_videos_table()
elif show_table=="COMMENTS":
    show_comments_table()   
#-----------------------------------------------------------------------------------------------------------------------
#Queries
#SQL Connection
mydb = mysql.connector.connect(
host="127.0.0.1",
user="root",
password="MySQL", 
database="youtubedata",
port=3306)
cursor = mydb.cursor()

question=st.selectbox("Select your Question",("1. All the videos and their corresponding channel",
                                              "2. Which channels have the most number of videos, and how many videos do they have?",
                                             "3. What are the top 10 most viewed videos and their respective channels?",
                                              "4. How many comments were made on each video, and what are their corresponding video names?",
                                              "5. Which videos have the highest number of likes, and what are their corresponding video names?",
                                             "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                              "7. What is the total number of views for each channel, and what are their corresponding channel names?",
                                              "8. What are the names of all the channels that have published videos in the year 2022?",
                                              "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                              "10. Which videos have the highest number of comments, and what are their corresponding channel names?"
                                             ))
#----------------------------------------------------------------------------------------------------------------------------

if question=="1. All the videos and their corresponding channel":
    query1="""select title as videos, channel_name as channelname from videos"""
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    df=pd.DataFrame(t1, columns=["video title", "channel name"])
    st.write(df)

elif question=="2. Which channels have the most number of videos, and how many videos do they have?":
    query2="""select channel_name as channel_name, total_videos as no_videos from channels
              order by total_videos desc"""
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    df2=pd.DataFrame(t2, columns=["channel name", "No. of videos"])
    st.write(df2)

elif question=="3. What are the top 10 most viewed videos and their respective channels?":
    query3="""select views as views, channel_name as channelname, title as videotitle from videos
               where views is not null order by views desc limit 10"""
    cursor.execute(query3)
    mydb.commit()
    t3=cursor.fetchall()
    df3=pd.DataFrame(t3, columns=["views","channel name", "videotitle"])
    st.write(df3)
    
elif question=="4. How many comments were made on each video, and what are their corresponding video names?":
    query4="""select comments as no_comments, title as videotitle from video where comments is not null"""
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    df4=pd.DataFrame(t4, columns=["No. of comments", "videotitle"])
    st.write(df4)    
    
elif question=="5. Which videos have the highest number of likes, and what are their corresponding video names?":
    query5="""select title as videotitle, channel_name as channelname, likes as likecount from videos where is not null order by likes desc"""
    cursor.execute(query5)
    mydb.commit()
    t5=cursor.fetchall()
    df5=pd.DataFrame(t5, columns=["Videotitle", "channelname", "likescount"])
    st.write(df5)  
    
elif question=="6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
    query6="""select likes as likecount, title as videotitle from videos"""
    cursor.execute(query6)
    mydb.commit()
    t6=cursor.fetchall()
    df6=pd.DataFrame(t6, columns=["likecount", "videotitle"])
    st.write(df6) 
    
elif question=="7. What is the total number of views for each channel, and what are their corresponding channel names?":
    query7="""select channel_name as channelname, views as totalviews from channels"""
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    df7=pd.DataFrame(t7, columns=["channel name", "total views"])
    st.write(df7)     
    
elif question=="8. What are the names of all the channels that have published videos in the year 2022?":
    query8="""select title as video_title, published_date as videorelease, channel_name as channelname from videos
                where extract(year from published_date)=2022"""
    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    df8=pd.DataFrame(t8, columns=["videotitle", "published_date", "channelname"])
    st.write(df8) 
    
elif question=="9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    query9="""select channel_name as channelname, AVG(duration) as averageduration from group by channel_name"""
    cursor.execute(query9)
    mydb.commit()
    t9=cursor.fetchall()
    df9=pd.DataFrame(t9, columns=["channelname", "averageduration"])
    st.write(df9)
    T9=[]
    
    for index, row in df9.itrrows():
        channel_title=row["channelname"]
        average_duration=row["average duration"]
        average_duration_str=str(average_duration)
        T9.append(dict(channeltitle=channel_title, avgduration=avg_duration_str))
    df1=pd.DataFrame(T9)
    st.write(df1)
    
elif question=="10. Which videos have the highest number of comments, and what are their corresponding channel names?":
    query10="""select title as videotitle, channel_name as channelname, comments as comments from videos where comments is not null
                order by comment desc"""
    cursor.execute(query10)
    mydb.commit()
    t10=cursor.fetchall()
    df10=pd.DataFrame(t10, columns=["video title", "channel name", "comments"])
    st.write(df10)
    


# In[ ]:





# In[ ]:




