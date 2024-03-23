import sqlalchemy as db
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy import BigInteger,Text,TIMESTAMP,Time,Table, Column, Integer, String, MetaData
from sqlalchemy.dialects.mysql import LONGTEXT
import pandas as pd
import googleapiclient.discovery 
import re
import streamlit as st
import plotly.express as px
import base64 

# Create a YouTube API client
api_key1 ='**********************************' #give your api key
youtube = googleapiclient.discovery.build('youtube', 'v3', developerKey=api_key1)

# Create a YouTube db in sql

engine = db.create_engine("mysql://root:password@127.0.0.1:3306/youtubeDB?charset=utf8mb4&collation=utf8mb4_0900_ai_ci")
if not database_exists(engine.url):
    create_database(engine.url)
conn = engine.connect()

# Get channel data based on the given channel id

def get_channel_data(channel_id):
    try:
        # Fetch channel details
        channel_response = youtube.channels().list(
            part='snippet,contentDetails,statistics',
            id=channel_id
        ).execute()
        
        # Extract required information from the channel_data into dictionary

        channel_data = {
            "channel_Id": channel_id,
            'channel_name': channel_response['items'][0]['snippet']['title'],
            'channel_video_count': channel_response['items'][0]['statistics']['videoCount'],
            'channel_subscriber_count': channel_response['items'][0]['statistics']['subscriberCount'],
            'channel_view_count': channel_response['items'][0]['statistics']['viewCount'],
            'channel_description': channel_response['items'][0]['snippet']['description'],
            'channel_playlist_id': channel_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        }
    
        return channel_data
    except:
        # Return false when there is no channel data

        return False


# Define a function to retrieve video IDs from channel playlist
    
def get_video_ids(youtube, channel_id):
    video_id = []
    try:
        # Fetch playlist ID
        playlist_response = youtube.channels().list(
            part='contentDetails',
            id=channel_id
        ).execute()
        
        playlist_id = playlist_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']     
        
        next_page_token = None
        while True:
            # Get playlist items
            request = youtube.playlistItems().list(
                part='contentDetails',
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token)
            response = request.execute()

            # Get video IDs
            for item in response['items']:
                video_id.append(item['contentDetails']['videoId'])

            # Check if there are more pages
            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                break

        return video_id
    except:
        # return empty video id
        return video_id

#get video information
    
def get_video_info(youtube,video_ids):
    video_data=[]
    try:
        for video_id in video_ids:
            request=youtube.videos().list(
                part="snippet,ContentDetails,statistics",
                id=video_id
            )
            response=request.execute()

            # convert PTxxMyyS to 00:xx:yy format using Timedelta function in pandas

            def time_duration(t):
                if (t != 'Not Available') :
                    a = pd.Timedelta(t)
                    b = str(a).split()[-1]
                    return b
                else:
                    return t
            def published_date_format(d):
                    d = d.replace('T', ' ').replace('Z', '')
                    return d
            

            for item in response["items"]:
                data=dict(channel_name=item['snippet']['channelTitle'],
                        channel_id=item['snippet']['channelId'],
                        video_id=item['id'],
                        video_title=item['snippet']['title'],
                        video_description=item['snippet'].get('description','Not Available'),
                        view_count=item['statistics'].get('viewCount',0),
                        like_count=item['statistics'].get('likeCount',0),
                        dislike_count=item['statistics'].get('dislikeCount',0),
                        comments_count=item['statistics'].get('commentCount',0),
                        favorite_count=item['statistics'].get('favoriteCount',0),
                        thumbnail=item['snippet']['thumbnails']['default']['url'],
                        published_date=published_date_format(item['snippet']['publishedAt']),
                        duration=time_duration(item['contentDetails'].get('duration','Not Available')),
                        definition=item['contentDetails']['definition'],
                        caption_status=item['contentDetails'].get('caption','Not Available')
                        )
                video_data.append(data)    
        return video_data
    except:
        return video_data

#get comment information
    
def get_comment_info(youtube,video_ids):
    Comment_data=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response=request.execute()

            def published_date_format(d):
                d = d.replace('T', ' ').replace('Z', '')
                return d
                
            for item in response['items']:
                data=dict(comment_id=item['snippet']['topLevelComment']['id'],
                        video_id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        comment_text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        comment_author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        comment_published=published_date_format(item['snippet']['topLevelComment']['snippet']['publishedAt']))
                
                Comment_data.append(data)
                
    except:
        pass
    return Comment_data

#get store information about the channel and create tables in sql

def store_tables(channel_id):
    try:
        channel_check_flag = False
        if db.inspect(engine).has_table("channels"):

            channel_alldata = pd.read_sql_table('channels',schema = 'youtubeDB', con = conn)
            if channel_id in channel_alldata.values:
                channel_check_flag = True
                return 'Channel details already inserted in SQL DB !'

        if not channel_check_flag:

            channel_data = get_channel_data(channel_id)
            video_ids = get_video_ids(youtube, channel_id)
            video_data = get_video_info(youtube, video_ids)
            comment_data = get_comment_info(youtube, video_ids)
        
            channel_df = pd.DataFrame(channel_data,index=[0])
            video_df = pd.DataFrame(video_data)
            comment_df = pd.DataFrame(comment_data)
        
            channel_df.to_sql('channels', engine, if_exists='append', index=False)
            video_df.to_sql('videos', engine , if_exists='append', index=False)
            comment_df.to_sql('comments', engine , if_exists='append', index=False)
            
            return "Channels details stored Successfully !"
        else:
            return 'Error in storing Channel details !'
    except:
        return 'Error in storing Channel details !'

#get get information about the channel and create tables in sql

def get_tables(channel_id):
    channel_check_flag = False
    try:
        if db.inspect(engine).has_table("channels"):
            channel_alldata = pd.read_sql_table('channels',schema = 'youtubeDB', con = conn)
            if channel_id in channel_alldata.values:
                channel_check_flag = True
                print('Channel details already inserted in SQL DB')
            
        channel_data = get_channel_data(channel_id)

        return channel_check_flag,channel_data
    except:
        return False,False
  
def sidebar_bg(side_bg):

   side_bg_ext = 'png'

   st.markdown(
      f"""
      <style>
      [data-testid="stSidebar"] > div:first-child {{
          background: url(data:image/{side_bg_ext};base64,{base64.b64encode(open(side_bg, "rb").read()).decode()});
      }}
      </style>
      """,

      
      unsafe_allow_html=True,
      )
   
side_bg = r'C:\Users\RUDRA\project\capstone1\dw.png'
sidebar_bg(side_bg)

with st.sidebar:
    st.title("About the Project")
    st.header(":grey[Instructions]")
    st.caption("1) Go to the About Tab of the YouTube channel , Under the Stats section click the Share button and select Copy Channel ID. ")
    st.caption("2) Paste the Channel ID in the input box.")
    st.caption("3) By clicking Get Channel Details button , it will give the details of the Channel.")
    st.caption("4) By clicking Store Data button , it will store the details of the Channel in SQL.")
    st.caption("5) By selecting any one options from the list box will give the data analysis made on the stored tables.")



st.title(":blue[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
channel_id=st.text_input("Enter the channel ID")

# check if channel id is not null
if(channel_id):
    channel_id_check_flag_store = False
    store_data_flag= False

    # Action when get channel details is clicked
    if (st.button("Get Channel Details")):
        channel_id_check_flag,channel_id_check_data = get_tables(channel_id)#hari

        # check if the given channel id is already in SQL
        if channel_id_check_flag == True:
            channel_id_check_flag_store = True
            st.warning('Channel Details already in table !')

        # if not then print the channel details in dataframe
        else:
            if channel_id_check_data != False:
                channel_df = pd.DataFrame(channel_id_check_data,index=[0])
                display_channel_df = channel_df.iloc[: , [1,2,3,5]]
                #display_channel_df1=pd.DataFrame(display_channel_df,columns=["Channel Name","Video Count","Subscriber Count","Channel Description"])
                st.write(display_channel_df)
                store_data_flag = True

            # if channel id data is false then invalid channel id
            else:
                st.warning("Invalid Channel id !")              

    # Action when store datais clicked
    if st.button("Store Data"):
        channel_id_check_flag,channel_id_check_data = get_tables(channel_id)

        # check channel id is already saved in SQL
        if channel_id_check_flag == True:
            st.warning('Channel Details already in table. Can not store again !')
        else:
            # store the values in SQL
            if channel_id_check_data != False:
                rvalue = store_tables(channel_id)
                st.success(rvalue)
            else:
                #if not invalid channel id
                st.warning("Invalid Channel id !")     

sql = db.text('select channel_name,channel_description from channels order by channel_name') 
result = conn.execute(sql) 
df=pd.DataFrame(result,columns=["Channel Name","Channel Description"])
st.write(df)

# Query selection list

question=st.selectbox("Select your question",("Select any query from the dropdown",
                                              "1. All the Videos and the Channel Name",
                                              "2. Channels with most number of Videos",
                                              "3. Top 10 most viewed Videos",
                                              "4. Comments in each Videos",
                                              "5. Videos with highest likes",
                                              "6. Likes and Dislikes of all Videos",
                                              "7. Views of each Channel",
                                              "8. Videos published in the year of 2022",
                                              "9. Average duration of all Videos in each Channel",
                                              "10. Videos with highest number of Comments"))

if question=="1. All the Videos and the Channel Name":
# Query 1 What are the names of all the videos and their corresponding channels
    
    sql1 = db.text('select video_title,channel_name from videos') 
    result1 = conn.execute(sql1) 

    df1=pd.DataFrame(result1,columns=["Video Title","Channel Name"])
    st.write(df1)

elif question=="2. Channels with most number of Videos":
    # Query 2 Which channels have the most number of videos, and how many videos do they have?

    sql2 = db.text('select channel_name,channel_video_count from channels order by channel_view_count desc') 
    result2 = conn.execute(sql2) 

    df2=pd.DataFrame(result2,columns=["Channel Name","Videos Count"])
    st.write(df2)
    #st.plotly_chart(px.bar(df2,x='Channel Name',y='Views'))
    fig=px.bar(df2,x='Channel Name',y='Videos Count',color="Channel Name",

                #color_discrete_sequence=color_discrete_sequence,
                color_discrete_sequence=[
                 px.colors.qualitative.Bold[0],
                 px.colors.qualitative.Bold[1],
                 px.colors.qualitative.Bold[2],
                 px.colors.qualitative.Bold[3],
                 px.colors.qualitative.Bold[4],
                 px.colors.qualitative.Bold[5],
                 px.colors.qualitative.Bold[6],
                 px.colors.qualitative.Bold[7],
                 px.colors.qualitative.Bold[8],
                 px.colors.qualitative.Bold[9]],
                )
    st.plotly_chart(fig)


elif question=="3. Top 10 most viewed Videos":
# Query 3 What are the top 10 most viewed videos and their respective channels?

    sql3 = db.text('select video_title,channel_name,view_count from videos where view_count is not null order by view_count desc limit 10') 
    result3 = conn.execute(sql3) 

    df3=pd.DataFrame(result3,columns=["Video Title","Channel Name","Views"])
    st.write(df3)

    fig=px.bar(df3,x='Video Title',y='Views')
        # overwrite tick labels    
    #fig.update_layout(
     #   xaxis = {
        #'tickmode': 'array',
        #'tickvals': list(range(length)),
      ## 'tickangle':60
        #}
    #)
    st.plotly_chart(fig)


elif question=="4. Comments in each Videos":
# Query 4 How many comments were made on each video, and what are their corresponding video names?

    sql4 = db.text('select video_title,comments_count from videos where comments_count is not null order by comments_count desc') 
    result4 = conn.execute(sql4) 

    df4=pd.DataFrame(result4,columns=["Video Title","Comments Count"])
    st.write(df4)

elif question=="5. Videos with highest likes":
# Query 5 Which videos have the highest number of likes, and what are their corresponding channel names?

    sql5 = db.text('select video_title,channel_name,like_count from videos where like_count is not null order by like_count desc') 
    result5 = conn.execute(sql5) 

    df5=pd.DataFrame(result5,columns=["Video Title","Channel Name","Likes Count"])
    st.write(df5)
    

elif question=="6. Likes and Dislikes of all Videos":
# Query 6 What is the total number of likes and dislikes for each video, and what are their corresponding video names?

    sql6 = db.text('select video_title,like_count,dislike_count from videos where like_count order by video_title') 
    result6 = conn.execute(sql6) 

    df6=pd.DataFrame(result6,columns=["Video Title","Likes Count","Dislike Count"])
    st.write(df6)

elif question=="7. Views of each Channel":
# Query 7 What is the total number of views for each channel, and what are their corresponding channel names?

    
        sql7 = db.text('select channel_name,channel_view_count from channels order by channel_name') 
        result7 = conn.execute(sql7) 
        df7=pd.DataFrame(result7,columns=["Channel Name","Views Count"])
        st.write(df7)
        # plotting a bar graph 

        fig=px.bar(df7,x='Channel Name',y='Views Count',color="Channel Name",

                #color_discrete_sequence=color_discrete_sequence,
                color_discrete_sequence=[
                 px.colors.qualitative.Bold[0],
                 px.colors.qualitative.Bold[1],
                 px.colors.qualitative.Bold[2],
                 px.colors.qualitative.Bold[3],
                 px.colors.qualitative.Bold[4],
                 px.colors.qualitative.Bold[5],
                 px.colors.qualitative.Bold[6],
                 px.colors.qualitative.Bold[7],
                 px.colors.qualitative.Bold[8],
                 px.colors.qualitative.Bold[9]],
                )
        st.plotly_chart(fig)
        #st.plotly_chart(px.bar(df7,x='Channel Name',y='Views Count'))
       
        
    

elif question=="8. Videos published in the year of 2022":
# Query 8 What are the names of all the channels that have published videos in the year 2022?

    sql8 = db.text('select video_title,published_date from videos where published_date is not null and year(published_date) = 2022 order by channel_name') 
    result8 = conn.execute(sql8) 

    df8=pd.DataFrame(result8,columns=["Video Title","Published Date"])
    st.write(df8)

elif question=="9. Average duration of all Videos in each Channel":
# Query 9 What is the average duration of all videos in each channel, and what are their corresponding channel names?

    sql9 = db.text('select channel_name,round(avg(duration),2) from videos group by channel_name order by channel_name') 
    result9 = conn.execute(sql9) 

    df9=pd.DataFrame(result9,columns=["Channel Name","Average Duration"])
    st.write(df9)
    
    #st.plotly_chart(px.bar(df9,x='Channel Name',y='Average Duration'))
    fig = px.pie(df9,values='Average Duration',names='Channel Name')
    st.plotly_chart(fig)


elif question=="10. Videos with highest number of Comments":
# Query 10 Which videos have the highest number of comments, and what are their corresponding channel names?

    sql10 = db.text('select video_title,channel_name,comments_count from videos where comments_count is not null order by comments_count desc') 
    result10 = conn.execute(sql10) 

    df10=pd.DataFrame(result10,columns=["Video Title","Channel Name","Comments Count"])
    st.write(df10)

