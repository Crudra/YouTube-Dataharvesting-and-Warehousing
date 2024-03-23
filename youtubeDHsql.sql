use youtubedb;
SET NAMES utf8mb4; 
ALTER DATABASE youtubedb CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci;


create table if not exists channels(	channel_id varchar(80) primary key,
					channel_name varchar(100),
                                    	channel_video_count int,
					channel_subscriber_count bigint,
					channel_view_count bigint,														channel_description text,
					channel_playlist_id varchar(80));


create table if not exists videos(		    channel_name varchar(100),
                                                    channel_id varchar(100),
                                                    video_id varchar(30) primary key,
                                                    video_title varchar(150),
                                                    video_description text,
                                                    view_count bigint,
                                                    like_count bigint,
                                                    dislike_count bigint,
                                                    comments_count int,
                                                    favorite_count int,
                                                    thumbnail varchar(200),
                                                    published_date timestamp,
                                                    duration time,
													definition varchar(10),
                                                    caption_status varchar(50));


create table if not exists comments(comment_id varchar(100) primary key,
                                                        video_id varchar(50),
                                                        comment_text text,
                                                        comment_author varchar(150),
                                                        comment_published timestamp);




