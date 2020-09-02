import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
SONG_DATA = config.get("S3","SONG_DATA")
LOG_DATA = config.get("S3","LOG_DATA")
LOG_JSONPATH = config.get("S3","LOG_JSONPATH")
DWH_ROLE_ARN=config.get("IAM_ROLE","ARN")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events(
event_id BIGINT IDENTITY(0,1) NOT NULL,
artist VARCHAR,
auth VARCHAR,
firstName VARCHAR,
gender VARCHAR,
iteminSession VARCHAR,
lastName VARCHAR,
length VARCHAR,
level VARCHAR,
location VARCHAR,
method VARCHAR,
page VARCHAR,
registration VARCHAR,
sessionid INTEGER,
song VARCHAR,
status INTEGER,
ts BIGINT,
userAgent VARCHAR,
userid INTEGER
)
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs(
num_songs INTEGER,
artist_id VARCHAR,
artist_latitude double precision,
artist_longitude double precision,
artist_location VARCHAR(500),
artist_name VARCHAR(500),
song_id VARCHAR,
title VARCHAR(500),
duration double precision,
year INTEGER)
""")

songplay_table_create = ("""
CREATE TABLE songplays(
songplay_id int IDENTITY(0,1) PRIMARY KEY,
start_time timestamp,
user_id VARCHAR(50),
level VARCHAR(50),
song_id VARCHAR(50),
artist_id VARCHAR(50),
session_id VARCHAR(50),
location VARCHAR(100),
user_agent VARCHAR(500)
)
""")

user_table_create = ("""
CREATE TABLE users(
user_id int IDENTITY(0,1) PRIMARY KEY,
first_name VARCHAR,
last_name VARCHAR,
gender VARCHAR,
level VARCHAR
)
""")

song_table_create = ("""
CREATE TABLE songs(
song_id int IDENTITY(0,1) PRIMARY KEY,
title VARCHAR,
artist_id VARCHAR,
year smallint,
duration double precision
)
""")

artist_table_create = ("""
CREATE TABLE artists(
artist_id int IDENTITY(0,1) PRIMARY KEY,
name VARCHAR,
location VARCHAR,
latitude double precision,
longitude double precision
)
""")

time_table_create = ("""
CREATE table time(
start_time timestamp,
hour smallint,
day smallint,
week smallint,
month smallint,
year smallint,
weekday smallint
)
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {}
    credentials 'aws_iam_role={}'
    format as json {}
    STATUPDATE ON
    region 'us-west-2';
""").format(LOG_DATA,DWH_ROLE_ARN,LOG_JSONPATH)

staging_songs_copy = ("""
COPY staging_songs FROM {}
    credentials 'aws_iam_role={}'
    format as json 'auto'
    ACCEPTINVCHARS AS '^'
    STATUPDATE ON
    region 'us-west-2';
""").format(SONG_DATA,DWH_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
 INSERT INTO songplays ( start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
    SELECT  DISTINCT TIMESTAMP 'epoch' + e.ts/1000 \
* INTERVAL '1 second'   AS start_time,
e.userId AS user_id,
e.level  AS level,
s.song_id  AS song_id,
s.artist_id AS artist_id,
e.sessionId AS session_id,
e.location  AS location,
e.userAgent AS user_agent
FROM staging_events AS e
JOIN staging_songs AS s
ON e.artist = s.artist_name
WHERE e.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users (first_name,last_name,gender,level)
SELECT
firstName AS first_name,
lastName AS last_name,
gender AS gender,
level AS level
FROM staging_events
WHERE page = 'NextSong';
""")

song_table_insert = ("""
INSERT INTO songs (title,artist_id,year,duration)
SELECT title,
artist_id,
year,
duration
FROM staging_songs;
""")

artist_table_insert = ("""
INSERT INTO artists (name,location,latitude,longitude)
SELECT 
s.artist_name AS name,
s.artist_location AS location,
s.artist_latitude AS latitude,
s.artist_longitude AS longitude
FROM staging_songs as s;
""")

time_table_insert = ("""
INSERT INTO time(start_time,hour,day,week,month,year,weekday)
SELECT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time,
EXTRACT(hour from start_time) AS hour,
EXTRACT(day from start_time) AS day,
EXTRACT(week from start_time) AS week,
EXTRACT(month from start_time) AS month,
EXTRACT(year from start_time) AS year,
EXTRACT(week from start_time) AS weekday
FROM staging_events
WHERE page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
