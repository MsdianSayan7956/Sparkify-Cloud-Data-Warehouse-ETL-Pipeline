import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

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
    CREATE TABLE staging_events (
        artist VARCHAR,
        auth VARCHAR,
        firstName VARCHAR,
        gender VARCHAR,
        itemInSession INTEGER,
        lastName VARCHAR,
        length FLOAT,
        level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration BIGINT,
        sessionId INTEGER,
        song VARCHAR,
        status INTEGER,
        ts BIGINT,
        userAgent VARCHAR,
        userId INTEGER
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs (
        num_songs INTEGER,
        artist_id VARCHAR,
        artist_latitude FLOAT,
        artist_longitude FLOAT,
        artist_location VARCHAR,
        artist_name VARCHAR,
        song_id VARCHAR,
        title VARCHAR,
        duration FLOAT,
        year INTEGER
    );
""")

songplay_table_create = ("""
    CREATE TABLE songplays (
        songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
        start_time TIMESTAMP NOT NULL SORTKEY DISTKEY,
        user_id INTEGER NOT NULL,
        level VARCHAR,
        song_id VARCHAR,
        artist_id VARCHAR,
        session_id INTEGER,
        location VARCHAR,
        user_agent VARCHAR
    );
""")

user_table_create = ("""
    CREATE TABLE users (
        user_id INTEGER NOT NULL PRIMARY KEY SORTKEY,
        first_name VARCHAR,
        last_name VARCHAR,
        gender VARCHAR,
        level VARCHAR
    ) DISTSTYLE ALL;
""")

song_table_create = ("""
    CREATE TABLE songs (
        song_id VARCHAR NOT NULL PRIMARY KEY SORTKEY,
        title VARCHAR NOT NULL,
        artist_id VARCHAR NOT NULL,
        year INTEGER,
        duration FLOAT
    ) DISTSTYLE ALL;
""")

artist_table_create = ("""
    CREATE TABLE artists (
        artist_id VARCHAR NOT NULL PRIMARY KEY SORTKEY,
        name VARCHAR NOT NULL,
        location VARCHAR,
        latitude FLOAT,
        longitude FLOAT
    ) DISTSTYLE ALL;
""")

time_table_create = ("""
    CREATE TABLE time (
        start_time TIMESTAMP NOT NULL PRIMARY KEY SORTKEY DISTKEY,
        hour INTEGER,
        day INTEGER,
        week INTEGER,
        month INTEGER,
        year INTEGER,
        weekday INTEGER
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF region 'us-west-2'
    FORMAT AS JSON {};
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    CREDENTIALS 'aws_iam_role={}'
    COMPUPDATE OFF region 'us-west-2'
    FORMAT AS JSON 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

# FIXED: Removed DISTINCT and added duration to JOIN condition for accurate matching
songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT 
        TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' as start_time,
        se.userId,
        se.level,
        ss.song_id,
        ss.artist_id,
        se.sessionId,
        se.location,
        se.userAgent
    FROM staging_events se
    LEFT JOIN staging_songs ss 
        ON se.song = ss.title 
        AND se.artist = ss.artist_name
        AND se.length = ss.duration
    WHERE se.page = 'NextSong'
    AND se.userId IS NOT NULL;
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT 
        userId,
        firstName,
        lastName,
        gender,
        level
    FROM staging_events
    WHERE userId IS NOT NULL
    AND page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT 
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT 
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT 
        start_time,
        EXTRACT(hour from start_time),
        EXTRACT(day from start_time),
        EXTRACT(week from start_time),
        EXTRACT(month from start_time),
        EXTRACT(year from start_time),
        EXTRACT(weekday from start_time)
    FROM songplays;
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert,