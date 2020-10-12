import configparser


# CONFIG
config = configparser.ConfigParser()
config.read("dwh.cfg")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_event;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_song;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay CASCADE;"
user_table_drop = "DROP TABLE IF EXISTS users CASCADE;"
song_table_drop = "DROP TABLE IF EXISTS song CASCADE;"
artist_table_drop = "DROP TABLE IF EXISTS artist CASCADE;"
time_table_drop = "DROP TABLE IF EXISTS time CASCADE;"

# CREATE TABLES

staging_events_table_create = """
    CREATE TABLE IF NOT EXISTS staging_event (
        artist VARCHAR,
        auth VARCHAR,
        firstName VARCHAR,
        gender VARCHAR(1),
        itemInSession INT,
        lastName VARCHAR,
        length FLOAT,
        level VARCHAR(4),
        location VARCHAR,
        method VARCHAR(3),
        page VARCHAR,
        registration BIGINT,
        sessionId INT,
        song VARCHAR,
        status INT,
        ts BIGINT,
        userAgent VARCHAR,
        userId INT
    );
"""

staging_songs_table_create = """
    CREATE TABLE IF NOT EXISTS staging_song (
        num_songs INT,
        artist_id VARCHAR,
        artist_latitude FLOAT,
        artist_longitude FLOAT,
        artist_location VARCHAR,
        artist_name VARCHAR,
        song_id VARCHAR,
        title VARCHAR,
        duration FLOAT,
        year INT
    );
"""

songplay_table_create = """
    CREATE TABLE IF NOT EXISTS songplay (
        songplay_id BIGINT IDENTITY(1, 1) PRIMARY KEY,
        start_time BIGINT REFERENCES time sortkey,
        user_id INT REFERENCES users,
        song_id VARCHAR REFERENCES song,
        artist_id VARCHAR REFERENCES artist,
        session_id INT,
        user_agent VARCHAR
    )
    diststyle even;
"""

user_table_create = """
    CREATE TABLE IF NOT EXISTS users (
        user_id VARCHAR PRIMARY KEY,
        first_name VARCHAR,
        last_name VARCHAR,
        gender VARCHAR(1),
        level VARCHAR(4)
    )
    diststyle all;
"""

song_table_create = """
    CREATE TABLE IF NOT EXISTS song (
        song_id VARCHAR PRIMARY KEY,
        title VARCHAR,
        artist_id VARCHAR REFERENCES artist,
        year INT,
        duration FLOAT
    )
    diststyle all;
"""

artist_table_create = """
    CREATE TABLE IF NOT EXISTS artist (
        artist_id VARCHAR PRIMARY KEY,
        name VARCHAR,
        location VARCHAR,
        lattitude FLOAT,
        longitude FLOAT
    )
    diststyle all;
"""

time_table_create = """
    CREATE TABLE IF NOT EXISTS time (
        start_time BIGINT PRIMARY KEY sortkey,
        hourt INT,
        day INT,
        week INT,
        month INT,
        year INT,
        weekday INT
    )
    diststyle even;
"""

# STAGING TABLES

staging_events_copy = ("""
""").format()

staging_songs_copy = ("""
""").format()

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
