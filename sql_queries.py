import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = ""
staging_songs_table_drop = ""
songplay_table_drop = ""
user_table_drop = ""
song_table_drop = ""
artist_table_drop = ""
time_table_drop = ""

# CREATE TABLES

staging_events_table_create= ("""
""")

staging_songs_table_create = ("""
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay (
        songplay_id IDENTITY(0, 1) PRIMARY KEY,
        start_time BIGINT REFERENCES time,
        user_id INT REFERENCES user,
        song_id VARCHAR REFERENCES song,
        artist_id VARCHAR REFERENCES artist,
        session_id INT,
        user_agent VARCHAR
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS user (
        user_id VARCHAR PRIMARY KEY,
        first_name VARCHAR,
        last_name VARCHAR,
        gender VARCHAR(1),
        level VARCHAR(4)
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS song (
        song_id VARCHAR PRIMARY KEY,
        title VARCHAR,
        artist_id VARCHAR REFERENCES artist,
        year INT,
        duration FLOAT
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artist (
        artist_id VARCHAR,
        name VARCHAR,
        location VARCHAR,
        lattitude FLOAT,
        longitude FLOAT
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time BIGINT PRIMARY KEY,
        hourt INT,
        day INT,
        week INT,
        month INT,
        year INT,
        weekday INT
    );
""")

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
