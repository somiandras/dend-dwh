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

staging_events_copy = (
    """
        copy staging_event from {bucket}
        credentials 'aws_iam_role={arn}'
        region 'us-west-2'
        json {jsonpath};
    """
).format(
    bucket=config.get("S3", "LOG_DATA"),
    arn=config.get("IAM_ROLE", "ARN"),
    jsonpath=config.get("S3", "LOG_JSONPATH"),
)

staging_songs_copy = (
    """
        copy staging_song from {bucket}
        credentials 'aws_iam_role={arn}'
        region 'us-west-2'
        json 'auto';
    """
).format(bucket=config.get("S3", "SONG_DATA"), arn=config.get("IAM_ROLE", "ARN"))

# FINAL TABLES

songplay_table_insert = """
    INSERT INTO songplay (
        start_time,
        user_id,
        song_id,
        artist_id,
        session_id,
        user_agent
    )
    SELECT
        ts as start_time,
        userId as user_id,
        song_id,
        artist_id,
        sessionId as session_id,
        userAgent as user_agent
    FROM staging_event e
    LEFT JOIN staging_song s ON
        e.artist = s.artist_name AND
        e.song = s.title AND
        e.length = s.duration;
"""

user_table_insert = """
    -- Get last songplay event for every userId in staging table
    CREATE TEMP TABLE user_last_song AS
    SELECT staging_event.*
    FROM (
        SELECT userId, max(ts) as ts
        FROM staging_event
        WHERE page = 'NextSong'
        GROUP BY userId
    ) last_ts
    LEFT JOIN staging_event USING (userId, ts)
    WHERE page = 'NextSong';

    -- Insert rows for userIds that are not already in users table
    INSERT INTO users
    SELECT
        uls.userId as user_id,
        uls.firstName as first_name,
        uls.lastName as last_name,
        uls.gender,
        uls.level
    FROM user_last_song uls
    LEFT JOIN users ON users.user_id = uls.userId
    WHERE users.user_id IS NULL;

    -- Update existing users' level to latest value where it changed
    UPDATE users
    SET level = user_last_song.level
    FROM user_last_song
    WHERE
        user_id = user_last_song.userId AND
        users.level != user_last_song.level;
"""

song_table_insert = """
    INSERT INTO song
    SELECT
        song_id,
        staging_song.title,
        staging_song.artist_id,
        staging_song.year,
        staging_song.duration
    FROM staging_song
    LEFT JOIN song USING (song_id)
    WHERE
        song.title IS NULL AND
        song_id IN (SELECT DISTINCT song_id FROM songplay);
"""

artist_table_insert = """
    INSERT INTO artist
    SELECT
        artist_id,
        staging_song.artist_name AS name,
        staging_song.artist_location AS location,
        staging_song.artist_latitude AS latitude,
        staging_song.artist_longitude AS longitude
    FROM staging_song
    LEFT JOIN artist USING (artist_id)
    WHERE
        artist.name IS NULL AND
        artist_id IN (SELECT DISTINCT artist_id FROM songplay);
"""

time_table_insert = """
    INSERT INTO time
    WITH converted AS (
        SELECT
            ts,
            'epoch'::timestamp + ts / 1000.0 * '1 second'::interval AS ts_converted
        FROM staging_event
    )
    SELECT
        ts AS start_time,
        date_part('hour', ts_converted) AS hour,
        date_part('day', ts_converted) AS day,
        date_part('week', ts_converted) AS week,
        date_part('month', ts_converted) AS month,
        date_part('year', ts_converted) AS year,
        date_part('dow', ts_converted) AS weekday
    FROM converted
    LEFT JOIN time ON time.start_time = converted.ts
    WHERE time.start_time IS NULL;
"""

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    time_table_create,
    artist_table_create,
    song_table_create,
    user_table_create,
    songplay_table_create,
]
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
]
