# Project: Data Warehouse

## Purpose of the project

The goal of this project is to set up an Amazon Redshift datawarehouse to store songplay event data and let us efficiently analyse it by  different dimensions. This includes loading data from raw JSON format into staging tables and inserting rows to the fact and dimension tables while ensuring uniqueness and efficiency where necessary.

## Summary of data

- Song dataset:
    - 14,896 unique songs
    - 9553 unique artists
- Log dataset:
    - 8056 songplay events (ie. `page` = NextSong)
    - 94 unique `user_id`
    - 209 unique `song_id`
    - 194 unique `artist_id`

The song dataset contains way more songs and artists that actually appear in the log event dataset. To avoid  oversized dimension tables I will only include songs and artists in the relevant tables that appear at least once in the songplay data.

## Database schema

Star schema where `songplay` table functions as the central fact table. `user`, `artist`, `song` and `time` tables serve as dimension tables, each holding columns relevant to these entities.

__Notes:__

Even if the project brief contains `level` and `location` as columns on `songplay` table, I left them out. `level` is a column on `user` table, and `location` is in `artist` table, so they are available by simple joins. This way our fact table can be as streamlined as possible without any loss of information.

The remaining columns on `songplay` are mostly foreign keys of dimension tables (`user_id`, `song_id`, `artist_id`), except for `session_id` and `user_agent`. `session_id` might also be a foreign key to a hypothetical `session` table which could contain session level information, including `user_agent`.

Also it feels unnecessary to create the dimension table `time` for extracting values from a timestamp. I would simply add a timestamp column to the fact table itself, and let the analytical queries make the extraction of date components based on their needs. This would further simplify the schema.

## Table design

I have chosen the following optimization strategies for the final tables in the schema:

1. songplay: EVEN distribution style with `start_time` as sorting key.
2. user: ALL distribution style
3. artist: ALL distribution style
4. song: ALL distribution style
5. time: EVEN distribution style with `start_time` as sorting key.

Factors to consider in table design:

- `user`, `artist` and `song` dimension tables are currently small enough to easily fit on all slices. This might change if lot of new data is inserted, but currently it will work.
- `songplay` and `time` tables contain the same number of rows (and most likely will contain very similar number of rows in the future), and they are the largest tables. Currently they fit into all the slices, but in a more realistic scenario these would quickly outgrow a single slice, so I chose EVEN distribution with `start_time` as sorting key, so that similar values will most likely reside on the same slices.

## Files

```bash
.
├── README.md
├── create_tables.py
├── dwh.cfg
├── etl.py
├── manage_cluster.ipynb
├── requirements.txt
├── sql_queries.py
└── test.ipynb
```

### README.md

You are reading this.

### create_tables.py

Script for creating the necessary tables (and dropping them first if they exist). Uses queries from `sql_queries.py`.

### dwh.cfg

Config variables for the datawarehouse and the ETL process. I added a few extra to the provided template to support creating the Redshift cluster from code.

### etl.py

Script for the ETL process. Run `python create_tables.py` before running the ETL script. Uses queries from `sql_queries.py`.

### manage_cluster.ipynb

This notebook contains the necessary code for launching and tearing down a Redshift cluster. For running the code you need to have a `user.cfg` config file with valid AWS access key and secret.

### requirements.txt

Dependencies for running the project.

### sql_queries.py

This file contains the queries for creating staging and final tables, and also the insert statements for loading data from raw JSON files to the tables.

### test.ipynb

Notebook for prototyping and testing SQL queries.

## Queries

Redshift does not support `ON CONFLICT` clauses, therefore the upsert operations look a bit more complicated. I wrote queries that would ensure uniqueness during initial load and subsequent inserts.

### user_table_insert

We need to make sure that:

1. all `userId`s in `staging_event` are only inserted maximum once (if they are not already in `users` table in case of a new insert later)
2. after running the query all user rows will contain the latest value for `level`.

I created a temp table that contains the last event for every `userId` in `staging_event`, and used that in the insert statement and a separate update statement.

```sql
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
```

### song_table_insert

No upsert in this case, only making sure that:

1. there is no duplication during later inserts,
2. only those songs are included that are included in `songplay`

Redshift does not enforce uniqueness on primary keys during inserts (on top of missing support for `ON CONFLICT` clauses), so I joined the target table on the staging table and insert only those rows that are not present in the target table.

**Note:** This method implicitly asusmes two things:

1. `staging_song` contains every `song_id` only once, so we don't need to ensure uniqueness there
2. song details of a specific `song_id` won't change later, so we don't have to do updates or flag duplicates

Given the nature of the data source I think these are reasonable assumptions.

```sql
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
```

### artist_table_insert

This is very similarly to `song_table_insert`, with same assumptions on uniqueness and persistence of artist details. The latter is somewhat weaker given the artists' location might change, but for the sake of keeping this project as lean as possible I just went with it.

Also only those artists are inserted that are included in `songplay` data.

```sql
INSERT INTO artist
SELECT
    artist_id,
    staging_song.name,
    staging_song.location,
    staging_song.lattitude,
    staging_song.longitude
FROM staging_song
LEFT JOIN artist USING (artist_id)
WHERE artist.name IS NULL AND artist_id IN (SELECT DISTINCT artist_id FROM songplay);
```

### time_table_insert

The extra twist here is that we need to transform integer `ts` into timestamps to be able to extract the necessary components. I solved this in the `converted` CTE.

The insert query also ensures uniqueness of `start_time` during later inserts, just as in the previous queries. This is somewhat overkill for this project as resolution of `start_time` is millisecond, but in a truly large application multiple events can be recorded even in the same millisecond.

```sql
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
```

## Running the scripts

1. To set up Redshift cluster, run cells in `manage_cluster.ipynb` notebook. **WARNING:** the script will look for AWS credentials in `users.cfg` which is not included in the repo.
2. Create staging and final tables by running `python create_tables.py`
3. Run ETL pipeline by running `python etl.py`

## Additional notes

1. I added my AWS user via an extra config file names `user.cfg`, which is not included in the repo to avoid sharing my credentials. This file is necessary to run commands in `manage_cluster.ipynb`.
2. I added a bunch of extra config paramteres to `dwh.cfg`. These are used in the scripts that create and delete the Redshift cluster. I also had to modify a bit how `create_tables.py` and `etl.py` reads in values for constructing the connection string.

