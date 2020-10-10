# Project: Data Warehouse

## Purpose of the project [WIP]

...

## Database schema

The schema is a star schema where `songplay` table functions as the central fact table, and `user`, `artist`, `song` and `time` tables serve as dimension tables, each holding columns relevant to these entities.

__Note:__ Even if the project brief contains `level` and `location` as columns on `songplay` table, I left them out. `level` is a column on `user` table, and `location` is in `artist` table, so they are available by simple joins. This way our fact table can be as streamlined as possible without any loss of information. 

The remaining columns on `songplay` are mostly actual foreign keys of dimension tables (`user_id`, `song_id`, `artist_id`), except for `session_id` and `user_agent`. `session_id` might also be a foreign key to a `session` table which contains session level information, while `user_agent` should be a column on this dimension.

## Table design [WIP]

I have chosen the following optimization strategies for the final tables in the schema:

1. songplay: EVEN distribution style with `(user_id, artist_id, song_id, start_time)` as a composite sorting key
2. user: AUTO distribution style with `???` as sorting key (in case AWS chooses to distribute the table in EVEN fashion)
3. artist: AUTO distribution style with `???` as sorting key (in case AWS chooses to distribute the table in EVEN fashion)
4. song: AUTO distribution style with `???` as sorting key (in case AWS chooses to distribute the table in EVEN fashion)

## Files [WIP]

```bash
.
├── create_tables.py
├── dwh.cfg
├── etl.py
├── README.md
└── sql_queries.py
```

## Running the scripts [WIP]


## Notes

1. I added my AWS user credentials in an extra config file names `user.cfg`, which I deleted before submitting the project to avoid sharing my credentials. This file is necessary to run 

2. I added a bunch of extra config paramteres to `dwh.cfg`. These are used in the scripts that create and delete the Redshift cluster. I also had to modify 