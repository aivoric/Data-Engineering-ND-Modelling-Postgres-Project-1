# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id SERIAL PRIMARY KEY
    , start_time bigint NOT NULL
    , user_id int NOT NULL
    , level varchar NOT NULL
    , song_id varchar
    , artist_id varchar
    , session_id int NOT NULL
    , location varchar NOT NULL
    , user_agent varchar NOT NULL
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id int PRIMARY KEY
    , first_name varchar NOT NULL
    , last_name varchar NOT NULL
    , gender varchar(1) NOT NULL
    , level varchar(10) NOT NULL
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id varchar PRIMARY KEY
    , title varchar NOT NULL
    , artist_id varchar NOT NULL
    , year smallint NOT NULL
    , duration float NOT NULL
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time bigint PRIMARY KEY
    , hour smallint NOT NULL
    , day smallint NOT NULL
    , week smallint NOT NULL
    , month smallint NOT NULL
    , year smallint NOT NULL
    , weekday smallint NOT NULL
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar PRIMARY KEY
    , name varchar NOT NULL
    , location varchar
    , latitude float
    , longitude float
)
""")

# INSERT RECORDS

user_table_insert = ("""
INSERT INTO users (
    user_id 
    , first_name
    , last_name
    , gender
    , level
)
VALUES (%s,%s,%s,%s,%s)
ON CONFLICT (user_id)
DO UPDATE SET 
    first_name  = EXCLUDED.first_name
    , last_name = EXCLUDED.last_name
    , gender = EXCLUDED.gender
    , level = EXCLUDED.level
""")

song_table_insert = ("""
INSERT INTO songs (
    song_id
    , title
    , artist_id
    , year
    , duration
)
VALUES (%s,%s,%s,%s,%s)
ON CONFLICT (song_id)
DO UPDATE SET 
    title  = EXCLUDED.title
    , artist_id = EXCLUDED.artist_id
    , year = EXCLUDED.year
    , duration = EXCLUDED.duration
""")

artist_table_insert = ("""
INSERT INTO artists (
    artist_id
    , name
    , location
    , latitude
    , longitude
)
VALUES (%s,%s,%s,%s,%s)
ON CONFLICT (artist_id)
DO UPDATE SET 
    name  = EXCLUDED.name
    , location = EXCLUDED.location
    , latitude = EXCLUDED.latitude
    , longitude = EXCLUDED.longitude
""")

time_table_insert = ("""
INSERT INTO time (
    start_time
    , hour
    , day
    , week
    , month
    , year
    , weekday
)
VALUES (%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT DO NOTHING
""")


# TEMP SONGPLAY TABLE
# -------------------------------------
# The following SQL statements are used for simple bulk uploading data via a temp table
# where no additional merging / manipulation of data is required

create_temp_table = "CREATE TEMP TABLE temp_table AS SELECT * FROM {} WITH NO DATA;"
copy_to_temp_table = "COPY temp_table FROM '{}' DELIMITER ',' CSV;"
insert_into_temp_table = "INSERT INTO {} SELECT * FROM temp_table ON CONFLICT DO NOTHING;"
drop_temp_table = "DROP TABLE temp_table;"


# TEMP SONGPLAY TABLE
# -------------------------------------
# The following SQL statements are used for processing the songplay log data.
# A temporary table is created to upload CSV data
# Data is then upserted into songplay while also getting artist_id and song_id

create_temp_songplay_table = ("""
CREATE TEMP TABLE IF NOT EXISTS temp_songplay_table (
    start_time bigint NOT NULL
    , user_id int NOT NULL
    , level varchar NOT NULL
    , session_id int NOT NULL
    , location varchar NOT NULL
    , user_agent varchar NOT NULL
    , song_name varchar NOT NULL
    , artist_name varchar NOT NULL
    , song_duration float NOT NULL
)
""")

copy_to_temp_songplay_table = "COPY temp_songplay_table FROM '{}' DELIMITER ',' CSV;"

insert_into_temp_songplay_data = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT tt.start_time, tt.user_id, tt.level, s.song_id, s.artist_id, tt.session_id, tt.location, tt.user_agent
    FROM temp_songplay_table tt
    LEFT JOIN ( 
        SELECT songs.song_id, songs.title, songs.duration, songs.artist_id, artists.name 
        FROM songs
        JOIN artists ON artists.artist_id = songs.artist_id) s
    ON s.title = tt.song_name AND s.duration = tt.song_duration AND s.name = tt.artist_name 
    ON CONFLICT DO NOTHING
""")

drop_temp_songplay_table = "DROP TABLE temp_songplay_table;"


# TESTS

select_test_records = ("""
    SELECT * FROM {} LIMIT {}
""")

select_not_null_artist_and_song_ids = ("""
    SELECT * FROM songplays 
    WHERE artist_id IS NOT NULL
    AND song_id IS NOT NULL
""")


# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]