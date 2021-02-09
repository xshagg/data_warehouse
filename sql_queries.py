import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events"
staging_songs_table_drop = "drop table if exists staging_songs"
songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES

staging_events_table_create = ("""
    create table staging_events(
        event_id bigint identity (0,1),
        artist_name varchar (255),
        auth varchar (50),
        user_first_name varchar (255),
        user_gender char (1),
        item_in_session int,
        user_last_name varchar (255),
        song_length double precision,
        user_level varchar (50),
        location varchar (255),
        method varchar (10),
        page varchar (50),
        registration varchar (50),
        session_id bigint,
        song_title varchar (255),
        status int,
        ts varchar (50),
        user_agent varchar (255),
        user_id int,
        PRIMARY KEY (event_id)
    )
""")

staging_songs_table_create = ("""
    create table staging_songs(
        song_id varchar (32),
        num_songs int,
        artist_id varchar (100),
        artist_latitude double precision,
        artist_longitude double precision,
        artist_location varchar (255),
        artist_name varchar (255),
        title varchar (255),
        duration double precision,
        year int,
        PRIMARY KEY (song_id)
    )
""")

songplay_table_create = ("""
    create table songplays(
        songplay_id bigint identity (0,1),
        start_time timestamp,
        user_id int,
        level varchar (16),
        song_id varchar (32),
        artist_id varchar (32),
        session_id bigint,
        location varchar (255),
        user_agent TEXT,
        PRIMARY KEY (songplay_id)
    )
""")

user_table_create = ("""
    create table users(
        user_id int,
        first_name varchar (255),
        last_name varchar (255),
        gender char (1),
        level varchar (16),
        PRIMARY KEY (user_id)
    )
""")

song_table_create = ("""
    create table songs(
        song_id varchar (32),
        title varchar (255),
        artist_id varchar (100),
        year int,
        duration double precision,
        PRIMARY KEY (song_id)
    )
""")

artist_table_create = ("""
    create table artists(
        artist_id varchar (32),
        name varchar (255),
        location varchar (255),
        latitude double precision,
        longitude double precision,
        PRIMARY KEY (artist_id)
    )
""")

time_table_create = ("""
    create table time(
        start_time timestamp,
        hour int,
        day int,
        week int,
        month int,
        year int,
        weekday int,
        PRIMARY KEY (start_time)
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events
    from {}
    iam_role {}
    json {};
""").format(
    config.get('S3', 'LOG_DATA'),
    config.get('IAM_ROLE', 'ARN'),
    config.get('S3', 'LOG_JSONPATH')
)

staging_songs_copy = ("""
    copy staging_songs
    from {}
    iam_role {}
    json 'auto';
""").format(
    config.get('S3', 'SONG_DATA'),
    config.get('IAM_ROLE', 'ARN')
)

# FINAL TABLES

# Thanks Tim Biegeleisen for this really weird string to timestamp conversion
# https://stackoverflow.com/questions/54193335/how-to-cast-bigint-to-timestamp-with-time-zone-in-postgres-in-an-update
songplay_table_insert = ("""
    insert into songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    select 
        timestamp 'epoch' + e.ts/1000 * interval '1 second' as start_time,
        e.user_id,
        e.user_level,
        s.song_id,
        s.artist_id,
        e.session_id,
        e.location,
        e.user_agent
    from staging_events e
    join staging_songs s on e.song_title = s.title and e.song_length = s.duration and e.artist_name = s.artist_name
    where e.page = 'nextsong'
""")

user_table_insert = ("""
    insert into users (user_id, first_name, last_name, gender, level)
    select distinct 
        user_id,
        user_first_name,
        user_last_name,
        user_gender,
        user_level
    from staging_events
    where page = 'NextSong'
""")

song_table_insert = ("""
    insert into songs (song_id, title, artist_id, year, duration)
    select distinct
        song_id,
        title,
        artist_id,
        year,
        duration
    from staging_songs
    where song_id is not null
""")

artist_table_insert = ("""
    insert into artists (artist_id, name, location, latitude, longitude)
    select distinct
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    from staging_songs
    where artist_id is not null
""")

time_table_insert = ("""
    insert into time(start_time, hour, day, week, month, year, weekday)
    select start_time,
        extract(hour from start_time),
        extract(day from start_time),
        extract(week from start_time),
        extract(month from start_time),
        extract(year from start_time),
        extract(dayofweek from start_time)
    from songplays
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create,
                        user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert,
                        time_table_insert]
