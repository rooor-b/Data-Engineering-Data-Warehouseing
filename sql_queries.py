import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events(
    staging_events_id INTEGER  IDENTITY(0,1)  PRIMARY KEY,
    artist         VARCHAR,
    auth           VARCHAR,
    firstName      VARCHAR,
    gender         VARCHAR(1),
    itemInSession  INTEGER,
    lastName       VARCHAR,
    length         DECIMAL,
    level          VARCHAR,
    location       VARCHAR,
    method         VARCHAR(6),
    page           VARCHAR,
    registration   VARCHAR,
    sessionId      INTEGER,
    song           VARCHAR,
    status         INTEGER,
    ts             bigint,
    userAgent      VARCHAR,
    userId         INTEGER
    ) DISTKEY("song") SORTKEY("ts");
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    staging_songs_id INTEGER  IDENTITY(0,1)  PRIMARY KEY,
    num_songs         INTEGER,
    artist_id         VARCHAR,
    artist_latitude   NUMERIC,
    artist_longitude  NUMERIC,
    artist_location   VARCHAR,
    artist_name       VARCHAR,
    song_id           VARCHAR,
    title             VARCHAR,
    duration          NUMERIC,
    year              INTEGER
    )DISTKEY("title");
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
    songplay_id  INTEGER    IDENTITY(0,1) PRIMARY KEY,
      start_time   TIMESTAMP  REFERENCES time(start_time)  SORTKEY,
      user_id      INTEGER    REFERENCES users(user_id),
      level        VARCHAR,
      song_id      VARCHAR    REFERENCES song(song_id)     DISTKEY,
      artist_id    VARCHAR    REFERENCES artist(artist_id),
      session_id   INTEGER,
      location     VARCHAR,
      user_agent   VARCHAR
    );
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
      user_id INTEGER PRIMARY KEY  SORTKEY,
      first_name  VARCHAR,
      last_name   VARCHAR,
      gender      VARCHAR,
      level       VARCHAR
    ) DISTSTYLE ALL;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song (
      song_id    VARCHAR  PRIMARY KEY  SORTKEY  DISTKEY,
      title      VARCHAR,
      artist_id  VARCHAR,
      year       INTEGER,
      duration   INTEGER
    );
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist(
      artist_id  VARCHAR  PRIMARY KEY  SORTKEY,
      name       VARCHAR,
      location   VARCHAR,
      latitude   DECIMAL,
      longitude  DECIMAL
    ) DISTSTYLE ALL;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
    start_time  TIMESTAMP   PRIMARY KEY  SORTKEY,
    hour        INTEGER,
    day         INTEGER,
    week        INTEGER,
    month       INTEGER,
    year        INTEGER,
    weekday     INTEGER
    ) DISTSTYLE ALL;
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events
    FROM '{}'
    CREDENTIALS 'aws_iam_role={}'
    JSON AS '{}'
    REGION 'us-west-2';
""").format(config.get('S3','LOG_DATA'),config.get('IAM_ROLE', 'ARN'),config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
    COPY staging_songs
    FROM '{}'
    CREDENTIALS 'aws_iam_role={}'
    json 'auto' compupdate off
    REGION 'us-west-2';
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays
(
      start_time,
      user_id,
      level,
      song_id,
      artist_id,
      session_id,
      location,
      user_agent
    )
    SELECT 
       (select TIMESTAMP 'epoch' + (events.ts/1000) * INTERVAL '1 Second ')  as start_time,
      events.userid,
      events.level,
      songs.song_id,
      songs.artist_id,
      events.sessionid,
      events.location,
      events.useragent
    FROM staging_events as events
    JOIN staging_songs  as songs
    ON 
      events.artist = songs.artist_name and
      events.song   = songs.title       and
      events.length = songs.duration;
""")

user_table_insert = ("""
INSERT INTO users(
      user_id,
      first_name,
      last_name,
      gender,
      level
    )
    SELECT 
      userid, 
      firstname, 
      lastname, 
      gender, 
      level
    FROM staging_events
    WHERE userId IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO song (
      song_id,
      title,
      artist_id,
      year,
      duration
    )
    SELECT
      song_id,
      title,
      artist_id,
      year,
      duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artist (
      artist_id,
      name,
      location,
      latitude,
      longitude
    )
    SELECT
      artist_id,
      artist_name,
      artist_location,
      artist_latitude,
      artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO time (
      start_time,
      hour,
      day,
      week,
      month,
      year,
      weekday
    )
    SELECT
      start_time,
      EXTRACT(HOUR  FROM start_time) as hour,
      EXTRACT(DAY   FROM start_time) as day,
      EXTRACT(WEEK  FROM start_time) as week,
      EXTRACT(MONTH FROM start_time) as month,
      EXTRACT(YEAR  FROM start_time) as year,
      EXTRACT(DOW   FROM start_time) as weekday
    FROM (select TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 Second ' as start_time from staging_events) staging_event_times 
    WHERE start_time IS NOT NULL;
""")
# TEST QUERIES
songplay_table_query = ("""
select count(distinct s.user_id) from songplays s inner join artist a on s.artist_id=a.artist_id where a.name='Muse'
""")
user_table_query = ("""
select count(*) as listing_counts,s.user_id,u.first_name from songplays s inner join users u on s.user_id=u.user_id group by s.user_id,u.first_name order by listing_counts desc;
""")
# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,  user_table_create, song_table_create, artist_table_create, time_table_create,songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
analytic_queries = [songplay_table_query, user_table_query]

