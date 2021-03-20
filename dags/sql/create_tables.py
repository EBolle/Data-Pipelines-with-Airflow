  # CREATE TABLE statements on the Redshift cluster


staging_events = """
DROP TABLE IF EXISTS public.staging_events;

CREATE TABLE public.staging_events (
    artist text,
    auth text,
    firstName text,
    gender text,
    itemInSession int,
    lastName text,
    length real,
    level text,
    location text,
    method text,
    page text,
    registration real,
    sessionId int,
    song text,
    status int,
    ts bigint sortkey,
    userAgent text,
    userId text)
diststyle even
;
"""


staging_songs = """
DROP TABLE IF EXISTS public.staging_songs;

CREATE TABLE staging_songs (
    num_songs int,
    artist_id text,
    artist_latitude real,
    artist_longitude real,
    artist_location text,
    artist_name text,
    song_id text,
    title text,
    duration real,
    year int)
diststyle even 
;
"""