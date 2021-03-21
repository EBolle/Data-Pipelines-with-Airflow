  -- CREATE TABLE statements on the Redshift cluster

 -- DROP the tables if they do exist to start with a fresh schema

DROP TABLE if EXISTS public.staging_events;
DROP TABLE if EXISTS public.staging_songs;
DROP TABLE if EXISTS public.songplays;
DROP TABLE if EXISTS public.users;
DROP TABLE if EXISTS public.songs;
DROP TABLE if EXISTS public.artists;
DROP TABLE if EXISTS public.time;

  -- CREATE the tables

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
diststyle even;


CREATE TABLE public.staging_songs (
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


CREATE TABLE public.songplays (
    songplay_id int identity(0, 1) PRIMARY KEY,
    start_time timestamp sortkey,
    user_id int,
    level text,
    song_id text,
    artist_id text,
    session_id int,
    location text,
    user_agent text)
diststyle even
;

CREATE TABLE public.users (
    user_id int primary key,
    first_name text,
    last_name text,
    gender text,
    level text)
diststyle all;


CREATE TABLE public.songs (
    song_id text primary key,
    title text,
    artist_id text,
    year int,
    duration real)
diststyle all
;


CREATE TABLE public.artists (
    artist_id text primary key,
    name text,
    location text,
    latitude real,
    longitude real)
diststyle all
;


CREATE TABLE public.time (
    start_time timestamp primary key sortkey,
    hour int,
    day int,
    week int,
    month int,
    year int,
    weekday boolean)
diststyle even
;
