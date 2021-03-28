# INSERT INTO statements on the Redshift cluster


songplays = """
INSERT INTO public.songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT timestamp 'epoch' + CAST(events.ts AS BIGINT)/1000 * interval '1 second' AS start_time 
,    cast(events.userid as int) as user_id
,    events.level
,    song.song_id
,    song.artist_id
,    events.sessionid as session_id
,    events.location
,    events.useragent as user_agent

FROM
    (
    SELECT *
        
    FROM
        public.staging_events events
        
    WHERE
        page = 'NextSong'
    ) as events
    left join public.staging_songs as song on song.artist_name = events.artist
                                          and song.title = events.song
                                          and song.duration = events.length

; 
"""


users = """
SELECT cast(userid as int) 
,    firstname
,    lastname
,    gender
,    level

FROM
    (
    SELECT userid
    ,    firstname
    ,    lastname
    ,    gender
    ,    level
    ,    row_number() over (partition by userid order by ts desc) as row_number_ts_desc
    
    FROM
        public.staging_events
        
    WHERE
        userid != ''
    )
    
WHERE
    row_number_ts_desc = 1 
;
"""


songs = """
SELECT song_id
,    title
,    artist_id
,    year
,    duration

FROM
    public.staging_songs
;
"""


artists = """
SELECT artist_id 
,    max(artist_name) as artist_id
,    max(artist_location) as location
,    max(artist_latitude) as latitude
,    max(artist_longitude) as longitude

FROM
    public.staging_songs
    
GROUP BY
    artist_id
;
"""


time = """
SELECT timestamp as start_time
,    extract(hour from timestamp) as hour
,    extract(day from timestamp) as day
,    extract(week from timestamp) as week
,    extract(month from timestamp) as month
,    extract(year from timestamp) as year
,    case when extract(weekday from timestamp) between 1 and 5 then TRUE else FALSE end as weekday

FROM
    ( 
    SELECT DISTINCT timestamp 'epoch' + CAST(ts AS BIGINT)/1000 * interval '1 second' AS timestamp 
    
    FROM
        public.staging_events

    WHERE
        page='NextSong'
    )
;  
"""
