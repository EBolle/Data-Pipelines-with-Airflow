  # INSERT INTO statements on the Redshift cluster


songplays = """"""


users = """
INSERT INTO users 
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
        staging_events
    WHERE
        userid != ''
    )
WHERE
    row_number_ts_desc = 1 
;
"""


songs = """
INSERT INTO songs 
SELECT song_id
,    title
,    artist_id
,    year
,    duration
FROM
    staging_songs
;
"""


artists = """
INSERT INTO artists
SELECT artist_id 
,    max(artist_name) as artist_id
,    max(artist_location) as location
,    max(artist_latitude) as latitude
,    max(artist_longitude) as longitude
FROM
    staging_songs
GROUP BY
    artist_id
;
"""


time = """
INSERT INTO time
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
        staging_events

    WHERE
        page='NextSong'
    )
;  
"""