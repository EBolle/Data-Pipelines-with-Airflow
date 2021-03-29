# SQL statements that can be used for quality checks given the SQLCheckOperator so every statement that returns a 0 or
# False will raise an error (https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/operators/sql/index.html)


songplays = """
SELECT count(*) as n_rows
,    count(distinct songplay_id) = count(*) as n_unique_id_check
,    min(extract(year from start_time)) = 2018 & max(extract(year from start_time)) = 2018 as year_check
,    min(extract(month from start_time)) = 11 & max(extract(month from start_time)) = 11 as month_check

FROM
    public.songplays
;
"""


users = """
SELECT count(*) as n_rows
,    count(distinct user_id) = count(*) as n_unique_id_check
,    sum(case when level in ('free', 'paid') then 1 else 0 end) = count(*) as level_check
,    sum(case when gender in ('M', 'F') then 1 else 0 end) = count(*) as gender_check

FROM
    public.users
;
"""


songs = """
SELECT count(*) as n_rows
,    count(distinct song_id) = count(*) as n_unique_id_check
,    sum(case when duration > 0 then 1 else 0 end) = count(*) as duration_check

FROM
    public.songs
;
"""


artists = """
SELECT count(*) as n_rows
,    count(distinct artist_id) = count(*) as n_unique_id_check

FROM
    public.artists
;
"""


time = """
SELECT count(*) as n_rows
,    count(distinct start_time) = count(*) as n_unique_id_check
,    min(extract(year from start_time)) = 2018 & max(extract(year from start_time)) = 2018 as year_check
,    min(extract(month from start_time)) = 11 & max(extract(month from start_time)) = 11 as month_check
,    sum(case when weekday in (True, False) then 1 else 0 end) = count(*) as weekday_check

FROM
    public.time
;
"""
