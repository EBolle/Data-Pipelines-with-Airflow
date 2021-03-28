# SQL statements that can be used for quality checks given the SQLCheckOperator so every statement that returns a 0
# or False will raise an error (https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/operators/sql/index.html)


songplays = """
SELECT count(*) as n_rows
,    count(distinct songplay_id) = count(*) as n_unique_id_check
,    min(extract(year from start_time)) = 2018 & max(extract(year from start_time)) = 2018
,    min(extract(month from start_time)) = 11 & max(extract(month from start_time)) = 11

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
"""

songs = """

"""

artists = """

"""

time = """

"""