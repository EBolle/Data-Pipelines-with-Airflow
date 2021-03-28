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

"""

songs = """

"""

artists = """

"""

time = """

"""