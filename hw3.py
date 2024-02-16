import psycopg
import datetime
import os

from hw1 import *


def select_all(func):
    def execute(**kargs):
        user = kargs["user"]
        host = kargs["host"]
        dbname = kargs["dbname"]
        with psycopg.connect(f"user='{user}' \
                            host='{host}' \
                            dbname='{dbname}'") as conn:
            with conn.cursor() as curs:
                curs.execute(func(**kargs))
                return curs.fetchall()
    return execute


def check_query_args(**kargs):
    query = kargs['query']
    if 'explain' in kargs and kargs['explain'] is True:
        query = 'EXPLAIN ANALYZE VERBOSE ' + query
    if 'n' in kargs:
        query = query + f" LIMIT {kargs['n']}"
    return query


def commit(func):
    """
    Q1. Complete the `commit()` decorator.
    This decorator should perform the following steps:
    a. Retrieve keyword arguments including
       `user`, `host`, `dbname`, and `isolation_level`.
    b. Create a connection using the `user`, `host`,
       and `dbname`, and set the isolation level.
    c. Execute a SQL query string returned from a function.
    d. Commit the changes.
    """
    def execute(**kargs):
        user = kargs['user']
        host = kargs['host']
        dbname = kargs['dbname']
        iso_level = kargs['isolation_level']
        conn = psycopg.connect(user=user, host=host, dbname=dbname,)
        cur = conn.cursor()
        conn.isolation_level = iso_level
        query = func(**kargs)
        cur.execute(query)
        conn.commit()

        cur.close()
        conn.close()

    return execute


@commit
def create_view_incident_with_details(**kargs):
    """
    Q2. Create a view called incident_with_details, that includes id,
    incident_datetime, incident_code, incident_category,
    incident_subcategory, incident_description, longitude,
    latitude, report_datetime, report_type_code, report_type_description,
    supervisor_district, police_district and neighborhood
    for all the rows in incident table.
    """
    query = f'''
    CREATE VIEW incident_with_details AS
    SELECT
        i.id,
        i.incident_datetime,
        i.incident_code,
        it.incident_category,
        it.incident_subcategory,
        it.incident_description,
        i.longitude,
        i.latitude,
        i.report_datetime,
        i.report_type_code,
        r.report_type_description,
        l.supervisor_district,
        l.police_district,
        l.neighborhood
    FROM incident i
    LEFT JOIN location l ON i.latitude = l.latitude
    AND i.longitude = l.longitude
    LEFT JOIN report_type r ON i.report_type_code = r.report_type_code
    LEFT JOIN incident_type it ON i.incident_code = it.incident_code;
            '''
    return check_query_args(query=query, **kargs)


@select_all
def daily_average_incident_increase(**kargs):
    """
    Q3. Complete the daily_average_incident_increase() function.
    This function connects to a database using the parameters
    user, host, dbname, and n. It returns n records of date
    and average_incident_increase.
    The date represents the date of incident_datetime,
    and average_incident_increase indicates
    the difference between the average number of incidents
    in the previous 6 days and the current date, and
    the average number of incidents between the current date
    and the next 6 days.
    The value is rounded to 2 decimal points (as float) and
    the records are ordered by date.
    If the parameter n is not provided, the function returns all rows.
    """
    query = f'''
    WITH incident_counts AS (
        SELECT
        CAST(DATE_TRUNC('day', incident_datetime) AS DATE) AS date,
        CAST(COUNT(*) AS NUMERIC) AS incident_count
    FROM incident
    GROUP BY CAST(DATE_TRUNC('day', incident_datetime) AS DATE)
    )
    SELECT
        date,
        CAST(
        ROUND(
            (AVG(incident_count) OVER (
            ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
            - AVG(incident_count) OVER (
            ORDER BY date ROWS BETWEEN CURRENT ROW AND 6 FOLLOWING)
            ), 2
        ) AS FLOAT
        ) AS average_incident_increase
    FROM incident_counts
    ORDER BY date
            '''
    return check_query_args(query=query, **kargs)


@select_all
def three_day_daily_report_type_ct(**kargs):
    """
    Q4. Complete the three_day_daily_report_type_ct() function.
    This function connects to a database using the parameters user,
    host, dbname, and n.
    It returns n records for all the incidents that occurred
    in the provided year and month.
    Each record includes the report_type_description,
    date, the number of incidents with the corresponding
    report_type_description one day before,
    the number of incidents with the corresponding
    report_type_description on the date,
    and the number of incidents with the corresponding
    report_type_description one day after.
    If the parameter n is not provided, the function returns all rows.
    """
    year_month = f"{kargs['year']}-{kargs['month']:02d}"
    query = f'''

    WITH counts AS (
    SELECT
        rt.report_type_description,
        CAST(DATE_TRUNC('day', i.incident_datetime) AS DATE) AS date,
        COUNT(*) AS incident_count
        FROM incident as i
        JOIN report_type as rt
        ON rt.report_type_code = i.report_type_code
        GROUP BY rt.report_type_description,
                DATE_TRUNC('day', i.incident_datetime)
    )
    SELECT
            co.report_type_description,
            co.date,
            LAG(co.incident_count, 1) OVER
              (PARTITION BY co.report_type_description ORDER BY co.date
              ) AS incidents_before,
            co.incident_count AS incidents_on_date,
            LEAD(co.incident_count, 1) OVER
            (PARTITION BY co.report_type_description ORDER BY co.date
            ) AS incidents_after
    FROM counts as  co
    WHERE (co.date::TEXT LIKE '{year_month}%' )
    ORDER BY co.report_type_description, co.date

                '''
    return check_query_args(query=query, **kargs)
