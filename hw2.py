import psycopg


def select_all(func):
    """
    Q1. Complete the select_all() decorator, which 1) retrieve
    keyword arguments including user, host and dbname,
    2) executes a SQL query string returned
    from a function and 3) returns the output.
    Ex.
    When
    @select_all
    def return_incident_category_count(**kargs):
        # complete
    , calling
    return_incident_category_count(user='postgres',
                                   host='127.0.0.1',
                                   dbname='msds691_HW',
                                   n=5)
    returns [('Other Miscellaneous', 101), ('Larceny Theft', 90),
    ('Robbery', 72), ('Drug Offense', 60), ('Burglary', 52)]
    """
    def wrapper(**kargs):
        user = kargs['user']
        host = kargs['host']
        dbname = kargs['dbname']

        conn = psycopg.connect(user=user, host=host, dbname=dbname)
        cur = conn.cursor()

        query = func(**kargs)
        cur.execute(query)
        results = cur.fetchall()

        cur.close()
        conn.close()

        return results

    return wrapper


def check_query_args(**kargs):
    query = kargs['query']
    if 'explain' in kargs and kargs['explain'] is True:
        query = 'EXPLAIN ANALYZE VERBOSE ' + query
    if 'n' in kargs:
        query = query + f" LIMIT {kargs['n']}"
    return query


@select_all
def return_incident_category_count(**kargs):
    """
    Q2. Complete the return_incident_category_count() function.
    This function connects to the database using the parameters
    user, host, dbname, and n, and retrieves n records of
    incident_category along with their corresponding count
    from the incident_type table.
    The function only retrieves records
    where incident_category is not null and orders them
    by count in descending order.
    If there are rows with the same count,
    the function sorts them alphabetically by incident_category
    in ascending order.
    If the parameter n is not provided, the function returns all rows.
    """
    query = """
            SELECT incident_category, COUNT(*) AS count
            FROM incident_type
            WHERE incident_category IS NOT NULL
            GROUP BY incident_category
            ORDER BY count DESC, incident_category ASC
            """

    return check_query_args(query=query, **kargs)


@select_all
def return_incident_count_by_category_subcategory(**kargs):
    """
    Q3. Complete the return_incident_count_by_category_subcategory() function.
    This function connects to the database
    using the provided user, host, dbname,
    count_limit, and n parameters.
    It returns n records of incident_category, incident_subcategory,
    and their count (occurrence) in the incident table
    where the count is greater than count_limit.
    The output is ordered by occurrence in descending order.
    If there are records with the same count value, they are ordered
    by incident_category alphabetically (ascending).
    """
    count_limit = kargs['count_limit']
    query = f"""
            SELECT it.incident_category,
                   it.incident_subcategory,
                   COUNT(*) AS occurrence
            FROM incident AS i
            JOIN incident_type AS it ON i.incident_code = it.incident_code
            GROUP BY it.incident_category, it.incident_subcategory
            HAVING COUNT(*) > {count_limit}
            ORDER BY occurrence DESC, it.incident_category ASC
            """
    return check_query_args(query=query, **kargs)


@select_all
def return_count_by_location_report_type_incident_description(**kargs):
    """
    Q4. Complete
    the return_count_by_location_report_type_incident_description() function.
    This function connects to the database
    using the given user, host, dbname, year,
    and n parameters, and returns an output of n rows (if n is given) or
    all rows of the following columns: year (extracted from incident_datetime),
    month (also extracted from incident_datetime), longitude, latitude,
    neighborhood, report_type_description, incident_description,
    and the corresponding count, which is ordered by count in descending order,
    and then by year, month, longitude, latitude, report_type_description,
    and incident_description in ascending order.
    """
    year = int(kargs["year"])
    query = f"""
    SELECT EXTRACT(YEAR FROM incident_datetime)::INTEGER AS year,
           EXTRACT(MONTH FROM incident_datetime)::INTEGER AS month,
           location.longitude,
           location.latitude,
           location.neighborhood,
           report_type.report_type_description,
           incident_type.incident_description,
           COUNT(*) AS count
    FROM incident
    JOIN location ON incident.longitude = location.longitude
                 AND incident.latitude = location.latitude
    JOIN report_type ON incident.report_type_code =
    report_type.report_type_code
    JOIN incident_type ON incident.incident_code = incident_type.incident_code
    WHERE EXTRACT(YEAR FROM incident_datetime)::INTEGER = {year}
    GROUP BY year, month, location.longitude, location.latitude,
             location.neighborhood, report_type.report_type_description,
             incident_type.incident_description
    ORDER BY count DESC, year, month, location.longitude, location.latitude,
             report_type.report_type_description,
             incident_type.incident_description
    """
    return check_query_args(query=query, **kargs)


@select_all
def return_avg_interval_days_per_incident_code(**kargs):
    """
    Q5.
    Complete the return_avg_interval_days_per_incident_code() function.
    This function calculates the average number of days taken between
    incident_datetime and report_datetime for each incident_code.
    Using user, host, dbname, and n, this function connects to the database
    and returns n rows of incident_code, incident_description,
    and avg_interval_days,
    where avg_interval_days is the average difference between report_datetime
    and incident_datetime extracted as days.
    The output should be ordered by avg_interval_days in descending order.
    If there are multiple rows with the same avg_interval_days,
    order by incident_code in ascending order.
    If n is not given, it returns all the rows.
    """
    query = """
    SELECT i.incident_code, it.incident_description,
           CAST(FLOOR(
           AVG(EXTRACT(DAY FROM (i.report_datetime - i.incident_datetime)))
           ) AS INTEGER)
           AS avg_interval_days
    FROM incident AS i
    JOIN incident_type as it ON i.incident_code = it.incident_code
    GROUP BY i.incident_code, it.incident_description
    ORDER BY avg_interval_days DESC, i.incident_code ASC
            """
    return check_query_args(query=query, **kargs)


@select_all
def return_monthly_count(**kargs):
    """
    Q6.
    Complete the `return_monthly_count()` function.
    This function returns the number of incidents in each month of each year.
    Using `user`, `host`, `dbname`, and `n`, this function connects to
    the database and returns `n` rows of `year`, `jan`, `feb`, `mar`, `apr`,
    `may`, `jun`, `jul`, `aug`, `sep`, `oct`, `nov` and `dec`,
    where each column includes the number of incidents
    for the corresponding year and month, ordered by year in ascending order.
    If `n` is not given, it returns all the rows.
    """

    query = """
            SELECT EXTRACT(YEAR FROM incident_datetime)::INTEGER AS year,
                NULLIF(COUNT(CASE WHEN EXTRACT(MONTH FROM incident_datetime)
                ::INTEGER=1 THEN 1 ELSE NULL END),0),
                NULLIF(COUNT(CASE WHEN EXTRACT(MONTH FROM incident_datetime)
                ::INTEGER=2 THEN 2 ELSE NULL END),0),
                NULLIF(COUNT(CASE WHEN EXTRACT(MONTH FROM incident_datetime)
                ::INTEGER=3 THEN 3 ELSE NULL END),0),
                NULLIF(COUNT(CASE WHEN EXTRACT(MONTH FROM incident_datetime)
                ::INTEGER=4 THEN 4 ELSE NULL END),0),
                NULLIF(COUNT(CASE WHEN EXTRACT(MONTH FROM incident_datetime)
                ::INTEGER=5 THEN 5 ELSE NULL END),0),
                NULLIF(COUNT(CASE WHEN EXTRACT(MONTH FROM incident_datetime)
                ::INTEGER=6 THEN 6 ELSE NULL END),0),
                NULLIF(COUNT(CASE WHEN EXTRACT(MONTH FROM incident_datetime)
                ::INTEGER=7 THEN 7 ELSE NULL END),0),
                NULLIF(COUNT(CASE WHEN EXTRACT(MONTH FROM incident_datetime)
                ::INTEGER=8 THEN 8 ELSE NULL END),0),
                NULLIF(COUNT(CASE WHEN EXTRACT(MONTH FROM incident_datetime)
                ::INTEGER=9 THEN 9 ELSE NULL END),0),
                NULLIF(COUNT(CASE WHEN EXTRACT(MONTH FROM incident_datetime)
                ::INTEGER=10 THEN 10 ELSE NULL END),0),
                NULLIF(COUNT(CASE WHEN EXTRACT(MONTH FROM incident_datetime)
                ::INTEGER=11 THEN 11 ELSE NULL END),0),
                NULLIF(COUNT(CASE WHEN EXTRACT(MONTH FROM incident_datetime)
                ::INTEGER=12 THEN 12 ELSE NULL END),0)
            FROM incident
            GROUP BY EXTRACT(YEAR FROM incident_datetime)::INTEGER
            ORDER BY year
            """
    return check_query_args(query=query, **kargs)


def create_index(**kargs):
    """
    Q7. Assuming that the query
    return_count_by_location_report_type_incident_description() (Q4)
    is the most frequently used query in your database,
    complete create_index() which creates indexes to improve its performance
    by at least 10%.
    For this question, you can assume that there will be no insertions or
    updates made to the database afterwards.
    Using streamlit, the create_index  will display the query improvement
    after you enter the absolute path of the data directory.
    """
    user = kargs['user']
    host = kargs['host']
    dbname = kargs['dbname']
    conn = psycopg.connect(user=user, host=host, dbname=dbname)
    cur = conn.cursor()
    query = """
            CREATE INDEX idx_incident_datetime ON incident
            USING btree(incident_datetime);
            CREATE INDEX idx_location_longitude ON location
            USING hash(longitude);
            CREATE INDEX idx_location_latitude ON location
            USING hash(latitude);
            CREATE INDEX idx_neighbor ON location
            USING hash(neighborhood);
            CLUSTER incident USING idx_incident_datetime;
        """
    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()
