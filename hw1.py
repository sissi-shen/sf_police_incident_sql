import psycopg

def drop_tables(user, host, dbname):
    """
    This function connects to a database
    using user, host and dbname, and
    drops all the tables including
    report_type, incident_type, location and incident.
    This function should work regardless of
    the existence of the table without any errors.
    """
    conn = psycopg.connect(f"dbname={dbname} user={user} host={host}")
    cur = conn.cursor()
    query = f"""
            DROP TABLE IF EXISTS report_type CASCADE;
            DROP TABLE IF EXISTS incident_type CASCADE;
            DROP TABLE IF EXISTS location CASCADE;
            DROP TABLE IF EXISTS incident CASCADE;
            """
    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()


def create_tables(user, host, dbname):
    """
    For the given user, host, and dbname,
    this function creates 4 different tables including
    report_type, incident_type, location and incident.
    The schema of each table is the following.
    report_type:
    report_type_code - varying character (length of 2), not null
    report_type_description - varying character (length of 100), not null
    primary key - report_type_code

    incident_type:
    incident_code - integer, not null
    incident_category - varying character (length of 100), null
    incident_subcategory - varying character (length of 100), null
    incident_description - varying character (length of 200), null
    primary key - incident_code

    location:
    longitude - real, not null
    latitude - real, not null
    supervisor_district - real, null
    police_district - varying character (length of 100), not null
    neighborhood - varying character (length of 100), null
    prinmary key - longitude, latitude

    incident:
    id - integer, not null
    incident_datetime - timestamp, not null
    report_datetime -  timestamp, not null
    longitude - real, null
    latitude - real, null
    report_type_code - varying character (length of 2), not null
    incident_code - integer, not null
    primary key - id
    foreign key - report_type_code, incident_code and
                  (longitude, latitude) pair.
    """
    conn = psycopg.connect(f"dbname={dbname} user={user} host={host}")
    cur = conn.cursor()
    query = f"""
    CREATE TABLE report_type
    (
    report_type_code VARCHAR(2) NOT NULL,
    report_type_description VARCHAR(100) NOT NULL,
    PRIMARY KEY (report_type_code)
    );
    CREATE TABLE incident_type
    (
    incident_code INTEGER NOT NULL,
    incident_category VARCHAR(100) NULL,
    incident_subcategory VARCHAR(100) NULL,
    incident_description VARCHAR(100) NULL,
    PRIMARY KEY (incident_code)
    );
    CREATE TABLE location
    (
    longitude REAL NOT NULL,
    latitude REAL NOT NULL,
    supervisor_district REAL NULL,
    police_district VARCHAR(100) NOT NULL,
    neighborhood VARCHAR(100) NULL,
    PRIMARY KEY (longitude, latitude)
    );
    CREATE TABLE incident
    (
    id INTEGER NOT NULL,
    incident_datetime TIMESTAMP NOT NULL,
    report_datetime TIMESTAMP NOT NULL,
    longitude REAL NULL,
    latitude REAL NULL,
    report_type_code VARCHAR(2) NOT NULL,
    incident_code INTEGER NOT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY (report_type_code) REFERENCES report_type (report_type_code)
    ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (incident_code) REFERENCES incident_type (incident_code)
    ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (longitude, latitude) REFERENCES location (longitude, latitude)
    ON UPDATE CASCADE ON DELETE CASCADE
    );
    """
    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()


def copy_data(user, host, dbname, dir):
    """
    Using user, host, dbname, and dir,
    this function connects to the database and
    loads data to report_type, incident_type, location and incident
    from report_type.csv, incident_type.csv, location.csv and incident.csv
    located in dir.
    Note: each file includes a header.
    """
    conn = psycopg.connect(f"dbname={dbname} user={user} host={host}")
    cur = conn.cursor()
    query = f"""
    COPY report_type
    FROM '{dir}/report_type.csv'
    DELIMITER ','
    CSV HEADER;
    COPY incident_type
    FROM '{dir}/incident_type.csv'
    DELIMITER ','
    CSV HEADER;
    COPY location
    FROM '{dir}/location.csv'
    DELIMITER ','
    CSV HEADER;
    COPY incident
    FROM '{dir}/incident.csv'
    DELIMITER ','
    CSV HEADER;
    """
    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()


def return_distinct_neighborhood_police_district(user, host, dbname, n=None):
    """
    Using user, host, dbname, dir, and n,
    this function connects to the database and
    returns n unique rows of neighborhood and police_district
    in the location table,
    where neighborhood is not null.
    The returned output is ordered by neighborhood and police_district
    in ascending order.
    If n is not given, it returns all the rows.
    """
    conn = psycopg.connect(f"dbname={dbname} user={user} host={host}")
    cur = conn.cursor()
    if n is not None:
        query = f"""
        SELECT DISTINCT neighborhood, police_district
        FROM location
        WHERE neighborhood IS NOT NULL
        ORDER BY neighborhood ASC, police_district ASC
        LIMIT %s;
        """
        cur.execute(query, (n,))
    else:

        query = f"""
        SELECT DISTINCT neighborhood, police_district
        FROM location
        WHERE neighborhood IS NOT NULL
        ORDER BY neighborhood ASC, police_district ASC;
        """
        cur.execute(query)

    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def return_distinct_time_taken(user, host, dbname, n=None):
    """
    Using user, host, dbname, dir, and n,
    this function connects to the database and
    returns n unique differences
    between report_datetime and incident_datetime in days
    in descending order.
    If n is not given, it returns all the rows.
    """
    conn = psycopg.connect(f"dbname={dbname} user={user} host={host}")
    cur = conn.cursor()
    if n is not None:
        query = f"""
        SELECT DISTINCT EXTRACT(DAY FROM (report_datetime - incident_datetime))
        AS diff
        FROM incident
        ORDER BY diff DESC
        LIMIT %s;
        """
        cur.execute(query, (n,))
    else:
        query = f"""
        SELECT DISTINCT EXTRACT(DAY FROM (report_datetime - incident_datetime))
        AS diff
        FROM incident
        ORDER BY diff DESC;
        """
        cur.execute(query)

    output = cur.fetchall()
    cur.close()
    conn.close()
    return output


def return_incident_with_incident_substring(user,
                                            host,
                                            dbname,
                                            substr,
                                            n=None):
    """
    Using user, host, dbname, dir, substr, and n,
    this function connects to the database and
    returns n unique id and incident_datetime in the incident table
    where its incident_code corresonds to the incident_description
    that includes substr, a substring ordered by id in ascending order.
    The search for the existence of the given substring
    should be case-insensitive.
    If n is not given, it returns all the rows.
    """
    conn = psycopg.connect(f"dbname={dbname} user={user} host={host}")
    cur = conn.cursor()
    if n is not None:
        query = f"""
        SELECT DISTINCT i.id, i.incident_datetime
        FROM incident i
        JOIN incident_type it ON i.incident_code = it.incident_code
        WHERE LOWER(it.incident_description) ILIKE '%{substr}%'
        ORDER BY i.id ASC
        LIMIT {n}
        """
        cur.execute(query)
    else:
        query = f"""
        SELECT DISTINCT i.id, i.incident_datetime
        FROM incident i
        JOIN incident_type it ON i.incident_code = it.incident_code
        WHERE LOWER(it.incident_description) ILIKE '%{substr}%'
        ORDER BY id ASC
        """
        cur.execute(query)

    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def return_incident_desc_for_report_type_desc(user,
                                              host,
                                              dbname,
                                              desc,
                                              n=None):
    """
    Using user, host, dbname, dir, substr, and n,
    this function connects to the database and
    returns n unique incident description in the incident_type table
    where its incident_code corresponds to desc as
    report_type_description ordered by incident_description in ascending order.
    The search of the report_type_description
    should be case-insensitive.
    If n is not given, it returns all the rows.
    """
    conn = psycopg.connect(f"dbname={dbname} user={user} host={host}")
    cursor = conn.cursor()
    if n is not None:
        query = f"""
            SELECT DISTINCT it.incident_description
            FROM incident_type it
            JOIN incident i ON it.incident_code = i.incident_code
            JOIN report_type rt ON rt.report_type_code = i.report_type_code
            WHERE LOWER(rt.report_type_description) ILIKE '%{desc}%'
            ORDER BY it.incident_description ASC
            LIMIT {n}
            """
    else:
        query = f"""
            SELECT DISTINCT it.incident_description
            FROM incident_type it
            JOIN incident i ON it.incident_code = i.incident_code
            JOIN report_type rt ON rt.report_type_code = i.report_type_code
            WHERE LOWER(rt.report_type_description) ILIKE '%{desc}%'
            ORDER BY it.incident_description ASC
            """
    cursor.execute(query)
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return results


def update_report_type(user, host, dbname, from_str, to_str):
    """
    Using user, host, dbname, dir, and n,
    this function connects to the database and
    updates report_type_code from from_str to to_str
    on the report_type table.
    """
    connect = psycopg.connect(f"dbname={dbname} user={user} host={host}")
    cur = connect.cursor()
    query = """
            UPDATE report_type
            
            SET report_type_code = %s
            WHERE  report_type_code = %s
            """
    #SELECT COUNT(*)
     # FROM incident update table, set blank = 1. whwer blank =2
    cur.execute(query, (to_str, from_str))
    connect.commit()
    cur.close()
    connect.close()  
  