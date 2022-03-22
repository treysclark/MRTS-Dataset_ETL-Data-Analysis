from time import perf_counter
import sqlalchemy
import yaml
import sys
import mysql.connector
import atexit
import pandas as pd
from time import perf_counter
from datetime import datetime
import transform.clean as clean
import load.validation as validation

# Get database parameters
with open("./load/db.yaml", "r") as stream:
    try:
        db = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)

config = {
    'user':         db['user'],
    'password':     db['pwrd'],
    'host':         db['host'],
    'port':         db['port'],
    'auth_plugin': 'mysql_native_password'
}

# Create MYSQL connections
try : 
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
except Exception as e:
    print("---- Error: mysql connection not created ->\n", e) 
    sys.exit(1) 

# Create SQLAlchemy connections 
try:
    # Create DB connection using sqlalchemy
    db_conn = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                               format(db['user'], db['pwrd'], 
                                                      db['host'], db['db']))
except Exception as e:
    print("---- Error: sqlalchemy connection not created -----\n", e)  
    sys.exit(1)  


def create_db():
    start_time = perf_counter()
    print("Processing: creating mrts database")  
    try:
        cursor.execute("CREATE DATABASE IF NOT EXISTS mrts")
    except Exception as e:
        print("----- Error: mrts database not created -----\n", e) 
        return sys.exit(1)   
    print("Completed: created mrts database in ", round(perf_counter()-start_time,4), " seconds.")  


def create_tables():
    start_time = perf_counter()
    print("Processing: creating tables") 
    # SQL STMT: Create NAICS Code table
    create_combined_sales = """CREATE TABLE IF NOT EXISTS combined_sales(
        id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        sales_date DATE NOT NULL,
        sales INT,
        cat_name VARCHAR(500) NOT NULL,
        CONSTRAINT uc_combined_sales UNIQUE (sales_date, sales, cat_name))
        ENGINE=InnoDB;"""
    # SQL STMT: Create store sales table
    create_store_sales = """CREATE TABLE IF NOT EXISTS store_sales (
        id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        sales_date DATE NOT NULL,
        sales INT, 
        cat_name VARCHAR(500) NOT NULL, 
        cat_code VARCHAR(50) NOT NULL,
        CONSTRAINT uc_sales UNIQUE (sales_date, sales, cat_name, cat_code))
        ENGINE=InnoDB;"""
    try:
        cursor.execute("USE mrts")
        # Create combined sales table   
        cursor.execute(create_combined_sales)
        # Create store sale table   
        cursor.execute(create_store_sales)
    except Exception as e:
        print("----- Error: tables not created -----\n", e) 
        return sys.exit(1)  
    print("Completed: created tables in ", round(perf_counter()-start_time,4), " seconds.") 


def insert_all_sales():
    create_db()
    create_tables()
    # Increase performance by retrieving data for all tables at once
    df_all_sales = clean.Clean().get_all_sales()
    insert_combined_sales(df_all_sales['df_combined'])
    insert_store_sales(df_all_sales['df_store'])
    # Verify the correct number of records and values were 
    # inserted into the db compared to the source data.
    validation.validate_all(df_all_sales)
    

def insert_combined_sales(df_combined):
    start_time = perf_counter()
    print("Processing: appending combined_sales table")  
    # Add to bottom of combined_sales table
    with db_conn.connect() as conn:
        try:
            # Using chunksize and multi-insert to increase insertion speed. 
            df_combined.to_sql(con=conn, name='combined_sales', if_exists='append', index=False, chunksize=1000, method='multi')    
        except Exception as e:
            print("----- Error: combined_sales not appended ------\n", e) 
            return sys.exit(1)   
    print(f"Completed: appended combined_sales table in ", round(perf_counter()-start_time,4), " seconds.")                                             


def insert_store_sales(df_store): 
    start_time = perf_counter()
    print("Processing: appending store_sales table") 
    # Add to bottom of store_sales table
    with db_conn.connect() as conn:
        try:
            # Using chunksize and multi-insert to increase insertion speed. 
            df_store.to_sql(con=conn, name='store_sales', if_exists='append', index=False, chunksize=1000, method='multi')      
        except Exception as e:
            print("----- Error: store_sales not appended -----\n", e) 
            return sys.exit(1)   
    print(f"Completed: appended store_sales table in ", round(perf_counter()-start_time,4), " seconds.") 


def read_combined_sales_count(): 
    start_time = perf_counter()
    print("Processing: counting records in combined_sales table") 
     # SQL STMT: Count records in store_sales table
    count_combined_sales = """SELECT COUNT(*) FROM combined_sales;"""
    try:
        cursor.execute("USE mrts") 
        cursor.execute(count_combined_sales)
    except Exception as e:
        print("----- Error: counting records in combined_sales table -----\n", e) 
        return sys.exit(1)     
    print("Completed: counting records in combined_sales table in ", round(perf_counter()-start_time,4), " seconds.") 
    # Return number of records in store_sales table
    return cursor.fetchone()[0]


def read_store_sales_count():
    start_time = perf_counter() 
    print("Processing: counting records in store_sales table") 
     # SQL STMT: Count records in store_sales table
    count_store_sales = """SELECT COUNT(*) FROM store_sales;"""
    try:
        cursor.execute("USE mrts") 
        cursor.execute(count_store_sales)
    except Exception as e:
        print("----- Error: counting records in store_sales table -----\n", e) 
        return sys.exit(1) 
    print("Completed: counting records in store_sales table in ", round(perf_counter()-start_time,4), " seconds.") 
    # Return number of records in store_sales table
    return cursor.fetchone()[0]


def read_calc_annual_sales(): 
    start_time = perf_counter()
    print("Processing: calculating annual sales from all tables") 
    calc_annual_sales = """
                            SELECT YEAR(sales_date), cat_name, SUM(sales)
                            FROM combined_sales
                            GROUP BY YEAR(sales_date), cat_name
                            UNION
                            SELECT YEAR(sales_date), cat_name, SUM(sales)
                            FROM store_sales
                            GROUP BY YEAR(sales_date), cat_name;
                        """
    try:
        cursor.execute("USE mrts") 
        cursor.execute(calc_annual_sales)
    except Exception as e:
        print("----- Error: counting records in store_sales table -----\n", e) 
        return sys.exit(1)  
    result = cursor.fetchall()
    print(f"Completed: calculated annual sales ({'{:,}'.format(len(result))} records) from all tables in ", 
            round(perf_counter()-start_time,4), " seconds.") 
    return result


def read_combined_sales():
    start_time = perf_counter()
    print("Processing: retrieving data from combined_sales table")
    # Do not combine by year. Doing so may hide months with empty sales that should be interpolated
    query = """
                SELECT sales_date, CAST(sales AS UNSIGNED)
                FROM combined_sales
                WHERE cat_name = %s;
            """
            
    # Filter by Kind of Business
    params = ("Retail and food services sales, total",)
    try: 
        cursor.execute("USE mrts;")
        cursor.execute(query, params)
    except Exception as e:
        print("---- Error: records not retrieved from combined_sales table\n", e)
        sys.exit(1)

    sales_date = []
    sales = []
    # Get DB data
    for row in cursor.fetchall():
        row_date = row[0]
        # Convert to datetime, so it can be grouped by years later
        sales_date.append(datetime(row_date.year, row_date.month, row_date.day))
        sales.append(row[1])
    # Setup dictionary to hold DB values
    dict = {"sales_date": sales_date, "sales":sales}
    print("Completed: retrieved data from combined_sales table in ", round(perf_counter()-start_time,4), " seconds.") 
    return pd.DataFrame(dict)


def read_percent_change():
    start_time = perf_counter()
    print("Processing: retrieving data from store_sales table")
    query = """
                SELECT 
                    sales_date,
                    MAX(CASE WHEN cat_code=%s THEN sales END) as mens_clothing,
                    MAX(CASE WHEN cat_code=%s THEN sales END) as womens_clothing,
                    MAX(CASE WHEN cat_code=%s THEN sales END) as all_clothing
                FROM store_sales
                WHERE sales_date < '2020-01-01'
                GROUP BY 1;
            """
    # Filter by NAICS Codes
    params = (44811, 44812, 4481)
    try: 
        cursor.execute("USE mrts;")
        cursor.execute(query, params)
    except Exception as e:
        print("---- Error: records not retrieved from store_sales table\n", e)
        sys.exit(1)
    sales_date = []
    mens_clothing = []
    womens_clothing = []
    all_clothing = []
    # Get DB data
    for row in cursor.fetchall():
        d = row[0]
        # Convert to datetime, so it can be grouped by years later
        sales_date.append(datetime(d.year, d.month, d.day))
        # Get sales
        mens_clothing.append(row[1])
        womens_clothing.append(row[2])
        all_clothing.append(row[3])
    print("Completed: retrieved data from store_sales table in ", round(perf_counter()-start_time,4), " seconds.") 
    # Setup dictionary to hold DB values
    dict = {"sales_date": sales_date, "mens_clothing":mens_clothing, "womens_clothing":womens_clothing, "all_clothing":all_clothing}
    return pd.DataFrame(dict)


def read_rolling_time():
    start_time = perf_counter()
    print("Processing: retrieving data from store_sales table")
    query = """
                SELECT 
                    sales_date,
                    MAX(CASE WHEN cat_code=%s THEN sales END) as new_cars,
                    MAX(CASE WHEN cat_code=%s THEN sales END) as used_cars
                FROM store_sales
                GROUP BY 1;
            """
    # Filter by NAICS Codes
    params = (44111,44112)
    try: 
        cursor.execute("USE mrts;")
        cursor.execute(query, params)
    except Exception as e:
        print("---- Error: records not retrieved from store_sales table\n", e)
        sys.exit(1)
    sales_date = []
    new_cars = []
    used_cars = []
    # Get DB data
    for row in cursor.fetchall():
        d = row[0]
        # Convert to datetime, so it can be grouped by years later
        sales_date.append(datetime(d.year, d.month, d.day))
        # Get sales
        new_cars.append(row[1])
        used_cars.append(row[2])
    print("Completed: retrieved data from store_sales table in ", round(perf_counter()-start_time,4), " seconds.") 
    # Setup dictionary to hold DB values
    dict = {"sales_date": sales_date, "new_cars":new_cars, "used_cars":used_cars}
    return pd.DataFrame(dict)


def read_trends():
    start_time = perf_counter()
    print("Processing: retrieving data from store_sales table")
    query = """
                SELECT 
                    sales_date,
                    MAX(CASE WHEN cat_code=%s THEN sales END) as sport_sales,
                    MAX(CASE WHEN cat_code=%s THEN sales END) as hobby_sales,
                    MAX(CASE WHEN cat_code=%s THEN sales END) as book_sales
                FROM store_sales
                GROUP BY 1;
            """
    # Filter by NAICS Codes
    params = (45111, 45112, 451211)
    try: 
        cursor.execute("USE mrts;")
        cursor.execute(query, params)
    except Exception as e:
        print("---- Error: records not retrieved from store_sales table\n", e)
        sys.exit(1)
    sales_date = []
    sport_sales = []
    hobby_sales = []
    book_sales = []
    # Get DB data
    for row in cursor.fetchall():
        d = row[0]
        # Convert to datetime, so it can be grouped by years later
        sales_date.append(datetime(d.year, d.month, d.day))
        # Get sales
        sport_sales.append(row[1])
        hobby_sales.append(row[2])
        book_sales.append(row[3])
    print("Completed: retrieved data from store_sales table in ", round(perf_counter()-start_time,4), " seconds.") 
    # Setup dictionary to hold DB values
    dict = {"sales_date": sales_date, "sport_sales":sport_sales, "hobby_sales":hobby_sales, "book_sales":book_sales}
    return pd.DataFrame(dict)


def empty_tables():
    start_time = perf_counter()
    print("Processing: emptying tables")
    try:
        cursor.execute("USE mrts")
        cursor.execute("TRUNCATE TABLE combined_sales")
        cursor.execute("TRUNCATE TABLE store_sales")        
    except Exception as e:
        print(" ----- Error: tables not emptied -----\n", e) 
        return sys.exit(1)   
    print("Completed: emptied tables in ", round(perf_counter()-start_time,4), " seconds.") 


def drop_db():
    start_time = perf_counter()
    print("Processing: dropping mrts database") 
    try:
        cursor.execute("DROP DATABASE IF EXISTS mrts")
    except Exception as e:
        print("----- Error: mrts database not dropped ------\n", e) 
        return sys.exit(1)   
    print("Completed: dropped mrts database in in ", round(perf_counter()-start_time,4), " seconds.") 


def drop_tables():
    start_time = perf_counter()
    print("Processing: dropping tables")
    try:
        cursor.execute("USE mrts")
        cursor.execute("DROP TABLE IF EXISTS combined_sales")
        cursor.execute("DROP TABLE IF EXISTS store_sales")        
    except Exception as e:
        print("----- Error: tables not dropped -----\n", e) 
        return sys.exit(1)   
    print("Completed: dropped tables in ", round(perf_counter()-start_time,4), " seconds.") 


@atexit.register
def close_conn():
    # Close connections
    cursor.close()
    cnx.close()
    # Make sure connection is closed 
    # Source: https://stackoverflow.com/a/51242577/848353
    db_conn.dispose()
