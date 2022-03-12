import sqlalchemy
import yaml
import sys
import mysql.connector
import atexit

import transform.clean as clean

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
    print("Error: mysql connection not created ->\n", e) 
    sys.exit(1) 

# Create SQLAlchemy connections 
try:
    # Create DB connection using sqlalchemy
    db_conn = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                               format(db['user'], db['pwrd'], 
                                                      db['host'], db['db']))
except Exception as e:
    print("---- Error: sqlalchemy connection not created -----\n", e)   



def create_db():
    print("Processing: creating mrts database")  
    try:
        cursor.execute("CREATE DATABASE IF NOT EXISTS mrts")
    except Exception as e:
        print("----- Error: mrts database not created -----\n", e) 
        return   
    print("Completed: created mrts database")  


def create_tables():
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
        return   
    print("Completed: created tables") 


def insert_combined_sales():
    # Get combined sales dataframes
    df_combined = clean.Clean().get_combined_sales()
    print("Processing: appending combined_sales table")  
    # Add to bottom of combined_sales table
    with db_conn.connect() as conn:
        try:
            df_combined.to_sql(con=conn, name='combined_sales', if_exists='append', index=False)    
        except Exception as e:
            print("----- Error: combined_sales not appended ------\n", e) 
            return   
        print("Completed: appended combined_sales table")                                             


def insert_store_sales(): 
    # Get cleaned dataframes
    df_store = clean.Clean().get_cleaned_store_sales()
    print("Processing: appending store_sales table") 
    # Add to bottom of store_sales table
    with db_conn.connect() as conn:
        try:
            df_store.to_sql(con=conn, name='store_sales', if_exists='append', index=False)
        except Exception as e:
            print("----- Error: store_sales not appended -----\n", e) 
            return   
        print("Completed: appended store_sales table")   


def drop_db():
    print("Processing: dropping mrts database") 
    try:
        cursor.execute("DROP DATABASE IF EXISTS mrts")
    except Exception as e:
        print("----- Error: mrts database not dropped ------\n", e) 
        return   
    print("Completed: dropped mrts database")   


def drop_tables():
    print("Processing: dropping tables")
    try:
        cursor.execute("USE mrts")
        cursor.execute("DROP TABLE IF EXISTS combined_sales")
        cursor.execute("DROP TABLE IF EXISTS store_sales")        
    except Exception as e:
        print("----- Error: tables not dropped -----\n", e) 
        return   
    print("Completed: dropped tables") 


def empty_tables():
    print("Processing: emptying tables")
    try:
        cursor.execute("USE mrts")
        cursor.execute("TRUNCATE TABLE combined_sales")
        cursor.execute("TRUNCATE TABLE store_sales")        
    except Exception as e:
        print(" ----- Error: tables not emptied -----\n", e) 
        return   
    print("Completed: emptied tables") 


@atexit.register
def close_conn():
    # Close connections
    cursor.close()
    cnx.close()
    # Make sure connection is closed 
    # Source: https://stackoverflow.com/a/51242577/848353
    db_conn.dispose()
