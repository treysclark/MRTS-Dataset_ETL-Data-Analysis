import sys
import yaml
import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

class QueryTrends:

    def __init__(self):
        with open("./load/db.yaml", "r") as stream:
            try:
                db = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print("---- Error: loading yaml ----\n", exc)
                sys.exit(1)

        config = {
            'user':         db['user'],
            'password':     db['pwrd'],
            'host':         db['host'],
            'database':     db['db'],
            'auth_plugin': 'mysql_native_password'
        }

        try:
            cnx = mysql.connector.connect(**config)
            cursor = cnx.cursor()
        except Exception as e:
            print("---- Error: creating mysql connection ----\n", e)
            sys.exit(1)

        cursor.execute("USE mrts;")

        query = """
                    SELECT 
                        sales_date,
                        MAX(CASE WHEN cat_code=%s THEN sales END) as sport_sales,
                        MAX(CASE WHEN cat_code=%s THEN sales END) as hobby_sales,
                        MAX(CASE WHEN cat_code=%s THEN sales END) as book_sales
                    FROM sales
                    GROUP BY 1;
                """

        # Filter by NAICS Codes
        params = (45111, 45112, 451211)
        cursor.execute(query, params)

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

        # Close connections     
        cursor.close()
        cnx.close()

        # Setup dictionary to hold DB values
        dict = {"sales_date": sales_date, "sport_sales":sport_sales, "hobby_sales":hobby_sales, "book_sales":book_sales}

        # Create DataFrame and interpolate missing values
        df = pd.DataFrame(dict)

        # Show missing values
        print(df.isna().sum())


    # Monthly Sales
    def get_monthly_trends(self):
        # Draw monthly plot  
        fig, ax = plt.subplots()
        ax.plot(self.df["sales_date"], self.df["sport_sales"], label="Sports")
        ax.plot(self.df["sales_date"], self.df["hobby_sales"], label="Hobby")
        ax.plot(self.df["sales_date"], self.df["book_sales"], label="Books")
        ax.legend(loc = 'upper left')
        plt.gca().set(title="Monthly Sales", xlabel="Months", ylabel="Sales")


    # Annual Sales
    def get_annual_trends(self):
        # Group by year
        df_annual = self.df.groupby(pd.Grouper(key='sales_date', freq='Y')).sum()
        # Setup annual plot
        fig, ax = plt.subplots()
        ax.plot(df_annual.index, df_annual["sport_sales"], label="Sports")
        ax.plot(df_annual.index, df_annual["hobby_sales"], label="Hobby")
        ax.plot(df_annual.index, df_annual["book_sales"], label="Books")
        ax.legend(loc = 'upper left')
        plt.gca().set(title="Annual Sales", xlabel="Years", ylabel="Sales")


    def show_reports(self):
        # Draw all plots
        plt.show()