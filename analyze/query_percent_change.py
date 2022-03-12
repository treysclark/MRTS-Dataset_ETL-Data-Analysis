import sys
import yaml
import mysql.connector
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime

class QueryPercentChange:

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
            self.cnx = mysql.connector.connect(**config)
            self.cursor = self.cnx.cursor()
        except Exception as e:
            print("---- Error: creating mysql connection ----\n", e)
            sys.exit(1)

        self.cursor.execute("USE mrts;")

        query = """
                    SELECT 
                        sales_date,
                        MAX(CASE WHEN cat_code=%s THEN sales END) as mens_clothing,
                        MAX(CASE WHEN cat_code=%s THEN sales END) as womens_clothing,
                        MAX(CASE WHEN cat_code=%s THEN sales END) as all_clothing
                    FROM sales
                    WHERE sales_date < '2020-01-01'
                    GROUP BY 1;
                """

        # Filter by NAICS Codes
        params = (44811, 44812, 4481)
        self.cursor.execute(query, params)

        sales_date = []
        mens_clothing = []
        womens_clothing = []
        all_clothing = []

        # Get DB data
        for row in self.cursor.fetchall():
            d = row[0]
            # Convert to datetime, so it can be grouped by years later
            sales_date.append(datetime(d.year, d.month, d.day))
            # Get sales
            mens_clothing.append(row[1])
            womens_clothing.append(row[2])
            all_clothing.append(row[3])

        # Close connections     
        self.cursor.close()
        self.cnx.close()
        # Setup dictionary to hold DB values
        dict = {"sales_date": sales_date, "mens_clothing":mens_clothing, "womens_clothing":womens_clothing, "all_clothing":all_clothing}
        # Create DataFrame 
        self.df = pd.DataFrame(dict)
        # Check for missing values
        print(self.df.isna().sum())
        # Interpolate up to two consecutive missing values for mens clothing
        self.df["mens_clothing"].interpolate(limit=2, inplace=True)


    def get_monthly(self):
        # Get Monthly Sales Percent of Change
        # Set index, calculate percent of change
        df_monthly_PoC = self.df.set_index("sales_date").pct_change()
        # Setup monthly plot  
        fig, ax = plt.subplots()
        ax.plot(df_monthly_PoC.index, df_monthly_PoC["mens_clothing"], label="Men's")
        ax.plot(df_monthly_PoC.index, df_monthly_PoC["womens_clothing"], label="Women's")
        ax.legend(loc = 'upper left')
        plt.gca().set(title="Monthly Percent of Change in Clothing Store Sales", xlabel="Months", ylabel="Percent Change")


    # Get Annual Sales Percent of Change
    def get_annual_poc(self):
        # Group by year while settting index, calculate percent of change
        df_annual_PoC = self.df.groupby(pd.Grouper(key='sales_date', freq='Y')).sum().pct_change()
        # Setup annual plot
        fig, ax = plt.subplots()
        ax.plot(df_annual_PoC.index, df_annual_PoC["mens_clothing"], label="Men's")
        ax.plot(df_annual_PoC.index, df_annual_PoC["womens_clothing"], label="Women's")
        ax.legend(loc = 'lower left')
        plt.gca().set(title="Annual Percent of Change in Clothing Store Sales", xlabel="Years", ylabel="Percent Change")


    # Get Annual Sales Percent of Whole
    def get_annual_pow(self):
        # Create a separate copy of dataframe
        df_PoW = self.df.copy(deep=True)
        print(df_PoW)
        # Calculate percent of whole
        df_PoW["mens_clothing"]  = np.divide(self.df['mens_clothing'], self.df['all_clothing'])
        df_PoW["womens_clothing"]  = np.divide(self.df['womens_clothing'], self.df['all_clothing'])
        # Drop "all_clothing" column
        df_PoW.drop("all_clothing", axis=1, inplace=True)
        # Setup monthly plot  
        fig, ax = plt.subplots()
        ax.plot(df_PoW["sales_date"], df_PoW["mens_clothing"], label="Men's")
        ax.plot(df_PoW["sales_date"], df_PoW["womens_clothing"], label="Women's")
        ax.legend(loc = 'upper left')
        plt.gca().set(title="Monthly Clothing Sales as a Percent of Whole", xlabel="Months", ylabel="Percent of Whole")


    # Get Annual Sales Percent of Change
    def get_annual_poc(self):
        # Group by year while settting index
        df_annual_PoW = self.df.groupby(pd.Grouper(key='sales_date', freq='Y')).sum()
        # Calculate percent of whole
        df_annual_PoW["mens_clothing"] = np.divide(df_annual_PoW['mens_clothing'], df_annual_PoW['all_clothing'])
        df_annual_PoW["womens_clothing"] = np.divide(df_annual_PoW['womens_clothing'], df_annual_PoW['all_clothing'])
        # Drop "all_clothing" column
        df_annual_PoW.drop("all_clothing", axis=1, inplace=True)
        # Setup annual plot  
        fig, ax = plt.subplots()
        ax.plot(df_annual_PoW.index, df_annual_PoW["mens_clothing"], label="Men's")
        ax.plot(df_annual_PoW.index, df_annual_PoW["womens_clothing"], label="Women's")
        ax.legend(loc = 'upper left')
        plt.gca().set(title="Annual Clothing Sales as a Percent of Whole", xlabel="Years", ylabel="Percent of Whole")


    def show_reports(self):
        # Draw all plots
        plt.show()

