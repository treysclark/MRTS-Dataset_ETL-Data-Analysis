import sys
import yaml
import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
from statsmodels.tsa.seasonal import seasonal_decompose

class QueryCombinedSales:

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

        self.df = {}


    def setup_df(self):
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
            self.cursor.execute("USE mrts;")
            self.cursor.execute(query, params)
        except Exception as e:
            print("Error: records not retrieved from combined_sales table\n", e)
            sys.exit(1)

        sales_date = []
        sales = []

        # Get DB data
        for row in self.cursor.fetchall():
            row_date = row[0]
            # Convert to datetime, so it can be grouped by years later
            sales_date.append(datetime(row_date.year, row_date.month, row_date.day))
            sales.append(row[1])

        print("Completed: retrieved data from combined_sales table")

        # Close connections     
        self.cursor.close()
        self.cnx.close()

        # Setup dictionary to hold DB values
        dict = {"sales_date": sales_date, "sales":sales}

        # Create DataFrame and interpolate any possible missing values
        self.df = pd.DataFrame(dict)
        # Check for missing values
        sum_nans = self.df.isna().sum()        
        if sum_nans.sum() == 0:
            nan_msg = "Verification: No missing values (nans)"
        else:
            nan_msg = f"Verification: missing values (nans)\n\t{sum_nans}"
            nan_msg += "\n Nans will be interpolated."
            # Interpolate for missing values
            self.df['sales'].interpolate(inplace=True)
        print(nan_msg)



    # Get Monthly Sales 
    def get_monthly(self):
        print("Processing: Monthly Sales")
        # Setup monthly plot  
        fig, ax = plt.subplots()
        ax.plot(self.df["sales_date"], self.df["sales"])
        plt.gca().set(title="Monthly Retail and Food Services Sales", xlabel="Months", ylabel="Sales")
        print("Completed: Monthly Sales")

    # Get Annual Sales
    def get_annual(self):
        print("Processing: Annual Sales")
        # Group by year
        df_annual = self.df.groupby(pd.Grouper(key='sales_date', freq='Y')).sum() 
        fig, ax = plt.subplots()
        ax.plot(df_annual.index, df_annual["sales"])
        plt.gca().set(title="Annual Retail and Food Services Sales", xlabel="Years", ylabel="Sales")
        print("Completed: Annual Sales")

    # Monthly Sales without Seasonality
    def get_monthly_decompose(self):
        print("Processing: Monthly Sales without Seasonality")
        self.df.set_index("sales_date", inplace=True)
        # TEST: Extract trend and seasonality and see whether multiplicative or additive is better
        # Multiplicative Decomposition 
        result_mul = seasonal_decompose(self.df['sales'], model='multiplicative', extrapolate_trend='freq')
        # Additive Decomposition
        result_add = seasonal_decompose(self.df['sales'],  model='additive', extrapolate_trend='freq')
        # Plot
        result_mul.plot().suptitle('Multiplicative Decompose')
        result_add.plot().suptitle('Additive Decompose')
        # Get Monthly Sales  
        # Setup monthly with multiplicative decompose plot  
        fig, ax = plt.subplots()
        ax.plot(self.df.index, self.df["sales"],label="Sales")
        ax.plot(self.df.index, result_mul.trend, label="Sales w/o Seasonality")
        ax.legend(loc = 'upper left')
        plt.gca().set(title="Monthly Retail and Food Services Sales\n(with multiplicative decompose)", xlabel="Months", ylabel="Sales")
        print("Completed: Monthly Sales without Seasonality")
    
    
    # Monthly Sales without Seasonality and a 5 Month Moving Average
    def get_monthly_decompose_mma(self):
        print("Processing: Monthly Sales without Seasonality and a 5 Month Moving Average")
        df_rolling = self.df.copy(deep=True)
        # Multiplicative Decomposition 
        result_mul = seasonal_decompose(self.df['sales'], model='multiplicative', extrapolate_trend='freq')
        df_rolling["sales"] = result_mul.trend.rolling(5).mean()
        # Drop missing values caused by 5 month rolling average
        df_rolling.dropna(inplace=True)
        # Draw monthly plot  
        fig, ax = plt.subplots()
        ax.plot(self.df.index, self.df["sales"], label="Sales", color="red")
        ax.plot(df_rolling.index, df_rolling["sales"], label="Sales 5MA w/o Seasonality", color="green")
        ax.legend(loc = 'upper left')
        plt.gca().set(title="Monthly Retail and Food Services Sales\n(with multiplicative decompose and 5 month moving averages (5MA))", xlabel="Months", ylabel="Sales")
        print("Completed: Monthly Sales without Seasonality and a 5 Month Moving Average")

    def show_reports(self):
        self.setup_df()
        self.get_monthly()
        self.get_annual()
        self.get_monthly_decompose()
        self.get_monthly_decompose_mma()
        # Draw all plots
        plt.show()

