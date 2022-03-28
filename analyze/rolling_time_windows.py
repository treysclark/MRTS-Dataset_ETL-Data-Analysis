import pandas as pd
import matplotlib.pyplot as plt
from statsmodels.tsa.seasonal import seasonal_decompose

import load.manage_db as manage_db

class RollingTimeWindow:

    def __init__(self):
        self.df = None

    def setup(self):
        self.df = manage_db.read_rolling_time()
        # Check for missing values
        sum_nans = self.df.isna().sum()        
        if sum_nans.sum() == 0:
            nan_msg = "Verification: No missing values (nans)"
        else:
            nan_msg = f"Verification: missing values (nans)\n\t{sum_nans}"
            nan_msg += "\n Nans will be interpolated."
            # Interpolate for missing values
            self.df.interpolate(inplace=True)
        print(nan_msg)


    # Monthly Sales
    def get_monthly(self):
        print("Processing: Monthly Sales")
        df_monthly = self.df.copy(deep=True)
        # Setup monthly plot
        fig, ax = plt.subplots()
        ax.plot(df_monthly.index, df_monthly["new_cars"], label="New")
        ax.plot(df_monthly.index, df_monthly["used_cars"], label="Used")
        ax.legend(loc = 'upper left')
        plt.gca().set(title="Monthly Car Sales", xlabel="Years", ylabel="Sales")
        print("Completed: Monthly Sales")


    # Monthly Sales 5MA
    def get_monthly_5ma(self):
        print("Processing: Monthly Sales 5MA")
        df_rolling = self.df.copy(deep=True)
        df_rolling["new_cars"] = self.df["new_cars"].rolling(5).mean()
        df_rolling["used_cars"] = self.df["used_cars"].rolling(5).mean()
        # Drop missing values caused by 5 month rolling average
        df_rolling.dropna(inplace=True)
        # Setup monthly with 5 month MA plot  
        fig, ax = plt.subplots()
        ax.plot(df_rolling.index, df_rolling["new_cars"], label="New w/ 5MA", color="green")
        ax.plot(df_rolling.index, df_rolling["used_cars"], label="Used w/ 5MA", color="purple")
        ax.legend(loc = 'upper left')
        plt.gca().set(title="Monthly New and Used Car Dealers Sales\n(with 5 Month Moving Average (5MA))", xlabel="Months", ylabel="Sales")
        print("Completed: Monthly Sales 5MA")


    # Monthly Sales 12MA
    def get_monthly_12ma(self):
        print("Processing: Monthly Sales 12MA")
        df_rolling = self.df.copy(deep=True)
        df_rolling["new_cars"] = self.df["new_cars"].rolling(12).mean()
        df_rolling["used_cars"] = self.df["used_cars"].rolling(12).mean()
        # Drop missing values caused by 12 month rolling average
        df_rolling.dropna(inplace=True)
        # Setup monthly with 12 month MA plot  
        fig, ax = plt.subplots()
        ax.plot(df_rolling.index, df_rolling["new_cars"], label="New w/ 12MA", color="green")
        ax.plot(df_rolling.index, df_rolling["used_cars"], label="Used w/ 12MA", color="purple")
        ax.legend(loc = 'upper left')
        plt.gca().set(title="Monthly New and Used Car Dealers Sales\n(with 12 Month Moving Average (12MA))", xlabel="Months", ylabel="Sales")
        print("Completed: Monthly Sales 12MA")


    # Draw all plots
    def show_reports(self):
        self.setup()
        self.get_monthly()
        self.get_monthly_5ma()
        self.get_monthly_12ma()
        plt.show()