import pandas as pd
import matplotlib.pyplot as plt

import load.manage_db as manage_db


class TrendComparisons:

    def __init__(self):
        self.df = None

    def setup(self):
        self.df = manage_db.read_trends()
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
    def get_monthly_trends(self):
        print("Processing: Monthly Sales")
        # Draw monthly plot  
        fig, ax = plt.subplots()
        ax.plot(self.df["sales_date"], self.df["sport_sales"], label="Sports")
        ax.plot(self.df["sales_date"], self.df["hobby_sales"], label="Hobby")
        ax.plot(self.df["sales_date"], self.df["book_sales"], label="Books")
        ax.legend(loc = 'upper left')
        plt.gca().set(title="Monthly Sales", xlabel="Months", ylabel="Sales")
        print("Completed: Monthly Sales")


    # Annual Sales
    def get_annual_trends(self):
        print("Processing: Annual Sales")
        # Group by year
        df_annual = self.df.groupby(pd.Grouper(key='sales_date', freq='Y')).sum()
        # Setup annual plot
        fig, ax = plt.subplots()
        ax.plot(df_annual.index, df_annual["sport_sales"], label="Sports")
        ax.plot(df_annual.index, df_annual["hobby_sales"], label="Hobby")
        ax.plot(df_annual.index, df_annual["book_sales"], label="Books")
        ax.legend(loc = 'upper left')
        plt.gca().set(title="Annual Sales", xlabel="Years", ylabel="Sales")
        print("Completed: Annual Sales")

    def show_reports(self):
        self.setup()
        self.get_monthly_trends()
        self.get_annual_trends()
        # Draw all plots
        plt.show()