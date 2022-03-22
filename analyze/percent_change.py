import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import load.manage_db as manage_db

class PercentChange:

    def __init__(self):
        self.df = None


    def setup(self):
        self.df = manage_db.read_percent_change()
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


    def get_monthly(self):
        print("Processing: Monthly Sales Percent of Change")
        # Get Monthly Sales Percent of Change
        # Set index, calculate percent of change
        df_monthly_PoC = self.df.set_index("sales_date").pct_change()
        # Setup monthly plot  
        fig, ax = plt.subplots()
        ax.plot(df_monthly_PoC.index, df_monthly_PoC["mens_clothing"], label="Men's")
        ax.plot(df_monthly_PoC.index, df_monthly_PoC["womens_clothing"], label="Women's")
        ax.legend(loc = 'upper left')
        plt.gca().set(title="Monthly Percent of Change in Clothing Store Sales", xlabel="Months", ylabel="Percent Change")
        print("Completed: Monthly Sales Percent of Change")


    # Get Annual Sales Percent of Change
    def get_annual_poc(self):
        print("Processing: Annual Sales Percent of Change")
        # Group by year while settting index, calculate percent of change
        df_annual_PoC = self.df.groupby(pd.Grouper(key='sales_date', freq='Y')).sum().pct_change()
        # Setup annual plot
        fig, ax = plt.subplots()
        ax.plot(df_annual_PoC.index, df_annual_PoC["mens_clothing"], label="Men's")
        ax.plot(df_annual_PoC.index, df_annual_PoC["womens_clothing"], label="Women's")
        ax.legend(loc = 'lower left')
        plt.gca().set(title="Annual Percent of Change in Clothing Store Sales", xlabel="Years", ylabel="Percent Change")
        print("Completed: Annual Sales Percent of Change")


    # Get Annual Sales Percent of Whole
    def get_annual_pow(self):
        print("Processing: Annual Sales Percent of Whole")
        # Create a separate copy of dataframe
        df_PoW = self.df.copy(deep=True)
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
        print("Completed: Annual Sales Percent of Whole")


    # Get Annual Sales Percent of Change
    def get_annual_poc(self):
        print("Processing: Annual Sales Percent of Change")
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
        print("Completed: Annual Sales Percent of Change")


    def show_reports(self):
        self.setup()
        self.get_monthly()
        self.get_annual_poc()
        self.get_annual_pow()
        self.get_annual_poc()
        # Draw all plots
        plt.show()

