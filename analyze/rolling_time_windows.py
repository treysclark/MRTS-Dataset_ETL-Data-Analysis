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

    # Annual Sales
    def get_annual(self):
        print("Processing: Annual Sales")
        # Group by year
        df_annual = self.df.groupby(pd.Grouper(key='sales_date', freq='Y')).sum()
        # Setup annual plot
        fig, ax = plt.subplots()
        ax.plot(df_annual.index, df_annual["new_cars"], label="New")
        ax.plot(df_annual.index, df_annual["used_cars"], label="Used")
        ax.legend(loc = 'upper left')
        plt.gca().set(title="Annual Car Sales", xlabel="Years", ylabel="Sales")
        print("Completed: Annual Sales")


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
        ax.plot(df_rolling.index, self.df["new_cars"].iloc[4:], label="New", color="red")
        ax.plot(df_rolling.index, df_rolling["new_cars"], label="New w/ 5MA", color="green")
        ax.plot(df_rolling.index, self.df["used_cars"].iloc[4:], label="Used", color="orange")
        ax.plot(df_rolling.index, df_rolling["used_cars"], label="Used w/ 5MA", color="purple")
        ax.legend(loc = 'upper left')
        plt.gca().set(title="Monthly New and Used Car Dealers Sales\n(with 5 Month Moving Average (5MA))", xlabel="Months", ylabel="Sales")
        print("Completed: Monthly Sales 5MA")


    # Monthly Sales w/out Seasonality 
    def get_monthly_decompose(self):
        print("Processing: Monthly Sales w/out Seasonality ")
        self.df.set_index("sales_date", inplace=True)
        # TEST: Extract trend and seasonality and see whether multiplicative or additive is better
        # Multiplicative Decomposition 
        result_mul_new = seasonal_decompose(self.df['new_cars'], model='multiplicative', extrapolate_trend='freq')
        result_mul_used = seasonal_decompose(self.df['used_cars'], model='multiplicative', extrapolate_trend='freq')
        # Additive Decomposition
        result_add_new = seasonal_decompose(self.df['new_cars'],  model='additive', extrapolate_trend='freq')
        result_add_used = seasonal_decompose(self.df['used_cars'],  model='additive', extrapolate_trend='freq')
        # Plot
        result_mul_new.plot().suptitle('Multiplicative Decompose')
        result_add_new.plot().suptitle('Additive Decompose')
        result_mul_used.plot().suptitle('Multiplicative Decompose')
        result_add_used.plot().suptitle('Additive Decompose')
        # Setup monthly with multiplicative decompose plot  
        fig, ax = plt.subplots()
        ax.plot(self.df.index, self.df["new_cars"], label="New", color="red")
        ax.plot(self.df.index, result_mul_new.trend, label="New w/o seasonality", color="green")
        ax.plot(self.df.index, self.df["used_cars"], label="Used", color="orange")
        ax.plot(self.df.index, result_mul_used.trend, label="Used w/o seasonality", color="purple")
        ax.legend(loc = 'upper left')
        plt.gca().set(title="Monthly New and Used Car Dealers Sales\n(with multiplicative decompose)", xlabel="Months", ylabel="Sales")
        print("Completed: Monthly Sales w/out Seasonality ")


    # Monthly Sales with 5MA Decompoose 
    def get_monthly_decompose_5ma(self):
        print("Processing: Monthly Sales with 5MA Decompoose")
        df_rolling_decompose = self.df.copy(deep=True)
        # Multiplicative Decomposition 
        result_mul_new = seasonal_decompose(self.df['new_cars'], model='multiplicative', extrapolate_trend='freq')
        result_mul_used = seasonal_decompose(self.df['used_cars'], model='multiplicative', extrapolate_trend='freq')
        df_rolling_decompose["new_cars"] = result_mul_new.trend.rolling(5).mean()
        df_rolling_decompose["used_cars"] = result_mul_used.trend.rolling(5).mean()
        # Drop missing values caused by 5 month rolling average
        df_rolling_decompose.dropna(inplace=True)
        # Draw monthly plot  
        fig, ax = plt.subplots()
        ax.plot(self.df.index, self.df["new_cars"], label="New", color="red")
        ax.plot(df_rolling_decompose.index, df_rolling_decompose["new_cars"], label="New 5MA w/o seasonality", color="green")
        ax.plot(self.df.index, self.df["used_cars"], label="Used", color="orange")
        ax.plot(df_rolling_decompose.index, df_rolling_decompose["used_cars"], label="Used 5MA w/o seasonality", color="purple")
        ax.legend(loc = 'upper left')
        plt.gca().set(title="Monthly New and Used Car Dealers Sales\n(with multiplicative decompose and 5 month moving averages (5MA))", xlabel="Months", ylabel="Sales")
        print("Completed: Monthly Sales with 5MA Decompoose")


    # Draw all plots
    def show_reports(self):
        self.setup()
        self.get_annual()
        self.get_monthly_5ma()
        self.get_monthly_decompose()
        self.get_monthly_decompose_5ma()
        plt.show()