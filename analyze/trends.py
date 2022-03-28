
import pandas as pd
import matplotlib.pyplot as plt
from statsmodels.tsa.seasonal import seasonal_decompose
import load.manage_db as manage_db

class Trends:

    def __init__(self):
        self.df = None


    def setup_df(self):
        # Create DataFrame and interpolate any possible missing values
        self.df = manage_db.read_combined_sales()
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
        ax.plot(self.df.index, result_mul.trend, label="Sales w/o Seasonality")
        ax.legend(loc = 'upper left')
        plt.gca().set(title="Monthly Retail and Food Services Sales\n(with multiplicative decompose)", xlabel="Months", ylabel="Sales")
        print("Completed: Monthly Sales without Seasonality")
    
    
    def show_reports(self):
        self.setup_df()
        self.get_monthly()
        self.get_annual()
        self.get_monthly_decompose()
        # Draw all plots
        plt.show()

