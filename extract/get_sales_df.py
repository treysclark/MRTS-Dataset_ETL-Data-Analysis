import pandas as pd
import numpy as np
from datetime import datetime

class GetSalesDF:
    def __init__(self, load_type):

        self.cur_year = None
        self.df_raw = pd.DataFrame()
        self.df_totals = pd.DataFrame()
        self.df_combined_sales = pd.DataFrame()
        self.df_store_sales = pd.DataFrame()
        # Unformated Excel columns   
        self.col_names = ["cat_code", "cat_name", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"]

        if load_type == "combined_sales":
            self.load_combined_sales_df()
        elif load_type == "store_sales":
            self.load_store_sales_df()
        elif load_type == "annual_sales":
            self.load_total_df()
        

    # Load combined sales dataframes
    def load_combined_sales_df(self):
        # Skip over FY2022, which is in progress. Dataset will be based on 1992 - 2021
        for sheet_num in range(1, 30):
            self.cur_year = 2022 - sheet_num
            # Access Excel sheet from Census.gov website
            self.df_raw = pd.read_excel("https://www.census.gov/retail/mrts/www/mrtssales92-present.xls", sheet_name=sheet_num)
            # Get converted retail sales
            self.df_combined_sales = self.df_combined_sales.append(self.get_combined_df())


    # Load store sales dataframes
    def load_store_sales_df(self):
        # Skip over FY2022, which is in progress. Dataset will be based on 1992 - 2021
        for sheet_num in range(1, 30):
            self.cur_year = 2022 - sheet_num
            # Access Excel sheet from Census.gov website
            self.df_raw = pd.read_excel("https://www.census.gov/retail/mrts/www/mrtssales92-present.xls", sheet_name=sheet_num)
            # Get converted store sales
            self.df_store_sales = self.df_store_sales.append(self.get_store_df())


    # Load totals (annual sales) for retail combined sales and store sales dataframes
    def load_total_df(self):
        print("Processing: retrieving annual sales from census.gov")
        # Skip over FY2022, which is in progress. Dataset will be based on 1992 - 2021
        for sheet_num in range(1, 30):
            self.cur_year = 2022 - sheet_num
            # Access Excel sheet from Census.gov website
            self.df_raw = pd.read_excel("https://www.census.gov/retail/mrts/www/mrtssales92-present.xls", sheet_name=sheet_num)
            # Get annual sales
            self.df_totals = self.df_totals.append(self.get_totals_df())
        # Census.gov does not calculate totals for any categories with missing monthly values
        # So, drop those rows from the df Source: https://stackoverflow.com/a/45466263/848353
        self.df_totals.dropna(subset=['annual_sales'], inplace=True)
        print(f"Completed: retrieved annual sales ({'{:,}'.format(self.df_totals.shape[0])} records) from census.gov")

    # Get retail combined sales
    def get_combined_df(self):
        # Access retail sales section
        df_combined_unf = self.df_raw.iloc[5:12, 1:14]

        # Assign column names except "cat_code", which is not part of retail sales
        df_combined_unf.columns=self.col_names[1:]

        # Create temp df
        df_combined = pd.DataFrame(columns=["sales_date", "sales", "cat_name"])

        # Assign sales from all months
        df_combined["sales"] = df_combined_unf.iloc[:,1:14].stack()

        # Loop thru 1st index (row number)
        for row in range(5,12):
            # Assign cat_name to the correct rows
            df_combined.loc[row]["cat_name"] = df_combined_unf.cat_name.loc[row]

            # Loop through 2nd index (month number)
            for month in range(1,13):
                # Assign sales_date with month based on 2nd index
                df_combined.loc[row].loc[f"{month}"]["sales_date"] = datetime.strptime(f"{self.cur_year}-{month}-1", '%Y-%m-%d').date()

        # Drop row tracker from index
        df_combined = df_combined.droplevel(0)
        
        # Replace "(NA)" and "(S)" values with nans
        df_combined["sales"].replace(to_replace=["(NA)","(S)"], value=np.nan, inplace=True)
        
        return df_combined


    # Get store sales
    def get_store_df(self):
        # Access retail sales section
        df_store_unf = self.df_raw.iloc[12:71, 0:14]
        
        # Assign column names
        df_store_unf.columns=self.col_names

        # Create temp df
        df_store = pd.DataFrame(columns=["sales_date", "sales", "cat_code", "cat_name"])

        # Assign sales from all months
        df_store["sales"] = df_store_unf.iloc[:,2:15].stack()
        
        # Loop thru 1st index (row number)
        for row in range(12,70):
            # Assign cat_code and cat_name to the correct rows
            df_store.loc[row]["cat_code"] = f"{df_store_unf.cat_code.loc[row]}"
            df_store.loc[row]["cat_name"] = df_store_unf.cat_name.loc[row]

            # Loop through 2nd index (month number)
            for month in range(1,13):
                # Assign sales_date with month based on 2nd index
                df_store.loc[row].loc[f"{month}"]["sales_date"] = datetime.strptime(f"{self.cur_year}-{month}-1", '%Y-%m-%d').date()

        # Drop row tracker from index
        df_store = df_store.droplevel(0)
        
        # Replace "(NA)" and "(S)" values with nans
        df_store["sales"].replace(to_replace=["(NA)","(S)"], value=np.nan, inplace=True)
        
        return df_store


    # Get annual sales from combined and store sales
    def get_totals_df(self):
        df_source_totals = pd.DataFrame(columns=["year", "cat_name", "annual_sales"])
        # Assign totals (either total or CY) along corresponding category name (NAICS)
        df_source_totals["cat_name"] = self.df_raw.iloc[5:69,1]
        df_source_totals["annual_sales"] = self.df_raw.iloc[5:69,14]
        df_source_totals['year'] = self.cur_year
        return df_source_totals