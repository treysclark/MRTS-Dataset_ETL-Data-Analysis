import pandas as pd
import numpy as np
from datetime import datetime

class GetSalesDF:
    def __init__(self):
        self.df_combined_sales = pd.DataFrame()
        self.df_store_sales = pd.DataFrame()

        # Unformated Excel columns   
        self.col_names = ["cat_code", "cat_name", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"]

        # Load retail combined sales and store sales dataframes
        self.load_all_df()
        

    # Load retail combined sales and store sales dataframes
    def load_all_df(self):
        for sheet_num in range(0, 30):
            cur_year = 2021 - sheet_num
            
            # Access Excel
            df_raw = pd.read_excel("https://www.census.gov/retail/mrts/www/mrtssales92-present.xls", sheet_name=sheet_num)
            
            # Get converted retail sales
            self.df_combined_sales = self.df_combined_sales.append(self.get_combined_df(df_raw, cur_year))

            # Get converted store sales
            self.df_store_sales = self.df_store_sales.append(self.get_store_df(df_raw, cur_year))



    # Get retail combined sales
    def get_combined_df(self, df_raw, cur_year):
        # Access retail sales section
        df_combined_unf = df_raw.iloc[5:12, 1:14]
        
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
                df_combined.loc[row].loc[f"{month}"]["sales_date"] = datetime.strptime(f"{cur_year}-{month}-1", '%Y-%m-%d').date()

        # Drop row tracker from index
        df_combined = df_combined.droplevel(0)
        
        # Replace "(NA)" and "(S)" values with nans
        df_combined["sales"].replace(to_replace=["(NA)","(S)"], value=np.nan, inplace=True)
        
        return df_combined


    # Get store sales
    def get_store_df(self, df_raw, cur_year):
        # Access retail sales section
        df_store_unf = df_raw.iloc[12:71, 0:14]
        
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
                df_store.loc[row].loc[f"{month}"]["sales_date"] = datetime.strptime(f"{cur_year}-{month}-1", '%Y-%m-%d').date()

        # Drop row tracker from index
        df_store = df_store.droplevel(0)
        
        # Replace "(NA)" and "(S)" values with nans
        df_store["sales"].replace(to_replace=["(NA)","(S)"], value=np.nan, inplace=True)
        
        return df_store

