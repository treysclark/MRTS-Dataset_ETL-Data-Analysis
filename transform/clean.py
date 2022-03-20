
import pandas as pd
import extract.get_sales_df as sales_dfs
import transform.eval_nans as eval_nans
import sys

class Clean:

    def __init__(self):
        self.df_combined = None
        self.df_store = None
        self.evals = None


    def show_nans(self):    
        print("Processing: retrieving data from census.gov")
        # Get all sales dataframes
        self.df_combined = sales_dfs.GetSalesDF("combined_sales").df_combined_sales
        self.df_store = sales_dfs.GetSalesDF("store_sales").df_store_sales
        print("Completed: retrieved data from census.gov")

        self.evals = eval_nans.EvalNames(self.df_store)
        self.show_store_nans()


    def get_combined_sales(self):    
        print("Processing: retrieving combined_sales data from census.gov")
        # Get all sales dataframes
        self.df_combined = sales_dfs.GetSalesDF("combined_sales").df_combined_sales
        print("Completed: retrieved combined_sales data from census.gov")
        return self.df_combined


    def get_cleaned_store_sales(self):    
        print("Processing: retrieving store_sales data from census.gov")
        # Get all sales dataframes
        self.df_store = sales_dfs.GetSalesDF("store_sales").df_store_sales
        print("Completed: retrieved store_sales data from census.gov")

        self.evals = eval_nans.EvalNames(self.df_store)
        self.show_store_nans()
        self.remove_store_nan_dfs()
        return self.df_store


    def show_store_nans(self):
        # Print nans by cat_code and then by year
        print(self.evals.msg_cat_code_year_nans)
       

    def remove_store_nan_dfs(self):

        # Notify user of status
        print(f"Processing: dropping or interpolating all nans")

        # Used to do quick check of expected record removal
        prev_record_count = self.df_store.shape[0]
        expected_records_interpolated = self.evals.count_interpolations
   
        # Interpolate grouped nan dataframe then merge with df_store if it has between (1-3) nans per year
        df_interpolated = self.evals.df_nans_interpolate.interpolate()
        # Match the interpolated values with the df_store dataframe based on the same cat_code, cat_name, and sales_date
        self.df_store = self.df_store.merge(df_interpolated, how='left', left_on=['cat_code', 'cat_name','sales_date'], right_on=['cat_code', 'cat_name','sales_date'])
        # Merge the two sales columns into one new column, then drop extra columns
        self.df_store ['sales'] = self.df_store['sales_x'].where(self.df_store['sales_x'].notnull(), self.df_store['sales_y'])    
        self.df_store.drop(['sales_x','sales_y'],axis=1, inplace=True)

        # Remove grouped nan dataframes from df_store if they contain too many nans (>3) by year to be interpolated 
        df_dropped = self.evals.df_nans_drop
        self.df_store = pd.concat([self.df_store, df_dropped, df_dropped]).drop_duplicates(keep=False)

        # Quick check that the correct number of records were removed
        dropped_record_count = df_dropped.shape[0]
        expected_record_count = prev_record_count - self.df_store.shape[0] 
        record_removal_diff = expected_record_count - dropped_record_count 
        # Notify user
        if record_removal_diff == 0:
            print(f"Completed: dropped {expected_record_count} nans and interpolated {expected_records_interpolated} nans")
        else:
            print(f"----Variance: expected ({expected_record_count}) and actual ({dropped_record_count}) record removal vary by {record_removal_diff}")
            sys.exit(1)


    # Used for data validation
    def count_store_records(self):

        print("Processing: retrieving store_sales data from census.gov")
        # Get store sales dataframes
        self.df_store = sales_dfs.GetSalesDF("store_sales").df_store_sales
        print("Completed: retrieved store_sales data from census.gov")

        self.evals = eval_nans.EvalNames(self.df_store)

        # Notify user of status
        print(f"Processing: counting number of nan rows")

        count_nan_records = self.evals.df_nans_drop.shape[0]

        # Notify user
        print(f"Completed: counted number of nan rows")

        # Get number of all records in store_sales including removed nans
        count_all_records = self.df_store.shape[0]        

        return {"count_all_records":count_all_records, "count_nan_records":count_nan_records}


