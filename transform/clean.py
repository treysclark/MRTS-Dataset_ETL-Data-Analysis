
import pandas as pd
import extract.get_sales_df as sales_dfs
import transform.eval_nans as eval_nans


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
        # Based on evaluation all nans must be dropped
        #   Except 1: cat_code 4422 year 2020 
        #   Except 2: cat_code 442299 year 2020
        #   Except 3: cat_code 44811 year 2020
        keep_nans = [("4422", 2020), ("442299", 2020), ("44811", 2020)]

        # Notify user of status
        print(f"Processing: removing all nans except the following cat_codes by year\n\t{keep_nans}")

        # Used to do quick check of expected record removal
        prev_record_count = self.df_store.shape[0]
        count_rows_removed = 0

        for key, df_group in self.evals.dict_df_nans.items():
            if key not in keep_nans:
                # Remove grouped nan dataframes from df_store if they are not in the exclusion list 
                self.df_store = pd.concat([self.df_store, df_group, df_group]).drop_duplicates(keep=False)
                # Track rows removed from df
                count_rows_removed += 1

        # Quick check that the correct number of records were removed

        # Multiply by 12 since there are 12 months in a year
        actual_record_count = count_rows_removed * 12
        expected_record_count = prev_record_count - self.df_store.shape[0]
        record_removal_diff = expected_record_count - actual_record_count
        msg_success = f"Validation Success: Correct number of nan records {actual_record_count} removed"
        msg_variance = f"Validation Variance: expected ({expected_record_count}) and actual ({actual_record_count}) record removal vary by {record_removal_diff}"
        removal_chk_msg = msg_success if record_removal_diff == 0 else msg_variance
        print(removal_chk_msg)

        # Notify user
        print(f"Completed: removed all nans except the following cat_codes by year\n\t{keep_nans}")


    # Used for data validation
    def count_store_records(self):

        print("Processing: retrieving store_sales data from census.gov")
        # Get all sales dataframes
        self.df_store = sales_dfs.GetSalesDF("monthly_sales").df_store_sales
        print("Completed: retrieved store_sales data from census.gov")

        self.evals = eval_nans.EvalNames(self.df_store)
        # Based on evaluation all nans must be dropped, since they are missing multiple years of sales data
        #   Except 1: cat_code 4422 year 2020 
        #   Except 2: cat_code 442299 year 2020
        #   Except 3: cat_code 44811 year 2020
        # The exceptions above should be interpolated since they are only missing a few months out of the year
        keep_nans = [("4422", 2020), ("442299", 2020), ("44811", 2020)]

        # Notify user of status
        print(f"Processing: counting number of nan rows except the following cat_codes by year\n{keep_nans}")

        count_nan_rows = 0

        for key, df_group in self.evals.dict_df_nans.items():
            if key not in keep_nans:
                # Track rows of nans
                count_nan_rows += 1

        # Notify user
        print(f"Completed: counted number of nan rows except the following cat_codes by year\n{keep_nans}")

        # Get number of all records in store_sales including removed nans
        count_all_records = self.df_store.shape[0]        
        # Number of nan rows are multiplied by 12 since there are 12 months in a year
        count_nan_records = count_nan_rows * 12

        return {"count_all_records":count_all_records, "count_nan_records":count_nan_records}


