
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
        self.df_combined = sales_dfs.GetSalesDF().df_combined_sales
        self.df_store = sales_dfs.GetSalesDF().df_store_sales
        print("Completed: retrieved data from census.gov")

        self.evals = eval_nans.EvalNames(self.df_store)
        self.show_store_nans()

    def get_combined_sales(self):    
        print("Processing: retrieving combined sales data from census.gov")
        # Get all sales dataframes
        self.df_combined = sales_dfs.GetSalesDF().df_combined_sales
        print("Completed: retrieved combined sales data from census.gov")
        return self.df_combined

    def get_cleaned_store_sales(self):    
        print("Processing: retrieving store sales data from census.gov")
        # Get all sales dataframes
        self.df_store = sales_dfs.GetSalesDF().df_store_sales
        print("Completed: retrieved store sales data from census.gov")

        self.evals = eval_nans.EvalNames(self.df_store)
        self.show_store_nans()
        self.remove_store_nan_dfs()
        return self.df_store


    def show_store_nans(self):
        # Print nans by cat_code and then by year
        print(self.evals.msg_cat_code_year_nans)
       

    def remove_store_nan_dfs(self):
        
        # Based on evaluation all nans must be dropped
        # Except 1: cat_code 4422 year 2020 
        # Except 2: cat_code 442299 year 2020
        # Except 3: cat_code 44811 year 2020
        # Except 4: cat_code 722511 year 2020
        keep_nans = [("4422", 2020), ("442299", 2020), ("44811", 2020), ("722511", 2020)]

        # Notify user of status
        print(f"Processing: removing all nans except the following cat_codes by year\n\t{keep_nans}")

        # Used to do quick check of expected row removal
        prev_row_count = self.df_store.shape[0]
        rows_removed_count = 0

        for key, df_group in self.evals.dict_df_nans.items():
            if key not in keep_nans:
                # Remove grouped nan dataframes from df_store if they are not in the exclusion list 
                self.df_store = pd.concat([self.df_store, df_group, df_group]).drop_duplicates(keep=False)
                # Track rows removed from df
                rows_removed_count += 1

        # Quick check that the correct number of rows were removed
        actual_row_count = rows_removed_count * 12
        expected_row_count = prev_row_count - self.df_store.shape[0]
        row_removal_diff = expected_row_count - actual_row_count
        msg_success = f"Validation Success: Correct number of nan rows {actual_row_count} removed"
        msg_variance = f"Validation Variance: expected ({expected_row_count}) and actual ({actual_row_count}) row removal vary by {row_removal_diff}"
        removal_chk_msg = msg_success if row_removal_diff == 0 else msg_variance
        print(removal_chk_msg)

        # Notify user
        print(f"Completed: removed all nans except the following cat_codes by year\n\t{keep_nans}")

