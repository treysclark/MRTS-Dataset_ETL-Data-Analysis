
class EvalNames:
    
    def __init__(self, df_store):
        self.df_store = df_store

        # Store grouped dataframe with nans greater than zero
        self.dict_df_nans = {}
        # Store message from show_cat_code_year_nans
        self.msg_cat_code_year_nans = ""
        self.update_cat_code_year_nans()
        
        
    # Currently not used
    def show_year_nans(self):
        # Split df into groups by "cat_code"
        df_store_gk = self.df_store.groupby(self.df_store.sales_date.map(lambda x: x.year))
        # Loop through each group
        for name, group in df_store_gk:
            nans = group.sales.isna().sum()
            # Only show years that have nans
            if nans > 0:
                print(f"year: {name} has {nans} nans")


    # Currently not used
    def show_cat_code_nans(self):
        # Split df into groups by "cat_code"
        df_store_gk = self.df_store.groupby("cat_code")
        # Loop through each group
        for name, group in df_store_gk:
            nans = group.sales.isna().sum()
            # Only show cat_codes that have nans
            if nans > 0:
                print(f"cat_code: {name} has {nans} nans")


    def update_cat_code_year_nans(self):
        # Notify user of status
        self.msg_cat_code_year_nans += "Processing: displaying category codes (NAICS) that have missing values (nans) by year"

        # Split df into groups by "cat_code" then by the "sales_date" year
        df_store_gk = self.df_store.groupby(["cat_code", self.df_store.sales_date.map(lambda x: x.year)])

        cur_cat_code = ""
        prev_year = None
        is_non_consec = False
        is_diff_cat_code = False

        # Loop through each group
        for names, df_group in df_store_gk:

            nans = df_group.sales.isna().sum()
            # Only show years that have nans
            if nans > 0:
                # Split the two group by keys 
                cat_code, year = names

                # Check if cat_code has changed
                is_diff_cat_code = True if cur_cat_code != cat_code else False
                
                # Check if nans are from consecutive year
                is_non_consec = False if prev_year == None else True if prev_year != year - 1 else False

                # Don't print 'nonconsecutive' for change in cat_codes 
                if is_non_consec and not is_diff_cat_code:
                    self.msg_cat_code_year_nans += "\n\t\t--nonconsecutive year--"

                # Print cat_code as header
                if is_diff_cat_code:
                    self.msg_cat_code_year_nans += f"\n\tCat_code: {cat_code}"
                    cur_cat_code = cat_code
                    is_non_consec = False

                # Print nans by year if nans are greater than zero
                self.msg_cat_code_year_nans += f"\n\t\tYear: {year} has {nans} nans"

                # Track previous year
                prev_year = year

                # Store grouped dataframe with nans greater than zero
                self.dict_df_nans[names] = df_group

        self.msg_cat_code_year_nans += "\nCompleted: displayed category codes (NAICS) that have missing values (nans) by year"
