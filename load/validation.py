
from time import perf_counter
import sys
import pandas as pd
import numpy as np

from transform.clean import Clean
import load.manage_db as manage_db
from extract.get_sales_df import GetSalesDF

def validate_all(df_all_sales):
    validate_combined_record_count(df_all_sales['df_combined'].shape[0])
    validate_store_record_count(df_all_sales["orig_store_record_count"], df_all_sales["dropped_record_count"])
    validate_totals(df_all_sales['df_annual'])


def validate_combined_record_count(count_source_combined):
    start_time = perf_counter()
    # Verify accuracy of combined_sales db insertion
    count_db_combined  = manage_db.read_combined_sales_count()
    # Notify user
    msg_combined_no_var = f"""Completed: The combined_sales records in the database ({count_db_combined}) 
    equals the combined_sales in the source data ({count_source_combined}). Validated in {round(perf_counter()-start_time,4)} seconds."""
    msg_combined_var = f"""---- Variance: The combined_sales records in the database ({count_db_combined}) 
    does not equals the combined_sales in the source data ({count_source_combined})."""
    if count_source_combined == count_db_combined:
        print(msg_combined_no_var)
    else:
        print(msg_combined_var)
        sys.exit(1)


def validate_store_record_count(count_source_store_records, count_source_store_nans):
    start_time = perf_counter()
    # Verify accuracy of store_sales db insertion
    count_db_store = manage_db.read_store_sales_count()
    # Notify user
    msg_store_no_var = f"""Completed: The store_sales records in the database ({count_db_store}) 
    equals the store_sales in the source data ({count_source_store_records}) less the nan rows removed ({count_source_store_nans}). Validated in {round(perf_counter()-start_time,4)} seconds."""
    msg_store_var = f"""---- Variance: The store_sales records in the database ({count_db_store}) 
    does not equals the store_sales in the source data ({count_source_store_records}) less the nan rows removed ({count_source_store_nans})."""
    if count_source_store_records - count_source_store_nans == count_db_store:
        print(msg_store_no_var)
    else:
        print(msg_store_var)
        sys.exit(1)


def validate_totals(df_source_totals):
    start_time = perf_counter()
    # Get annual totals from census.gov
    df_source_totals.sort_values(['year', 'cat_name'], ascending=[True, True], inplace=True)
    # Get totals from database
    df_db_totals = pd.DataFrame(columns=["year", "cat_name", "annual_sales"])
    db_totals = manage_db.read_calc_annual_sales()

    print('Processing: validating annual sales between source and db (excluding effects of nans)')
    # Add list of tuples to dataframe Source: https://stackoverflow.com/a/48220676/848353
    df_db_totals[["year", "cat_name", "annual_sales"]] = pd.DataFrame(db_totals)
    df_db_totals.sort_values(['year', 'cat_name'], ascending=[True, True], inplace=True)

    # Merge source and db by matching year and cat_name
    # The source data does not calculate totals when monthly sales are missing.
    # So, this validation excludes all nans regardless of whether they were dropped or interpolated
    df_merge = pd.merge(df_source_totals, df_db_totals, on= ["year", "cat_name"], suffixes=("_source", "_db"), how="inner", indicator=True)
    # Compare annual sales between source and db
    df_merge["match"] = np.where(df_merge["annual_sales_source"] != df_merge["annual_sales_db"], True, False)
    # Count number of variances annual sales between source
    count_var = df_merge["match"].sum()
    if count_var == 0:
        print(f"Completed: no variance in annual sales between source and db (excluding effects of nans). Validated in {round(perf_counter()-start_time,4)} seconds.")
    else: 
        print(f"---- Variance: there are {count_var} variance(s) in annual sales between source and db\nThe variances are as follows:")
        df_vars = df_merge[df_merge["match"] == True]
        print(df_vars.iloc[:,0:4])
        sys.exit(1)