import sys
from transform.clean import Clean
import load.manage_db as manage_db

def validate_record_count():
    # Verify accuracy of combined_sales db insertion
    count_source_combined = Clean().get_combined_sales().shape[0]
    count_db_combined  = manage_db.read_combined_sales_count()
    msg_combined_no_var = f"""Validation Success: The combined_sales records in the database ({count_db_combined}) 
    equals the combined_sales in the source data ({count_source_combined})."""
    msg_combined_var = f"""Validation Fail: The combined_sales records in the database ({count_db_combined}) 
    does not equals the combined_sales in the source data ({count_source_combined})."""
    msg_combined_validation = msg_combined_no_var if count_source_combined == count_db_combined else msg_combined_var
    print(msg_combined_validation)

    # Verify accuracy of store_sales db insertion
    count_source_store =  Clean().count_store_records()
    count_source_all_store = count_source_store["count_all_records"]
    count_source_nan_store = count_source_store["count_nan_records"]
    count_db_store = manage_db.read_store_sales_count()
    msg_store_no_var = f"""Validation Success: The store_sales records in the database ({count_db_store}) 
    equals the store_sales in the source data ({count_source_all_store}) less the nan rows removed ({count_source_nan_store})."""
    msg_store_var = f"""Validation Fail: The store_sales records in the database ({count_db_store}) 
    does not equals the store_sales in the source data ({count_source_all_store}) less the nan rows removed ({count_source_nan_store})."""
    msg_store_validation = msg_store_no_var if count_source_all_store - count_source_nan_store == count_db_store else msg_store_var
    print(msg_store_validation)


def validate_totals():
    pass