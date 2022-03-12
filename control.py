import os
import sys

import transform.clean as clean
import load.manage_db as manage_db
import load.validation as validation
import analyze.query_combined_sales as query_sales
import analyze.query_percent_change as query_percent
import analyze.query_rolling_time_windows as query_rolling
import analyze.query_trends as query_trends

getSalesDF = None


# Get CLAs
if len(sys.argv) > 1:
    # Prevent other scripts with CLAs from being called
    filename = os.path.basename(sys.argv[0])
    if filename == 'control.py':

        # Call relevant functions based on CLA
        argument = sys.argv[1]
        if argument == "-etl":
            manage_db.create_db()
            manage_db.create_tables()
            manage_db.insert_combined_sales()
            manage_db.insert_store_sales()
        if argument == "-clean":
            clean.Clean().get_combined_sales()
            clean.Clean().get_cleaned_store_sales()
        if argument == "-drop_db":
            manage_db.drop_db()
        if argument == "-drop_tables":
            manage_db.drop_tables()
        if argument == "-empty_tables":
            manage_db.empty_tables()
        if argument == "-validate":
            pass
        if argument == "-query_sales":
            query_sales.QueryCombinedSales().show_reports()
        if argument == "-query_percent":
            query_percent.QueryPercentChange().show_reports()
        if argument == "-query_rolling":
            query_rolling.QueryRollingTimeWindow().show_reports()
        if argument == "-query_trends":
            query_trends.QueryTrends().show_reports()
        else:
            print("""Argument not recognized please use one of the following:
                -etl, -clean, -drop_db, -drop_tables, -empty_tables, -validate,
                -query_sales, -query_percent, -query_rolling, -query_trends
                """)

