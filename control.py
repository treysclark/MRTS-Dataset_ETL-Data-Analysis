import os
import sys
from time import perf_counter

import transform.clean as clean
import load.manage_db as manage_db
import load.validation as validation
from analyze.trends import Trends
from analyze.percent_change import PercentChange
from analyze.rolling_time_windows import RollingTimeWindow
from analyze.trend_comparisons import TrendComparisons

getSalesDF = None


# Get CLAs
if len(sys.argv) > 1:
    # Prevent other scripts with CLAs from being called
    filename = os.path.basename(sys.argv[0])
    if filename == 'control.py':

        # Call relevant functions based on CLA
        argument = sys.argv[1]
        if argument == "-etl":
            start_time = perf_counter()
            manage_db.insert_all_sales()
            print(f"Completed: ETL of MRTS dataset in ", round(perf_counter()-start_time,4), " seconds.")  
            sys.exit(0)
        elif argument == "-clean":
            clean.Clean().get_combined_sales()
            clean.Clean().get_cleaned_store_sales()
            sys.exit(0)
        elif argument == "-drop_db":
            manage_db.drop_db()
            sys.exit(0)
        elif argument == "-drop_tables":
            manage_db.drop_tables()
            sys.exit(0)
        elif argument == "-empty_tables":
            manage_db.empty_tables()
            sys.exit(0)
        elif argument == "-validate":
            df_all_sales = clean.Clean().get_all_sales()
            validation.validate_all(df_all_sales)
            sys.exit(0)
        elif argument == "-analyze_trends":
            Trends().show_reports()
            sys.exit(0)
        elif argument == "-analyze_trend_comparisons":
            TrendComparisons().show_reports()
            sys.exit(0)
        elif argument == "-analyze_percent":
            PercentChange().show_reports()
            sys.exit(0)
        elif argument == "-analyze_rolling":
            RollingTimeWindow().show_reports()
            sys.exit(0)
        else:
            print("""---- Error: Argument not recognized please use one of the following:
                -etl, -clean, -drop_db, -drop_tables, -empty_tables, -validate,
                -analyze_trends, -analyze_trend_comparisons, -analyze_percent, 
                -analyze_rolling""")

