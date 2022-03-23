# ETL MRTS Data Analysis

The goal of this project was to analyze the "Monthly Retail Trade Survey" dataset from the U.S. Census Bureau. Then present that analysis using data visualization tools. The following report will present the findings and the steps taken from start to finish. The steps included the following:

- Researching information about the dataset, such as the meaning of NAICS codes and the data gathering process
- Extracting the dataset from an Excel workbook hosted on the U.S. Census website
- Transforming the dataset so that extraneous columns and rows were excluded, coded values were translated, and disaggregated some rows, creating schemas and installation scripts
- Loading the dataset into separate MYSQL tables, validating completeness and accuracy with source files
- Querying the database using conditionals such "CASE" and "WHERE" statements, aggregating by
- Examining trends, percentage of changes, and rolling time trends
- Implementing smoothing techniques such as removing seasonality and using moving averages
- Creating plots that compared different variables for different periods
- Providing historical context


## Data Exploration:
The Monthly Retail Trade Survey is part of a group of surveys conducted by the U.S. Census Bureau that provides comprehensive data on U.S. retail economic activity.

The survey sample size is currently about 13,000 employer firms. Sample revisions occur every 5 - 7 years to ensure the samples are representative, efficient, up-to-date, and accurate. Federal statisticians create sales estimates based on these surveys. The current dataset includes sales estimates from 1992 to part of 2021.

The goal of the survey is to provide timely information about retail and food services store sales and retail store inventories. It also helps provide an objective assessment for public policy and business decisions.

The MRTS utilizes the North American Industry Classification System (NAICS), the standard used by Federal statistical agencies to classify business establishments. MRTS includes retailers who sell through brick-and-mortar stores, paper and or electronic solicitation, door to door, infomercials, vending machines, e-commerce, etc.


## Extraction:
The MRTS dataset was accessed from the [U.S. Census website](https://www.census.gov/retail/index.html). The raw dataset is in the form of an Excel Workbook, where annual sales are separated into separate sheets. The ETL workflow will only gather sales from 1992 to 2021, since 2022 is still in progress at the time of this writing.

![Example of Excel dataset](/images/extract/dataset.png)

A loop was created in Python to load the 29 sheets (years) into DataFrames using the Pandas library.

![Loading Excel dataset using Pandas](/images/extract/pandas-excel-access.png)

The dataset was then processed based on whether it represented monthly combined sales, monthly store sales, or annual totals.

- **Combined Sales**: Monthly combined sales are the aggregation of the monthly store sales. Monthly combined sales do not have NAICS codes and are not likely to contain missing data. 

    ![Creating combined sales DataFrame](/images/extract/combined-sales.png)


- **Store Sales**: Monthly store sales are more likely to contain missing data. Depending on the year, missing data was represented by ```(S)``` and ```(NA)```. These values were replaced with ```nans``` using Pandas ```replace``` function.

    ![Creating store sales DataFrame](/images/extract/store-sales.png)



- **Annual Sales**:
Annual sales represent both combined and store annual totals. These totals are used to validate the values inserted into the MYSQL database, which is discussed later.

    ![Creating annual sales DataFrame](/images/extract/annual-sales.png)


## Transform:
It was only necessary to clean the monthly store sales. That process consisted of the following:

- Store sales are grouped first by their NAICS codes and then by year. 
  - This is helpful in counting how many months a particular NAICS code has with missing sales.  

    ![Display nans by group](/images/transform/eval-nans.png)

  - If the store sales have less than four missing monthly sales in a year, then they are interpolated.

    ![Interpolate nans by group](/images/transform/interpolate-nans.png)


  - Store sales that have multiple years of missing values for multiple months are dropped, since it would be ineffective to interpolate the missing values.

    ![Remove nans by group](/images/transform/drop-nans.png)

## Load:
The load process includes utilizing the MYSQL database and validating that the stored data matched the source dataset.

### Managing MYSQL:
The MYSQL instance is managed in the following manner:
- **Connections**: 
  - **MYSQL Connector**: This library is used for DDL and DQL commands.

    ![MYSQL Connector](/images/load/mysql-connector.png)

  - **SQLAlchemy**: This library is used for DML commands, primarily insert.

    ![MYSQL Connector](/images/load/sqlalchemy.png)

- **DDL Commands**: 
  - **Create**: In addition to creating the MRTS database, two tables are created to store the combined store sales and individual store sales.

    ![Create Command](/images/load/create.png)

  - **Truncate**: Emptying the tables is helpful when restarting the ETL process.

    ![Truncate Command](/images/load/empty.png)

  - **Drop**: The user has the option of removing the MRTS database or just the tables.

    ![Drop Commands](/images/load/drop.png)

- **DQL Commands**: 
  - **Read**: These commands are used in the data validation process. There are other read commands that are discussed in the analysis section.

    ![Read Command](/images/load/read.png)

- **DML Commands**: 
  - **Insert**: SQLAlchemy is used to quickly insert batches of records from the transformed DataFrames directly into MYSQL.

    ![Insert Command](/images/load/insert.png)
    
### Validation:
The accurracy of the database is validated by record count and annual sales.

- **Record Count**: These functions compare the number of records inserted into the database to the number of monthly sales in the source data. Comparisons are made on the following two categories:
    -  **Combined Sales**: 
        ![Validate Combined Sales](/images/load/validate-combined-sales.png)
        
    -  **Store Sales**: 
        ![Validate Store Sales](/images/load/validate-store-sales.png)

- **Annual Sales**: The MRTS source dataset calculates the annual totals per year for each combined sale and store. These sums are labeled either as "Total" or "CY" for cummulative total. However, if a store has any missing values it's annual total is not calculated in the source dataset. So, the validation of annual sales does not take these rows missing values into account. This should not be a problem because some of the source dataset when grouped by year and NAICS code are missing a significant amount of consecutive monthly sales, which are dropped during the transformation process. 
    ![Validate Totals](/images/load/validate-totals.png) 

The user is notified if the validation process identifies any variances between the source dataset and the database records.
![Validation Msg](/images/load/validation-msg.png) 

## Analysis:
The main focus of this project was on the ETL workflow. However some analysis was done as well, which consists of the following:

### Trends: 
The following section analyzes the economic trends found in some of the results. Economic trends are various indicators that show the financial health of a region or country. These trends are analyzed by various professionals. A few examples of why these trends are monitored are as follows [6]:

- Identifying the level of productivity
- Develop forecasts
- Evaluate regulatory requirements
- Identify business market share
- Analyze business potential
- Plan investments

#### Retail and Food Services Sales: 
- Analysis of the retail and food services category from 1992 to 2020 shows an upward trend in sales. There was a significant disruption to that upward trend in the aftermath of the 2007-2008 financial crisis, which preceded the Great Recession.

    - When using the monthly data, the trend becomes slightly hidden by seasonal variances. Monthly data can be smoothed by removing the seasonality from the data and optionally applying a moving average.
    
    ![Monthly Retail Sales](/images/analysis/monthly-combined.png)
    
    - However, the trend can be seen clearly when aggregating monthly sales to annual.
    
    ![Annual Combined Sales](/images/analysis/annual-combined.png) 
    

### Percent of Change:
    ![Percent of Change](/images/analysis/percent-change.png) 
    
### Rolling Time Windows: 
    ![Rolling Time Windows](/images/analysis/rolling-time.png) 
    

    
    
    
    
    
