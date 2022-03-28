# ETL MRTS Data Analysis

## Table of Contents
1. [Overview](#overview)
2. [Data Exploration](#data-exploration)
3. [Extract](#extract)
4. [Transform](#transform)
5. [Load](#load)
6. [Analyze](#analyze)
7. [Control](#control)
8. [Conclusion](#conclusion)
9. [References](#references)

## Overview:
The goal of this project was to create a ETL pipeline of the "Monthly Retail Trade Survey" dataset from the U.S. Census Bureau. Then analyze the data using data visualization tools. The following report will present the findings and the steps taken from start to finish. The steps included the following:

- Researching information about the dataset, such as the meaning of NAICS codes and the data gathering process
- Extracting the dataset from an Excel workbook hosted on the U.S. Census website
- Transforming the dataset so that extraneous columns and rows were excluded and missing values were either dropped or interpolated
- Loading the dataset into separate MYSQL tables, validating completeness and accuracy with source files
- Querying the database using unions and conditionals such as "CASE" and "WHERE" statements, and aggregating columns
- Examining trends, percentage of changes, and rolling time trends
- Implementing smoothing techniques such as removing seasonality and using moving averages
- Creating plots that compared different variables for different periods
- Providing historical context


## Data Exploration:
The Monthly Retail Trade Survey is part of a group of surveys conducted by the U.S. Census Bureau that provides comprehensive data on U.S. retail economic activity.

The survey sample size is currently about 13,000 employer firms. Sample revisions occur every 5 - 7 years to ensure the samples are representative, efficient, up-to-date, and accurate. Federal statisticians create sales estimates based on these surveys. The current dataset includes sales estimates from 1992 to part of 2021.

The goal of the survey is to provide timely information about retail and food services store sales and retail store inventories. It also helps provide an objective assessment for public policy and business decisions.

The MRTS utilizes the North American Industry Classification System (NAICS), the standard used by Federal statistical agencies to classify business establishments. MRTS includes retailers who sell through brick-and-mortar stores, paper and or electronic solicitation, door to door, infomercials, vending machines, e-commerce, etc.


## Extract:
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
        ![Validate Combined Sales](/images/load/validate-combined-records.png)
        
    -  **Store Sales**: 
        ![Validate Store Sales](/images/load/validate-store-records.png)

- **Annual Sales**: The MRTS source dataset calculates the annual totals per year for each combined sale and store. These sums are labeled either as "Total" or "CY" for cummulative total. However, if a store has any missing values it's annual total is not calculated in the source dataset. So, the validation of annual sales does not take these rows missing values into account. This should not be a problem because some of the source dataset when grouped by year and NAICS code are missing a significant amount of consecutive monthly sales, which are dropped during the transformation process. 
    ![Validate Totals](/images/load/validate-totals.png) 

The user is notified if the validation process identifies any variances between the source dataset and the database records.
![Validation Msg](/images/load/validation-msg.png) 

## Analyze:
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

    - When using the monthly data, the trend becomes slightly hidden by seasonal variances. 
    
    ![Monthly Combined Sales](/images/analyze/trends/monthly-combined.png)
    ![Monthly Combined Sales Code](/images/analyze/trends/monthly-combined-code.png)
    
    - Monthly data can be smoothed by removing the seasonality from the data.

    ![Monthly Combined Sales w/o Seasonality](/images/analyze/trends/monthly-combined-decompose.png)
    ![Monthly Combined Sales w/o Seasonality Code](/images/analyze/trends/monthly-combined-decompose-code.png)
    
    - The trend can also be seen clearly when aggregating monthly sales to annual sales.
    
    ![Annual Combined Sales](/images/analyze/trends/annual-combined.png) 
    ![Annual Combined Sales Code](/images/analyze/trends/annual-combined-code.png) 

#### Bookstores, Sporting Goods Stores, and Hobbies, Toys, and Games Stores
- When comparing businesses like bookstores, sporting goods stores, and hobbies, toys, and games stores, it is clear from the charts that sporting goods stores have the highest trend. It grew faster than any other category, especially in the year 2020. That is not surprising since according to McKinsey and Company, there was a significant increase in home gym spending during the initial coronavirus outbreak [2]. However, it is surprising that bookstores began a downward trend during that same time when a large part of the population was staying home. It is hard to believe that online book sales did not increase. However, these sales are likely lumped into the business category, "Electronic shopping and mail-order houses," if the book stores have a separate fulfillment location than their brick-and-mortar stores [7].

    ![Monthly Book, Sports, Toys Sales](/images/analyze/trend_comparisons/monthly.png)
    ![Monthly Book, Sports, Toys Sales Code](/images/analyze/trend_comparisons/monthly-code.png)
    

- There is significant seasonality in these categories of store sales when looking at monthly data. That is especially true for sporting goods stores and hobbies, toys, and games stores. As a result, it is easier to see trends by aggregating the monthly sales to annual. Trends could also be easier to see using the smoothing techniques mentioned in the analysis of the retail and food services category.

    ![Annual Book, Sports, Toys Sales](/images/analyze/trend_comparisons/annual.png) 
    ![Annual Book, Sports, Toys Sales Code](/images/analyze/trend_comparisons/annual-code.png) 


### Percent of Change:

- The following analysis uses Pandas' Percent Change function to evaluate sales data from the men's and women's clothing stores. Percent of change is a mathematical formula for calculating the degree of change over time. Percentage of change is important for predicting spending patterns because it informs companies on how much to produce in the future. For example, exercise equipment manufacturer Peloton suffered significant damage to its stock price when it was reported that its production far exceeded consumer demand [7]. Timely forecasting is especially needed for companies who rely on Just-In-Time manufacturing. Companies that have lean inventories need to know when demand changes dramatically.

- The analysis shows that both types of clothing stores are subject to the same market forces.

    ![Percent of Change Monthly](/images/analyze/percent_change/poc-monthly.png)
    ![Percent of Change Monthly Code](/images/analyze/percent_change/poc-monthly-code.png)
    
    ![Percent of Change Annual](/images/analyze/percent_change/poc-annual.png)
    ![Percent of Change Annual Code](/images/analyze/percent_change/poc-annual-code.png)
    
- When comparing the men's and women's clothing stores' percentage of contribution to the whole clothing industry, it is clear that women's clothing stores have dominated sales since 1992 and likely before then as well. Women's clothing stores represented about 37% of clothing sales in 1992 and decreased to about 20% in 2019. Meanwhile, men's clothing only represented about 12% of clothing sales in 1992 and about 5% in 2019.

    ![Percent of Whole Monthly](/images/analyze/percent_change/pow-monthly.png)
    ![Percent of Whole Monthly Code](/images/analyze/percent_change/pow-monthly-code.png)
    
    ![Percent of Whole Annual](/images/analyze/percent_change/pow-annual.png)
    ![Percent of Whole Annual Code](/images/analyze/percent_change/pow-annual-code.png)
    
### Rolling Time Windows: 

- Rolling time windows are used by financial experts to smooth trends in spending patterns. Economic trends are often subject to seasonal variations, making it hard to see underlying trends. According to the Federal Reserve Bank of Dallas, data on its website is smoothed using a 3, 4, or 5-month moving average. Applying moving averages smooths out seasonal fluctuations in the data in order to reduce or remove short-term volatility.

    - It is important to remember there is a trade-off in smoothing data. While larger the moving averages make the data less volatile it decreases its timeliness. However, other moving averages can be used to offset the loss of timeliness, such as weighted moving averages or a centered moving average.

    - The following analysis compares new and used car dealer sales. The charts below show the results of monthly new and used car dealer sales with and without various rolling time windows.
    - **Monthly Sales - No Smoothing**:
        -  The first chart shows the monthly car sales without using monthly moving averages.
        
        ![Car Sales](/images/analyze/rolling_time/monthly.png) 
        ![Car Sales Code](/images/analyze/rolling_time/monthly-code.png) 
        
        
    - **5 Month Moving Average**:
        - This chart shows the monthly car sales with only a 5-month moving average applied. This increases the smoothness of the chart, but also reduces its timeliness.
    
        ![Rolling Time Windows 5MA Cars](/images/analyze/rolling_time/monthly-5MA.png) 
        ![Rolling Time Windows 5MA Cars Code](/images/analyze/rolling_time/monthly-5MA-code.png) 
    
    
    - **12 Month Moving Average**:
        - The last chart shows the monthly car sales with a 12-month moving average applied. The 12-month time period could has successfully smoothed the data. However, it has also further reduce the timeliness of the data [3].
         
        ![Rolling Time Windows 12MA Cars](/images/analyze/rolling_time/monthly-12MA.png) 
        ![Rolling Time Windows 12MA Cars Code](/images/analyze/rolling_time/monthly-12MA-code.png) 
    
    
## Control:
The control module manages all of the workflows for this project. Users can execute one of ten commands by adding command line arguments during the execution of the script. The commands are as follows:

- "**-etl**" Run the complete extract, transform, load (including data validation) workflow
    
    ![ETL Argument](/images/control/etl-arg.png) 
    
- "**-clean**" Retrieve the data and transform the store sales
     
     ![Clean Argument](/images/control/clean-arg.png) 
     
- "**-drop_db**" Delete the MRTS database from MYSQL
     
     ![Drop DB Argument](/images/control/drop-db-arg.png) 
     
- "**-drop_tables**" Delete the MRTS database tables (combined_sales and store_sales) from MYSQL
     
     ![Drop Tables Argument](/images/control/drop-tables-arg.png) 
- "**-empty_tables**" Truncate (empty the MRTS database tables (combined_sales and store_sales) from MYSQL while maintaining the table structure
     
     ![Empty Tables Argument](/images/control/empty-tables-arg.png) 
     
- "**-validate**" Verify that the record count and annual totals between the source dataset (Census.gov) and the MYSQL database match 
     
     ![Validate Argument](/images/control/validate-arg.png) 
     
- "**-analyze_trends**" Retrieve charts which compare monthly and annual sales data with and without seasonality 
     
     ![Analyze Trends Argument](/images/control/trends-arg.png) 
     
- "**-analyze_trend_comparisons**" Display charts which compare different store sales trends
     
     ![Analyze Trend Comparisons Argument](/images/control/trend-comparisons-arg.png)
     
- "**-analyze_percent**" Show charts that show percent change and percent whole calculations
     
     ![Analyze Percent Argument](/images/control/percent-arg.png) 
     
- "**-analyze_rolling**" Retrieve charts with moving averages
     
     ![Analyze Rolling Argument](/images/control/rolling-arg.png) 
     
        
## Conclusion:
The goal of the project was achieved. Users of the script can execute the whole ETL pipeline by including the "-etl" argument. 

Future iterations of this project will allow the user to do the following:
- Enter a starting and ending year to limit the amount of data processed. Or, since the dataset is pulled directly from the US Census Bureau's website, end year can be used to gather the most recently completed sales data. 
- Enter the NAICS codes to run analysis on instead of using hard-coded values. 
- An option will be given to allow the user to save charts to a folder instead of automatically displaying them.

## References
1. Brownlee, J. (2020, December 9). How to decompose time series data into trend and seasonality. Machine Learning Mastery. Retrieved January 22, 2022, from https://machinelearningmastery.com/decompose-time-series-data-trend-seasonality/

2. Falardeau, E., Glynn, J., & Ostromecka, O. (2021, June 22). Sweating for the fitness consumer. Our Insights. Retrieved January 22, 2022, from https://www.mckinsey.com/industries/consumer-packaged-goods/our-insights/sweating-for-the-fitness-consumer

3. Federal Reserve Bank of Dallas. (n.d.). Smoothing Data with Moving Averages. Dallasfed.org. Retrieved January 22, 2022, from https://www.dallasfed.org/research/basics/moving.aspx#:~:text=Economists%20use%20a%20simple%20smoothing,average%20of%20several%20months'%20data.

4. Mitchell, T. (2017, June 13). Managing bad data in ETL. Tim Mitchell. Retrieved January 22, 2022, from https://www.timmitchell.net/post/2017/02/16/managing-bad-data-in-etl/

5. PennState. (n.d.). 5.1 decomposition models: Stat 510. PennState: Statistics Online Courses. Retrieved January 22, 2022, from https://online.stat.psu.edu/stat510/lesson/5/5.1

6. Prabhakaran, S. (2021, December 19). Time series analysis in python - A comprehensive guide with examples - ML+. Machine Learning Plus. Retrieved January 22, 2022, from https://www.machinelearningplus.com/time-series/time-series-analysis-python/

7. Thomas, L. (2022, January 21). Peloton to halt production of its bikes, Treadmills as demand wanes. CNBC. Retrieved January 23, 2022, from https://www.cnbc.com/2022/01/20/peloton-to-pause-production-of-its-bikes-treadmills-as-demand-wanes.html

8. US Census Bureau: Rob Swartz (SSSD Division Security Coordinator), P. B. (S. S. S. D. (2009, January 16). US Census Bureau Retail Trade Monthly Retail Trade Survey Methodology Page. United States Census Bureau. Retrieved January 22, 2022, from https://www.census.gov/retail/mrts/how_surveys_are_collected.html
