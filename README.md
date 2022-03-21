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

    ![Remove nans by group](/images/transform/interpolate-nans.png)


  - Store sales that have multiple years of missing values for multiple months were dropped, since it would be ineffective to interpolate the missing values.

    ![Remove nans by group](/images/transform/drop-nans.png)

## Load
