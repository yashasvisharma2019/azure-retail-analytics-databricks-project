# Azure Retail Data Engineering Project Documentation

## Project Overview
This project demonstrates an end-to-end Azure retail analytics pipeline built using a public retail dataset sourced from the GitHub repository [AZURE-DATA-ENGINEER---DATABRICKS-PROJECTS](https://github.com/manish040596/AZURE-DATA-ENGINEER---DATABRICKS-PROJECTS). The dataset was ingested through Azure Data Factory into Azure Data Lake Storage Gen2 under the storage account `pocretailproject00`, then processed in Databricks using a medallion-style structure with bronze and silver layers.

The objective was to clean the retail transaction data and build analytical outputs for daily revenue and purchases, revenue by payment method, store performance, loyalty-level revenue contribution, and product-category revenue distribution.

## Architecture
The implementation followed a simple cloud data engineering workflow:

1. **Source dataset**: Retail transaction dataset from the GitHub repository.
2. **Ingestion**: Azure Data Factory pipeline copied raw files into ADLS Gen2 `pocretailproject00`.
3. **Storage layers**: Databricks volumes were created for bronze, silver, and gold under the `retailproject.retail` schema.
4. **Processing**: Raw parquet data was read from the bronze layer, cleaned with PySpark, standardized, and written into the silver layer.
5. **Analysis and visualization**: SQL queries and Databricks visualizations were used on the cleaned silver dataset to produce business insights.

## Services Used
- **Azure Data Factory** for orchestration and raw data movement into the lake.
- **Azure Data Lake Storage Gen2** for centralized cloud storage using the storage account `pocretailproject00`.
- **Azure Databricks** for data cleaning, transformation, SQL analysis, and dashboards.
- **PySpark** for schema handling and transformation logic.

## Dataset and Schema
The dataset contains retail transaction fields such as `TransactionID`, `CustomerID`, `CustomerAge`, `Gender`, `ProductID`, `ProductName`, `ProductCategory`, `SubCategory`, `Brand`, `Quantity`, `Amount`, `Discount`, `PaymentType`, `StoreID`, `StoreLocation`, `StoreRegion`, `TransactionDate`, `ReturnStatus`, `DeviceUsed`, and `CustomerLoyaltyLevel`.

The raw records showed common quality issues such as inconsistent casing, extra spaces, null values in customer fields, and transaction timestamps stored as strings before cleaning.

## Step-by-Step Implementation

### 1. Databricks setup
A Databricks setup notebook was used to select the `retailproject` catalog, create the `retail` schema, and create three storage volumes named bronze, silver, and gold.

```sql
USE CATALOG retailproject;
CREATE SCHEMA IF NOT EXISTS retail;
CREATE VOLUME IF NOT EXISTS retailproject.retail.bronze;
CREATE VOLUME IF NOT EXISTS retailproject.retail.silver;
CREATE VOLUME IF NOT EXISTS retailproject.retail.gold;
```

### 2. Data ingestion with Azure Data Factory
The raw dataset was first gathered from the GitHub repository and then moved into ADLS using an Azure Data Factory copy pipeline. The ADF pipeline copied data from the source location into the bronze zone so that Databricks could consume it from cloud storage.

A screenshot of the Data Factory pipeline is shown below.

[image:1]

### 3. Bronze layer processing
The bronze notebook copied data from the ADLS path `abfss://retail@pocretailproject00.dfs.core.windows.net/bronze` into the Databricks bronze volume and then loaded the parquet file into a Spark DataFrame.

The notebook confirms a bronze read path similar to:

```python
sourcepath = "abfss://retail@pocretailproject00.dfs.core.windows.net/bronze"
bronzedf = spark.read.parquet("/Volumes/retailproject/retail/bronze/.../retaildata.parquet")
```

### 4. Data cleaning in Databricks
The raw data was cleaned in PySpark before writing it into the silver layer. The cleaning work included removing invalid records, normalizing text values, standardizing fields like payment type, store region, and device used, and converting transaction dates from strings into timestamps.

Observed examples in the raw data included values such as ` cash `, `UPI `, ` web `, `South `, and mixed-case variants like `WEB` and `web`, which were standardized during transformation.

The cleaning logic also filtered rows using non-null checks on critical fields such as `TransactionID`, `CustomerID`, and `TransactionDate`, then wrote the cleaned output into the silver volume in parquet format.

### 5. Silver layer analysis
The cleaned silver dataset was read in a second notebook and registered as a temporary view named `Retaildata` for SQL-based analysis.

```python
silverdf = spark.read.parquet("/Volumes/retailproject/retail/silver")
silverdf.createOrReplaceTempView("Retaildata")
```

### 6. Business questions answered
The final analysis focused on the following visual outputs:[file:2]

- Daily revenues and purchases.
- Revenue by payment method.
- Store performance.[file:2]
- Loyalty-level revenue contribution.
- Product-category revenue contribution.

## SQL and Visual Analysis
The silver notebook contains aggregation queries and Databricks visualizations built on the cleaned retail view.

### Daily revenue and purchases
A SQL query grouped data by transaction date and calculated total revenue and total purchases per day using `sum(Amount)` and `count(distinct TransactionID)`.

Example logic:

```sql
SELECT date(TransactionDate) AS TransactionDate,
       round(sum(Amount), 2) AS totalrevenue,
       count(distinct TransactionID) AS totalpurchase
FROM Retaildata
GROUP BY 1;
```
<img width="1342" height="500" alt="visualization" src="https://github.com/user-attachments/assets/3b00e120-e3e0-488b-b7db-07a9c1244bb1" />

### Revenue by payment method
The notebook aggregated revenue by `PaymentType` and showed that the cleaned data supports categories such as `UPI`, `NETBANKING`, `CASH`, and `CARD`.

<img width="1342" height="500" alt="visualization (1)" src="https://github.com/user-attachments/assets/39051d1d-7d0f-4c2b-8f05-682f73f360af" />


### Store performance
Store-level revenue analysis was created using `StoreLocation`, allowing comparison across cities such as Delhi, Jaipur, Chennai, Kolkata, Hyderabad, Bangalore, Pune, and Mumbai.
<img width="1342" height="500" alt="visualization (2)" src="https://github.com/user-attachments/assets/2970a722-6796-46d8-b587-d60ec6371178" />


### Loyalty-level revenue contribution
Revenue contribution by customer loyalty level was analyzed using the `CustomerLoyaltyLevel` field, which includes levels such as BRONZE, SILVER, GOLD, and PLATINUM.
<img width="1342" height="500" alt="visualization (4)" src="https://github.com/user-attachments/assets/3d86eb4d-484e-4ed8-a2e7-2855f91aff3a" />

### Product-category analysis
Revenue was also grouped by `ProductCategory`, with categories including Home, Apparel, Grocery, and Electronics.
<img width="1342" height="500" alt="visualization (3)" src="https://github.com/user-attachments/assets/adea1a09-1776-42ac-8370-a9bcaa10bd41" />


## Key Results
The silver notebook output shows product-category revenue totals of 168236.96 for Home, 142906.41 for Apparel, 197978.54 for Grocery, and 184341.95 for Electronics.

Revenue by payment method in the visualization output was 187472.64 for UPI, 169487.94 for NETBANKING, 235006.96 for CASH, and 101496.32 for CARD.

Store-location revenue output in the notebook showed 116746.30 for Delhi, 62147.62 for Jaipur, 92493.36 for Chennai, 120687.29 for Kolkata, 30630.84 for Hyderabad, 100496.96 for Bangalore, 113135.06 for Pune, and 57126.43 for Mumbai.

The daily revenue table in the notebook also confirmed that the analysis was created at date level with both revenue and purchase counts, for example 69047.60 revenue and 6 purchases on 2024-05-05, and 49759.00 revenue and 8 purchases on 2024-05-26.

## Difficulties Faced
Several practical issues were encountered during the project:

- **Inconsistent categorical values**: Fields such as payment type, device used, and store region had inconsistent spaces and capitalization in the raw layer.
- **Null values**: Some records had missing values in customer-related columns like age.
- **Timestamp conversion**: `TransactionDate` initially appeared as a string and needed conversion into a proper timestamp/date format for time-based analysis.
- **Layer organization**: A clear bronze-to-silver structure had to be maintained so raw data and cleaned data stayed separate.
- **Visualization readiness**: Raw data was not directly suitable for dashboards until categorical cleanup and schema normalization were complete.

## How the Problems Were Solved
Each issue was handled through a structured transformation approach:

- Text columns were trimmed and standardized to remove leading or trailing spaces and enforce consistent casing.
- Important business columns were validated before analysis, and invalid rows were filtered where required.
- Transaction dates were converted into timestamp/date format so daily aggregations could be computed correctly.
- Cleaned data was written into the silver layer as parquet for efficient downstream SQL queries.
- Temporary SQL views in Databricks simplified visual creation and made business metrics easier to query repeatedly.

## Project Learnings
This project strengthened practical understanding of Azure-based data engineering patterns by connecting ingestion, storage, transformation, and analytics in one workflow.

It also highlighted why data cleaning is essential before dashboarding, especially when business metrics depend on consistent dimensions such as payment method, loyalty segment, and store location.

## Repository Structure Suggestion
A clean GitHub structure for this project can be:

```text
AZURE-RETAIL-DATA-ENGINEERING-PROJECT/
│
├── README.md
├── notebooks/
│   ├── Setup.ipynb
│   ├── Bronze.ipynb
│   └── silver.ipynb
├── 
│   └── 
└── 
```

## README Summary
This repository contains an end-to-end Azure retail analytics project using Azure Data Factory, ADLS Gen2, and Databricks. The pipeline ingests raw retail data from GitHub into the lake, transforms it from bronze to silver, and generates analytical visualizations for daily sales trends, payment behavior, store performance, loyalty contribution, and product-category revenue.
