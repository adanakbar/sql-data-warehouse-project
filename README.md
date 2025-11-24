# Data Warehouse and Analytics Project

Welcome to the Data Warehouse & Analytics Project! 🚀

This repo features a complete data warehouse solution built to deliver real, actionable insights following modern data engineering best practices.

![Data Architecture](docs/data_architecture.jpg)

The data architecture for this project follows Medallion Architecture Bronze, Silver, and Gold layers:

1. *Bronze Layer:* Stores raw, unprocessed data exactly as received from source systems. Data is ingested from CSV files into the SQL Server database.
2. *Silver Layer:* Applies data cleansing, standardization, and normalization to prepare data for analysis.
3. *Gold Layer:* Contains business-ready data modeled in a star schema, optimized for reporting and analytics.

