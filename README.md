# stock_streaming
This project aim to produce stock streaming ETL data pipeline by leveraging Databricks Declarative Automation Bundles, CI/CD and Spark Declarative Pipelines technique.

## Landing job:
* Land data to volume `databricks bundle run yfinance_landing_job -t dev`
<img width="2880" height="1450" alt="image" src="https://github.com/user-attachments/assets/b1eaaf85-2778-4ebd-acf5-e74256ec5f96" />

## Stock streaming pipeline:
* Run pipeline `databricks bundle run stock_streaming_pipeline -t dev`
<img width="2880" height="1468" alt="image" src="https://github.com/user-attachments/assets/80092708-5f60-4bb0-ab17-60b8b3d3ebf8" />
