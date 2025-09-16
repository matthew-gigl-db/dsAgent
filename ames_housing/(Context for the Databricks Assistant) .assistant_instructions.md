Instructions:  Copy all lines below starting with line 3 and incorporate into your personal Databricks Assitant's User Instructions. Update the widget defaults based on your catalog and schema, etc. 

# Formatting 
- Use one tab for indentations in all code and markdown cells.

# Computation
- Always use serverless compute; avoid PySpark RDDs.
- Prefer MapInArrow and pandas UDFs for distributed computation in Spark.
- For plotting:
	- Use Spark Native plotting with `df.plot` if Spark > 4.0.
	- Otherwise, use Plotly in light mode and display plots with `display(...)`.
- Before importing nonstandard Python packages, suggest adding them to the serverless environment and confirm Databricks compatibility.
- Keep all data processing in Spark where possible.

# Data & Project 
- Table: `main.dev_matthew_giglia_ames_housing.sale_prices` is a streaming table created by a pipeline.  
	- Primary key: `property_id`
	- Target variable: `sale_price` (should be numeric)
	- Non-feature column: `_rescued_data`
- Use MLFlow 3 for experiment tracking and model management.
- Start new notebooks with Databricks Widgets for:
	- `catalog_use` with default main
	- `schema_use` with default dev_matthew_giglia_ames_housing
	- `ml_model_name_use` with default dev_matthew_giglia_house_price_prediction
	- `ml_model_storage_location` with default /Volumes/main/dev_matthew_giglia_ames_housing/ml_artifacts
	- `ml_model_experiments_name_use` with default /Workspace/experiments/[dev matthew_giglia] house_price_prediction







