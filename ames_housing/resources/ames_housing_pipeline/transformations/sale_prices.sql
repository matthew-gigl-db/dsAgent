-- Create and populate the target table.

CREATE OR REFRESH STREAMING TABLE sale_prices (
	property_id STRING NOT NULL PRIMARY KEY COMMENT 'Unique identifier for each property, allowing for easy reference and tracking within the dataset.'
	,ms_subclass INT COMMENT 'Indicates the subclass of the property, which helps categorize the type of residential structure.'
	,ms_zoning STRING COMMENT 'Describes the zoning classification of the property, which can affect land use and development potential.'
	,lot_frontage INT COMMENT 'Represents the length of the property that borders the street, which can influence property value and accessibility.'
	,lot_area INT COMMENT 'The total area of the lot in square feet, providing insight into the size of the property.'
	,street_type STRING COMMENT 'Specifies the type of street on which the property is located, which can impact traffic and neighborhood characteristics.'
	,alley_access STRING COMMENT 'Indicates whether the property has access to an alley, which can affect utility access and parking options.'
	,lot_shape STRING COMMENT 'Describes the geometric shape of the lot, which can influence building design and land use.'
	,land_contour STRING COMMENT 'Details the topography of the land, which can affect drainage, landscaping, and construction.'
	,utilities STRING COMMENT 'Lists the available utilities for the property, such as water, electricity, and gas, which are essential for habitation.'
	,lot_configuration STRING COMMENT 'Describes the layout of the lot, which can impact how the property can be developed or used.'
	,land_slope STRING COMMENT 'Indicates the slope of the land, which can affect construction and landscaping decisions.'
	,neighborhood STRING COMMENT 'Identifies the neighborhood in which the property is located, providing context for local amenities and demographics.'
	,condition_primary STRING COMMENT 'Describes the primary condition of the property, which can influence its market value and livability.'
	,condition_secondary STRING COMMENT 'Provides information on any secondary conditions affecting the property, offering a more comprehensive view of its state.'
	,building_type STRING COMMENT 'Specifies the type of building, such as single-family home or multi-family dwelling, which helps categorize the property.'
	,house_style STRING COMMENT 'Describes the architectural style of the house, which can be a factor in buyer preferences and market trends.'
	,overall_quality INT COMMENT 'Rates the overall quality of the property, which can be a key indicator of its value and desirability.'
	,overall_condition INT COMMENT 'Assesses the general condition of the property, providing insight into maintenance and potential repair needs.'
	,year_built INT COMMENT 'Indicates the year the property was constructed, which can affect its historical value and compliance with modern standards.'
	,year_remodeled INT COMMENT 'Specifies the year of any significant remodeling, which can enhance the property\'s appeal and functionality.'
	,roof_style STRING COMMENT 'Describes the style of the roof, which can influence aesthetics and maintenance requirements.'
	,roof_material STRING COMMENT 'Indicates the material used for the roof, which can affect durability and insurance costs.'
	,exterior_primary STRING COMMENT 'Details the primary exterior material of the property, which can impact maintenance and curb appeal.'
	,exterior_secondary STRING COMMENT 'Specifies any secondary exterior materials used, providing additional context for the property\'s appearance.'
	,masonry_veneer_type STRING COMMENT 'Describes the type of masonry veneer, if present, which can enhance the property\'s aesthetic and value.'
	,masonry_veneer_area INT COMMENT 'Indicates the area covered by masonry veneer, which can affect the overall appearance and insulation of the property.'
	,exterior_quality STRING COMMENT 'Rates the quality of the exterior finishes, which can influence buyer perceptions and property value.'
	,exterior_condition STRING COMMENT 'Assesses the condition of the exterior, providing insight into maintenance needs and potential repairs.'
	,foundation_type STRING COMMENT 'Specifies the type of foundation, which can impact structural integrity and suitability for various climates.'
	,basement_quality STRING COMMENT 'Describes the quality of the basement, which can affect its usability and overall property value.'
	,basement_condition STRING COMMENT 'Assesses the condition of the basement, providing insight into potential issues and maintenance needs.'
	,basement_exposure STRING COMMENT 'Indicates the level of exposure of the basement to natural light, which can affect its livability and use.'
	,basement_finish_type_1 STRING COMMENT 'Specifies the type of finish applied to the first basement area, which can influence its functionality and appeal.'
	,basement_finished_sf_1 INT COMMENT 'Indicates the square footage of the finished first basement area, providing insight into usable space.'
	,basement_finish_type_2 STRING COMMENT 'Describes the type of finish applied to the second basement area, if applicable, adding to the property\'s usable space.'
	,basement_finished_sf_2 INT COMMENT 'Indicates the square footage of the finished second basement area, contributing to the overall living space.'
	,basement_unfinished_sf INT COMMENT 'Represents the square footage of the unfinished basement area, which may offer potential for future development.'
	,total_basement_sf INT COMMENT 'Summarizes the total square footage of the basement, combining both finished and unfinished areas for a complete view.'
	,heating_type STRING COMMENT 'Specifies the type of heating system in the property, which can affect comfort and energy efficiency.'
	,heating_quality STRING COMMENT 'Rates the quality of the heating system, providing insight into its effectiveness and reliability.'
	,central_air STRING COMMENT 'Indicates whether the property has central air conditioning, which can enhance comfort during warmer months.'
	,electrical_system STRING COMMENT 'Describes the type of electrical system in place, which is crucial for safety and functionality.'
	,first_floor_sf INT COMMENT 'Indicates the square footage of the first floor, providing insight into the layout and space available.'
	,second_floor_sf INT COMMENT 'Represents the square footage of the second floor, contributing to the overall living area of the property.'
	,low_quality_finished_sf INT COMMENT 'Specifies the square footage of any low-quality finished areas, which may affect overall property value.'
	,above_ground_living_area INT COMMENT 'Summarizes the total square footage of living space above ground, which is a key factor in property valuation.'
	,basement_full_bathrooms INT COMMENT 'Indicates the number of full bathrooms located in the basement, which can enhance the property\'s functionality.'
	,basement_half_bathrooms INT COMMENT 'Specifies the number of half bathrooms in the basement, contributing to the overall bathroom count of the property.'
	,full_bathrooms INT COMMENT 'Represents the total number of full bathrooms in the property, which is an important consideration for potential buyers.'
	,half_bathrooms INT COMMENT 'Counts the number of half bathrooms in the property, which is relevant for assessing its functionality.'
	,bedrooms_above_ground INT COMMENT 'Indicates the number of bedrooms located above ground level, which is important for understanding living space.'
	,kitchens_above_ground INT COMMENT 'Counts the number of kitchens located above ground, relevant for assessing property functionality.'
	,kitchen_quality STRING COMMENT 'Describes the quality of the kitchen, which can significantly impact buyer interest and property value.'
	,total_rooms_above_ground INT COMMENT 'Counts the total number of rooms above ground level, providing insight into the property\'s livable space.'
	,functional_rating STRING COMMENT 'Rates the functionality of the property, which can affect its usability and market appeal.'
	,fireplaces_count INT COMMENT 'Counts the number of fireplaces in the property, which can enhance its appeal and value.'
	,fireplace_quality STRING COMMENT 'Describes the quality of the fireplaces, which can influence buyer interest and property valuation.'
	,garage_type STRING COMMENT 'Indicates the type of garage associated with the property, which is important for assessing parking options.'
	,garage_year_built INT COMMENT 'Specifies the year the garage was built, which can affect its condition and relevance to the property.'
	,garage_finish STRING COMMENT 'Describes the finish of the garage, which can impact its usability and aesthetic appeal.'
	,garage_car_capacity INT COMMENT 'Indicates the number of cars that can be accommodated in the garage, relevant for assessing parking availability.'
	,garage_area INT COMMENT 'Represents the total area of the garage, providing insight into its size and potential use.'
	,garage_quality STRING COMMENT 'Rates the overall quality of the garage, which can influence buyer interest and property value.'
	,garage_condition STRING COMMENT 'Describes the condition of the garage, which is important for assessing maintenance needs.'
	,paved_driveway STRING COMMENT 'Indicates whether the property has a paved driveway, which can enhance accessibility and curb appeal.'
	,wood_deck_sf INT COMMENT 'Represents the square footage of any wood deck on the property, relevant for outdoor living space assessment.'
	,open_porch_sf INT COMMENT 'Indicates the square footage of any open porch, which can enhance the property\'s outdoor appeal.'
	,enclosed_porch_sf INT COMMENT 'Represents the square footage of any enclosed porch, providing additional living space options.'
	,three_season_porch_sf INT COMMENT 'Indicates the square footage of any three-season porch, which can extend the usability of outdoor space.'
	,screen_porch_sf INT COMMENT 'Represents the square footage of any screened porch, relevant for assessing outdoor comfort and usability.'
	,pool_area INT COMMENT 'Indicates the area of any pool on the property, which can significantly impact property value and appeal.'
	,pool_quality STRING COMMENT 'Describes the quality of the pool, which can influence buyer interest and property valuation.'
	,fence_type STRING COMMENT 'Indicates the type of fencing around the property, which can affect privacy and security.'
	,miscellaneous_feature STRING COMMENT 'Lists any additional features of the property that may not fit into standard categories, providing a fuller picture of its offerings.'
	,miscellaneous_value INT COMMENT 'Represents the value associated with miscellaneous features, which can impact overall property valuation.'
	,month_sold INT COMMENT 'Indicates the month in which the property was sold, relevant for analyzing market trends.'
	,year_sold INT COMMENT 'Specifies the year the property was sold, providing context for market conditions at the time of sale.'
	,sale_type STRING COMMENT 'Describes the type of sale, which can influence the property\'s marketability and buyer perception.'
	,sale_condition STRING COMMENT 'Indicates the condition under which the sale occurred, providing insight into market dynamics.'
	,sale_price DECIMAL(10,0) COMMENT 'Represents the final sale price of the property, which is crucial for assessing market value and trends.'
	,_rescued_data STRING COMMENT 'Contains any additional data that has been recovered or salvaged, which may provide further insights into the property.'
	,CONSTRAINT valid_schema EXPECT (_rescued_data is NULL) ON VIOLATION DROP ROW
	,CONSTRAINT valid_property_id EXPECT (property_id IS NOT NULL) ON VIOLATION DROP ROW
	,CONSTRAINT valid_sale_price EXPECT (sale_price IS NOT NULL) ON VIOLATION DROP ROW
	,CONSTRAINT quality_year_built_remodeled_sold EXPECT (year_built <= year_remodeled AND year_remodeled <= year_sold)
	,CONSTRAINT quality_lot_frontage EXPECT (lot_frontage IS NOT NULL AND lot_frontage >= 0)
	,CONSTRAINT quality_lot_area EXPECT (lot_area IS NOT NULL AND lot_area >= 0)
	,CONSTRAINT quality_overall_quality EXPECT (overall_quality IS NOT NULL AND overall_quality >= 1 AND overall_quality <= 10)
	,CONSTRAINT quality_overall_condition EXPECT (overall_condition IS NOT NULL AND overall_condition >= 1 AND overall_condition <= 10)
)
TBLPROPERTIES (
	'delta.enableChangeDataFeed' = 'true'
	,'delta.enableDeletionVectors' = 'true'
	,'delta.enableRowTracking' = 'true'
	,'delta.autoOptimize.optimizeWrite' = 'true'
	,'delta.autoOptimize.autoCompact' = 'true'
	,'quality' = 'silver'
);

CREATE FLOW sales_prices_cdc AS AUTO CDC INTO
	sale_prices
FROM STREAM(bronze_cdf)
KEYS
	(property_id)
APPLY AS DELETE WHEN
	_change_type = "delete"
APPLY AS TRUNCATE WHEN
	_change_type = "truncate"
SEQUENCE BY
	(rcrd_timestamp, _commit_timestamp)
COLUMNS * EXCEPT
	(_change_type, _commit_version, _commit_timestamp, index_file_source_id, file_metadata, rcrd_timestamp, ingest_time, order)
STORED AS
	SCD TYPE 1;


CREATE STREAMING TABLE sale_prices_quarantine (
	CONSTRAINT invalid_schema EXPECT (_rescued_data is not NULL) ON VIOLATION DROP ROW
  ,CONSTRAINT invalid_property_id EXPECT (property_id IS NULL) ON VIOLATION DROP ROW
	,CONSTRAINT invalid_sale_price EXPECT (sale_price IS NULL) ON VIOLATION DROP ROW
)
TBLPROPERTIES (
	'delta.enableChangeDataFeed' = 'true'
	,'delta.enableDeletionVectors' = 'true'
	,'delta.enableRowTracking' = 'true'
	,'delta.autoOptimize.optimizeWrite' = 'true'
	,'delta.autoOptimize.autoCompact' = 'true'
	,'quality' = 'bronze'
) 
AS SELECT * EXCEPT (_change_type, _commit_version, _commit_timestamp) FROM STREAM(bronze_cdf);