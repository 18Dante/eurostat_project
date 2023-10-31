/* This script is generating a dimension table with age groups data. 
The data is extracted from age group code which is provided by Eurostat.
FROM clauses may need to be adjusted according to your database setup */
USE default_db
GO

-- The following CTE is generating unique age group codes from population data table
WITH 
age_groups_initial AS
(
SELECT DISTINCT 
p.age_group
FROM eurostat_project.population AS p
),

/* This CTE is generating a table with age group in a format that is easy to split.
The split also takes place in this CTE. */

age_groups_processed AS
(
SELECT  
p.age_group,
REPLACE(p.age_group, 'Y', '') AS age_range,
a.ordinal,
a.value
FROM age_groups_initial AS p
CROSS APPLY string_split(REPLACE(p.age_group, 'Y', ''), '-', 1) AS a
WHERE p.age_group NOT IN ('UNK', 'Y_GE90', 'Y_LT5')
)

-- Processing the result of split above and generating a final output table 
SELECT DISTINCT
p.age_group,
p.age_range,
age_start.value AS age_range_start,
age_end.value AS age_range_end,
age_start.value AS disp_order
FROM age_groups_processed AS p
INNER JOIN (SELECT value, age_group FROM age_groups_processed WHERE ordinal = 1) AS age_start ON age_start.age_group = p.age_group
INNER JOIN (SELECT value, age_group FROM age_groups_processed WHERE ordinal = 2) AS age_end ON age_end.age_group = p.age_group
UNION
-- Adding data for 3 age group types that were not proccessed before
SELECT  
p.age_group,
REPLACE(p.age_group, 'Y', '') AS age_range,
'age_range_start' = CASE p.age_group
	WHEN 'UNK' THEN NULL
	WHEN 'Y_GE90' THEN 90
	WHEN 'Y_LT5' THEN 0
	END,
'age_range_end' = CASE p.age_group
	WHEN 'UNK' THEN NULL
	WHEN 'Y_GE90' THEN NULL
	WHEN 'Y_LT5' THEN 4
	END,
'disp_order' = CASE p.age_group
	WHEN 'UNK' THEN 200
	WHEN 'Y_GE90' THEN 90
	WHEN 'Y_LT5' THEN 0
	END
FROM age_groups_initial AS p
WHERE p.age_group IN ('UNK', 'Y_GE90', 'Y_LT5')


