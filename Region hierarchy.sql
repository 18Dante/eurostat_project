--This query generates hierarchy of metropolitan regions extracted from Eurostat


USE default_db --Replace this with your database name
GO

/* This CTE generates a parent - child relationship table. 
The rule here is that region codes with 2 characters are countries and those with more than 2, are metropolitan regions. 
Thanks to that, we can extract parents for all metropolitan regions and assign NUTS_3_METRO label to all countries. */
WITH nuts_metro_regions AS
(
SELECT
	NULL AS parent,
	'NUTS_3_METRO' AS child,
	'NUTS 3 Metropolitan Regions' AS label,
	1 AS level
UNION
SELECT 
	IIF(LEN(mr.region_code) = 2, 'NUTS_3_METRO', LEFT(mr.region_code, 2)) AS parent,
	mr.region_code AS child,
	mr.label,
	IIF(LEN(mr.region_code) = 2, 2, 3) AS level
FROM eurostat_project.metropolitan_regions AS mr -- You need to align the schema and table name with your database
),

-- This is a recursive CTE that also generates a path for the structure.
nuts_structure AS
(
SELECT
n0.parent,
n0.child,
n0.level,
n0.label,
CAST('NUTS_3_METRO' AS nvarchar(MAX)) AS path
FROM nuts_metro_regions AS n0
WHERE n0.parent IS NULL

UNION ALL

SELECT
n.parent,
n.child,
n.level,
n.label,
CAST(CONCAT_WS('|', ns.path, n.label) AS nvarchar(MAX)) AS path
FROM nuts_metro_regions AS n
INNER JOIN nuts_structure AS ns ON ns.child = n.parent

)

SELECT 
parent,
child,
label,
level,
path
FROM nuts_structure
ORDER BY level

