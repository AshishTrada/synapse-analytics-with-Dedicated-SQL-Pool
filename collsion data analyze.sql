IF NOT EXISTS (SELECT * FROM sys.objects O JOIN sys.schemas S ON O.schema_id = S.schema_id WHERE O.NAME = 'collisionData' AND O.TYPE = 'U' 
            AND S.NAME = 'dbo')
CREATE TABLE dbo.collisionData
	(
		crash_date DATE,
		on_street_name NVARCHAR(255),
		number_of_persons_injured INT,
		number_of_persons_killed INT,
		number_of_pedestrians_injured INT,
		number_of_pedestrians_killed INT,
		number_of_cyclist_injured INT,
		number_of_cyclist_killed INT,
		number_of_motorist_injured INT,
		number_of_motorist_killed INT,
		contributing_factor_vehicle_1 NVARCHAR(255),
		contributing_factor_vehicle_2 NVARCHAR(255),
		collision_id BIGINT,
		vehicle_type_code1 NVARCHAR(255)
	)
WITH
	(
	DISTRIBUTION = ROUND_ROBIN,
	 CLUSTERED COLUMNSTORE INDEX
	 -- HEAP
	)
GO

-- copy the data
COPY INTO dbo.collisionData
(crash_date 1, on_street_name 2, number_of_persons_injured 3, number_of_persons_killed 4, number_of_pedestrians_injured 5, number_of_pedestrians_killed 6, number_of_cyclist_injured 7, number_of_cyclist_killed 8, number_of_motorist_injured 9, number_of_motorist_killed 10, contributing_factor_vehicle_1 11, contributing_factor_vehicle_2 12, collision_id 13, vehicle_type_code1 14)
FROM 'https://synapsedatasetadls.dfs.core.windows.net/project-data/vehicle-collision-data/nyc_collision_data.csv'
WITH
(
	FILE_TYPE = 'CSV'
	,MAXERRORS = 0
	,FIRSTROW = 02
);

SELECT TOP 100 * FROM dbo.collisionData;

-- Transformation query
SELECT
    crash_date,
    UPPER(on_street_name) AS on_street_name,
    COALESCE(number_of_persons_injured, 0) AS number_of_persons_injured,
    COALESCE(number_of_persons_killed, 0) AS number_of_persons_killed,
    COALESCE(number_of_pedestrians_injured, 0) AS number_of_pedestrians_injured,
    COALESCE(number_of_pedestrians_killed, 0) AS number_of_pedestrians_killed,
    COALESCE(number_of_cyclist_injured, 0) AS number_of_cyclist_injured,
    COALESCE(number_of_cyclist_killed, 0) AS number_of_cyclist_killed,
    COALESCE(number_of_motorist_injured, 0) AS number_of_motorist_injured,
    COALESCE(number_of_motorist_killed, 0) AS number_of_motorist_killed,
    contributing_factor_vehicle_1,
    contributing_factor_vehicle_2,
    collision_id,
    vehicle_type_code1
FROM dbo.collisionData;

-- Create a view for yearly collision summary
CREATE VIEW YearlyCollisionSummary AS
SELECT
    YEAR(crash_date) AS crash_year,
    COUNT(*) AS total_collisions,
    SUM(number_of_persons_injured) AS total_injuries,
    SUM(number_of_persons_killed) AS total_deaths
FROM dbo.collisionData
GROUP BY YEAR(crash_date);

select * from YearlyCollisionSummary;

-- Additional queries for analysis

-- Query 1: Top 5 streets with the most collisions
SELECT TOP 5
    on_street_name,
    COUNT(*) AS collision_count
FROM dbo.collisionData
GROUP BY on_street_name
ORDER BY collision_count DESC;

-- Query 2: Total injuries and deaths per vehicle type
SELECT
    vehicle_type_code1,
    SUM(number_of_persons_injured) AS total_injuries,
    SUM(number_of_persons_killed) AS total_deaths
FROM dbo.collisionData
GROUP BY vehicle_type_code1
ORDER BY total_injuries DESC;

-- Query 3: Monthly collision trend
SELECT
    YEAR(crash_date) AS crash_year,
    MONTH(crash_date) AS crash_month,
    COUNT(*) AS total_collisions
FROM dbo.collisionData
GROUP BY YEAR(crash_date), MONTH(crash_date)
ORDER BY crash_year, crash_month;

-- Query 4: Contribution factors causing the most injuries
SELECT TOP 5
    contributing_factor_vehicle_1,
    SUM(number_of_persons_injured) AS total_injuries
FROM dbo.collisionData
GROUP BY contributing_factor_vehicle_1
ORDER BY total_injuries DESC;

-- Query 5: Collisions involving pedestrians
SELECT
    crash_date,
    on_street_name,
    number_of_pedestrians_injured,
    number_of_pedestrians_killed
FROM dbo.collisionData
WHERE number_of_pedestrians_injured > 0 OR number_of_pedestrians_killed > 0
ORDER BY crash_date DESC;

-- Query 6: Yearly collisions by severity (injuries and deaths)
SELECT
    YEAR(crash_date) AS crash_year,
    SUM(CASE WHEN number_of_persons_injured > 0 THEN 1 ELSE 0 END) AS collisions_with_injuries,
    SUM(CASE WHEN number_of_persons_killed > 0 THEN 1 ELSE 0 END) AS collisions_with_deaths
FROM dbo.collisionData
GROUP BY YEAR(crash_date)
ORDER BY crash_year;
