DROP DATABASE IF EXISTS US_Honeybee_Colonies;
CREATE DATABASE US_Honeybee_Colonies;
USE US_Honeybee_Colonies;
DROP TABLE IF EXISTS bee_colonies_county_src;
DROP TABLE IF EXISTS bee_colonies_state_src;
DROP TABLE IF EXISTS population_src;
DROP TABLE IF EXISTS Bee_Colonies;

CREATE TABLE `bee_colonies_county_src` (
  `Year` text,
  `Geo_Level` text,
  `State` text,
  `State_ANSI` text,
  `Ag_District` text,
  `Ag_District_Code` text,
  `County` text,
  `County_ANSI` text,
  `Value` text
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SET GLOBAL local_infile=1;

LOAD DATA LOCAL INFILE
'/Users/evanadinolfe/Downloads/individual_project/bee_colonies_county_src.csv'
INTO TABLE bee_colonies_county_src
CHARACTER SET 'latin1'
COLUMNS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;

CREATE TABLE `bee_colonies_state_src` (
  `Year` text,
  `Geo_Level` text,
  `State` text,
  `State_ANSI` text,
  `Ag_District` text,
  `Ag_District_Code` text,
  `County` text,
  `County_ANSI` text,
  `Value` text
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SET GLOBAL local_infile=1;

LOAD DATA LOCAL INFILE
'/Users/evanadinolfe/Downloads/individual_project/bee_colonies_state_src.csv'
INTO TABLE bee_colonies_state_src
CHARACTER SET 'latin1'
COLUMNS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;

CREATE TABLE `population_src` (
  `FIPStxt` text,
  `State` text,
  `Area_name` text,
  `Rural_urban_code_2013` text,
  `Population_1990` text,
  `Population_2000` text,
  `Population_2010` text,
  `Population_2020` text
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SET GLOBAL local_infile=1;

LOAD DATA LOCAL INFILE
'/Users/evanadinolfe/Downloads/individual_project/population_src.csv'
INTO TABLE population_src
CHARACTER SET 'latin1'
COLUMNS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;

ALTER TABLE population_src
ADD State_ANSI TINYINT UNSIGNED, ADD County_ANSI SMALLINT;

UPDATE population_src
SET State_ANSI = FLOOR(FIPStxt/1000);

UPDATE population_src
SET County_ANSI = FIPStxt % 1000;

UPDATE bee_colonies_county_src
SET Value = NULL
WHERE Value LIKE '%(D)%';

ALTER TABLE population_src
ADD Geo_Level VARCHAR(7);

UPDATE population_src
SET Geo_Level = 'Country'
WHERE State_ANSI = 0 AND County_ANSI = 0;

UPDATE population_src
SET Geo_Level = 'State'
WHERE State_ANSI > 0 AND County_ANSI = 0;

UPDATE population_src
SET Geo_Level = 'County'
WHERE State_ANSI > 0 AND County_ANSI > 0;

UPDATE population_src
SET Rural_urban_code_2013 = NULL
WHERE Rural_urban_code_2013 = '';

UPDATE population_src
SET Population_1990 = NULL
WHERE Population_1990 = '';

UPDATE population_src
SET Population_2000 = NULL
WHERE Population_2000 = '';

UPDATE population_src
SET Population_2010 = NULL
WHERE Population_2010 = '';

UPDATE population_src
SET Population_2020 = NULL
WHERE Population_2020 = '';

UPDATE bee_colonies_county_src
SET County_ANSI = (
SELECT County_ANSI 
FROM population_src 
WHERE Area_Name LIKE '%aleutians east%') 
WHERE County_ANSI = '' AND State = 'Alaska' AND County LIKE '%Aleutian%';

UPDATE bee_colonies_county_src
SET County_ANSI = (
SELECT County_ANSI 
FROM population_src 
WHERE Area_Name LIKE '%kenai%') 
WHERE County_ANSI = '' AND State = 'Alaska' AND County LIKE '%Kenai%';

UPDATE bee_colonies_county_src
SET County_ANSI = (
SELECT County_ANSI
FROM population_src
WHERE Area_Name LIKE '%anchorage municipality%') 
WHERE County_ANSI = '' AND State = 'Alaska' AND County LIKE '%anchorage%';

UPDATE bee_colonies_county_src
SET County_ANSI = (
SELECT County_ANSI
FROM population_src
WHERE Area_Name LIKE '%fairbanks north%')
WHERE County_ANSI = '' AND State = 'Alaska' AND County LIKE '%fairbanks%';

UPDATE bee_colonies_state_src
SET County_ANSI = 0, Ag_District_Code = NULL;

SELECT * FROM bee_colonies_county_src;

SELECT * FROM population_src;

CREATE TABLE Geo_Codes (
Geo_Level VARCHAR(7),
State_ANSI INT,
County_ANSI INT,
State VARCHAR(200),
Area_Name VARCHAR(43),
PRIMARY KEY (State_ANSI, County_ANSI)
);

INSERT INTO Geo_Codes (Geo_Level, State_ANSI, County_ANSI, State, Area_Name)
SELECT DISTINCT Geo_Level, State_ANSI, County_ANSI, State, Area_Name FROM population_src;

CREATE TABLE Ag_Codes (
State_ANSI INT,
Ag_District_Code INT,
Ag_District VARCHAR(28),
PRIMARY KEY (State_ANSI, Ag_District_Code)
);

INSERT INTO Ag_Codes (State_ANSI, Ag_District_Code, Ag_District)
SELECT DISTINCT State_ANSI, Ag_District_Code, Ag_District FROM bee_colonies_county_src;

CREATE TABLE Bee_Colonies (
State_ANSI INT,
County_ANSI INT,
Ag_District_Code INT,
Colonies_2002 INT,
Colonies_2007 INT,
Colonies_2012 INT,
Colonies_2017 INT,
Colonies_2022 INT,
PRIMARY KEY (State_ANSI, County_ANSI)
);

INSERT INTO Bee_Colonies
SELECT State_ANSI, County_ANSI, Ag_District_Code, 
SUM(IF(year=2002, NULLIF(REPLACE(value,',',''),''), NULL)),
SUM(IF(year=2007, NULLIF(REPLACE(value,',',''),''), NULL)),
SUM(IF(year=2012, NULLIF(REPLACE(value,',',''),''), NULL)),
SUM(IF(year=2017, NULLIF(REPLACE(value,',',''),''), NULL)),
SUM(IF(year=2022, NULLIF(REPLACE(value,',',''),''), NULL))
FROM bee_colonies_county_src
GROUP BY State_ANSI, County_ANSI, Ag_District_Code;

INSERT INTO Bee_Colonies
SELECT State_ANSI, County_ANSI, Ag_District_Code, 
SUM(IF(year=2002, NULLIF(REPLACE(value,',',''),''), NULL)),
SUM(IF(year=2007, NULLIF(REPLACE(value,',',''),''), NULL)),
SUM(IF(year=2012, NULLIF(REPLACE(value,',',''),''), NULL)),
SUM(IF(year=2017, NULLIF(REPLACE(value,',',''),''), NULL)),
SUM(IF(year=2022, NULLIF(REPLACE(value,',',''),''), NULL))
FROM bee_colonies_state_src
GROUP BY State_ANSI, County_ANSI, Ag_District_Code;


CREATE TABLE Population (
State_ANSI INT,
County_ANSI INT,
Rural_Urban_Code_2013 INT,
Population_1990 INT,
Population_2000 INT,
Population_2010 INT,
Population_2020 INT,
PRIMARY KEY (State_ANSI, County_ANSI)
);

INSERT INTO Population (State_ANSI, County_ANSI, Rural_Urban_Code_2013, Population_1990, Population_2000, Population_2010, Population_2020)
SELECT DISTINCT State_ANSI, County_ANSI, Rural_Urban_Code_2013, NULLIF(REPLACE(Population_1990,',',''),''), NULLIF(REPLACE(Population_2000,',',''),''), NULLIF(REPLACE(Population_2010,',',''),''), NULLIF(REPLACE(Population_2020,',',''),'')
FROM population_src;

ALTER TABLE Bee_Colonies
ADD FOREIGN KEY (State_ANSI, Ag_District_Code) REFERENCES Ag_Codes(State_ANSI, Ag_District_Code);

ALTER TABLE Population
ADD FOREIGN KEY (State_ANSI, County_ANSI) REFERENCES Geo_Codes(State_ANSI, County_ANSI);

#Question 2
SELECT Geo_Level, Geo_Codes.State_ANSI, State, Geo_Codes.County_ANSI, Area_Name, Colonies_2002, 
Colonies_2007, Colonies_2012, Colonies_2017, Colonies_2022
FROM Bee_Colonies
JOIN Geo_Codes ON Geo_Codes.State_ANSI = Bee_Colonies.State_ANSI AND Geo_Codes.County_ANSI = Bee_Colonies.County_ANSI
WHERE State = 'NJ'
GROUP BY Geo_Level, Geo_Codes.State_ANSI, State, Geo_Codes.County_ANSI, Area_Name, Colonies_2002, Colonies_2007, 
Colonies_2012, Colonies_2017, Colonies_2022
ORDER BY Colonies_2022 DESC;

#Question 3
SELECT (COUNT(State_ANSI)-1) AS Number_of_Counties
FROM Geo_Codes
WHERE State = (
SELECT State 
FROM geo_codes
WHERE area_name = 'New York' AND geo_level = 'state');

#Question 4
SELECT Bee_Colonies.State_ANSI, Bee_Colonies.County_ANSI, Area_Name AS County_Name, 
FORMAT(Population.Population_2020, 0) AS Population_2020, Bee_Colonies.Colonies_2022
FROM Geo_Codes 
LEFT JOIN Bee_Colonies ON Geo_Codes.State_ANSI = Bee_Colonies.State_ANSI AND Geo_Codes.County_ANSI = Bee_Colonies.County_ANSI
LEFT JOIN Population ON Population.State_ANSI = Bee_Colonies.State_ANSI AND Population.County_ANSI = Bee_Colonies.County_ANSI
WHERE State = (
SELECT State
FROM geo_codes
WHERE area_name = 'New York' AND geo_level = 'state')
ORDER BY Population.Population_2020 DESC;

#Question 5
SELECT Geo_Level, Bee_Colonies.State_ANSI, State, Bee_Colonies.County_ANSI, Area_Name, Colonies_2002, 
Colonies_2007, Colonies_2012, Colonies_2017, Colonies_2022 
FROM Bee_Colonies
JOIN Geo_Codes ON Bee_Colonies.State_ANSI = Geo_Codes.State_ANSI AND Bee_Colonies.County_ANSI = Geo_Codes.County_ANSI
WHERE State = (
SELECT State
FROM geo_codes
WHERE area_name = 'Alaska' and Geo_level = 'state') AND geo_level = 'county' AND Colonies_2022 = (
SELECT MAX(Colonies_2022)
FROM Bee_Colonies
JOIN Geo_Codes ON Bee_Colonies.State_ANSI = Geo_Codes.State_ANSI AND Bee_Colonies.County_ANSI = Geo_Codes.County_ANSI
WHERE State = (
SELECT State
FROM geo_codes
WHERE area_name = 'Alaska' and Geo_level = 'state') AND geo_level = 'county'
);

#Question 6
SELECT geo_codes.state_ansi, state AS State, COUNT(Ag_District) AS num_ag_districts
FROM Geo_Codes
JOIN Bee_Colonies ON Geo_Codes.state_ansi = Bee_Colonies.state_ansi AND Geo_Codes.county_ansi = Bee_Colonies.county_ansi
JOIN Ag_Codes ON Ag_Codes.state_ansi = Bee_Colonies.state_ansi AND Ag_Codes.ag_district_code = Bee_Colonies.ag_district_code
GROUP BY state_ansi, state
ORDER BY state ASC;

#Question 7
DROP TABLE IF EXISTS Q7;
CREATE TABLE Q7 AS
SELECT Bee_Colonies.State_ANSI, Area_Name AS State,
Ag_Codes.Ag_District_Code, Ag_District, 
SUM(Colonies_2002) AS Colonies_2002, SUM(Colonies_2007) AS Colonies_2007, SUM(Colonies_2012) AS Colonies_2012, SUM(Colonies_2017) AS Colonies_2017, SUM(Colonies_2022) AS Colonies_2022
FROM Ag_Codes
JOIN Bee_Colonies ON Bee_Colonies.State_ANSI = Ag_Codes.State_ANSI AND Bee_Colonies.Ag_District_Code = Ag_Codes.Ag_District_Code
JOIN Geo_Codes ON Bee_Colonies.State_ANSI = Geo_Codes.State_ANSI AND Geo_Level = 'State'
GROUP BY Bee_Colonies.State_ANSI, Area_Name, Ag_District_Code, Ag_District;

SELECT * FROM Q7
WHERE Colonies_2022 = (
SELECT MAX(Colonies_2022)
FROM Q7);

#Question 8
SELECT State, Area_name AS State_Name, 
FORMAT(Colonies_2002, 0), FORMAT(Colonies_2007, 0), FORMAT(Colonies_2012, 0), 
FORMAT(Colonies_2017, 0), FORMAT(Colonies_2022, 0), 
CONCAT(ROUND((Colonies_2022 - Colonies_2002) / Colonies_2002 * 100, 2), '%') AS Percent_Change
FROM Bee_Colonies
JOIN Geo_Codes ON Bee_Colonies.State_ANSI = Geo_Codes.State_ANSI AND Bee_Colonies.County_ANSI = Geo_Codes.County_ANSI
WHERE Geo_Level = 'state' AND ((Colonies_2022 - Colonies_2002) / Colonies_2002 * 100 = (
SELECT MAX(PChange)
FROM (
SELECT (Colonies_2022 - Colonies_2002) / Colonies_2002 * 100 AS PChange
FROM Bee_Colonies
JOIN Geo_Codes ON Bee_Colonies.State_ANSI = Geo_Codes.State_ANSI AND Bee_Colonies.County_ANSI = Geo_Codes.County_ANSI
WHERE Geo_Level = 'state') AS B) OR
(Colonies_2022 - Colonies_2002) / Colonies_2002 * 100 = (
SELECT MIN(PChange)
FROM (
SELECT (Colonies_2022 - Colonies_2002) / Colonies_2002 * 100 AS PChange
FROM Bee_Colonies
JOIN Geo_Codes ON Bee_Colonies.State_ANSI = Geo_Codes.State_ANSI AND Bee_Colonies.County_ANSI = Geo_Codes.County_ANSI
WHERE Geo_Level = 'state') AS B));

#Question 9
SELECT State, Area_Name AS State_Name, FORMAT(Population_1990, 0), FORMAT(Population_2000, 0), FORMAT(Population_2010, 0), 
FORMAT(Population_2020, 0), FORMAT(Population_2020 - Population_1990, 0) AS Amount_of_Change
FROM Geo_Codes
JOIN Population ON Population.State_ANSI = Geo_Codes.State_ANSI AND Population.County_ANSI = Geo_Codes.County_ANSI
WHERE Geo_Level = 'state' AND State IN (
SELECT State
FROM Bee_Colonies
JOIN Geo_Codes ON Bee_Colonies.State_ANSI = Geo_Codes.State_ANSI AND Bee_Colonies.County_ANSI = Geo_Codes.County_ANSI
WHERE Geo_Level = 'state' AND ((Colonies_2022 - Colonies_2002) / Colonies_2002 * 100 = (
SELECT MAX(PChange)
FROM (
SELECT (Colonies_2022 - Colonies_2002) / Colonies_2002 * 100 AS PChange
FROM Bee_Colonies
JOIN Geo_Codes ON Bee_Colonies.State_ANSI = Geo_Codes.State_ANSI AND Bee_Colonies.County_ANSI = Geo_Codes.County_ANSI
WHERE Geo_Level = 'state') AS B) OR
(Colonies_2022 - Colonies_2002) / Colonies_2002 * 100 = (
SELECT MIN(PChange)
FROM (
SELECT (Colonies_2022 - Colonies_2002) / Colonies_2002 * 100 AS PChange
FROM Bee_Colonies
JOIN Geo_Codes ON Bee_Colonies.State_ANSI = Geo_Codes.State_ANSI AND Bee_Colonies.County_ANSI = Geo_Codes.County_ANSI
WHERE Geo_Level = 'state') AS B)));





