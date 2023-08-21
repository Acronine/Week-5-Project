LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/dota_2_matches.csv'
INTO TABLE dota_2_matches
FIELDS TERMINATED BY ','
IGNORE 1 ROWS;