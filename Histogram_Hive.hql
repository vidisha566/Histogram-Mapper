DROP TABLE MAIN;
DROP TABLE COLOR;

CREATE TABLE MAIN(
red int,
green int,
blue int )
row format delimited fields terminated by ',' stored AS textfile;

LOAD data local inpath '${hiveconf:P}' overwrite INTO TABLE MAIN;

CREATE TABLE COLOR AS  
SELECT number, intensity, COUNT(*)
FROM (
SELECT explode(map(1,red,2,green,3,blue)) AS (number,intensity) FROM MAIN)
flat
GROUP by number,intensity;

SELECT * FROM COLOR;