DROP TABLE Graph1;
DROP TABLE Graph2;
DROP TABLE Graph3;
DROP TABLE Graph4;

CREATE TABLE Graph1(
node int,
neighbour_node int )
row format delimited fields terminated by ',' stored AS textfile;

LOAD data local inpath '${hiveconf:G}' INTO TABLE Graph1;

CREATE TABLE Graph2 AS
SELECT node, COUNT( neighbour_node) AS neighbour_node_count FROM Graph1
GROUP BY node;

CREATE TABLE Graph3 AS
SELECT neighbour_node_count, node FROM Graph2;

CREATE TABLE Graph4 AS
SELECT neighbour_node_count, COUNT( node ) FROM Graph3
GROUP BY neighbour_node_count;

SELECT * FROM Graph4;
