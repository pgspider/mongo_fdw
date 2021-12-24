-- Before running this file User must create database mongo_fdw_regress and
-- mongo_fdw_regress1 databases on MongoDB with all permission for
-- MONGO_USER_NAME user with MONGO_PASS password and ran mongodb_init.sh file
-- to load collections.
\set ECHO none
\ir sql/parameters.conf
\set ECHO all
\c contrib_regression
--Testcase 1:
CREATE EXTENSION IF NOT EXISTS mongo_fdw;
--Testcase 2:
CREATE SERVER mongo_server FOREIGN DATA WRAPPER mongo_fdw
  OPTIONS (address :MONGO_HOST, port :MONGO_PORT);
--Testcase 3:
CREATE USER MAPPING FOR public SERVER mongo_server;

-- Check version
--Testcase 4:
SELECT mongo_fdw_version();

-- Create foreign tables
--Testcase 5:
CREATE FOREIGN TABLE f_mongo_test (_id name, a int, b text)
  SERVER mongo_server OPTIONS (database 'mongo_fdw_regress', collection 'mongo_test');
--Testcase 6:
CREATE FOREIGN TABLE f_test_tbl1 (_id NAME, c1 INTEGER, c2 VARCHAR(10), c3 CHAR(9),c4 INTEGER, c5 pg_catalog.Date, c6 DECIMAL, c7 INTEGER, c8 INTEGER)
  SERVER mongo_server OPTIONS (database 'mongo_fdw_regress', collection 'test_tbl1');
--Testcase 7:
CREATE FOREIGN TABLE f_test_tbl2 (_id NAME, c1 INTEGER, c2 VARCHAR(14), c3 VARCHAR(13))
  SERVER mongo_server OPTIONS (database 'mongo_fdw_regress', collection 'test_tbl2');
--Testcase 8:
CREATE FOREIGN TABLE countries (_id NAME, name VARCHAR, population INTEGER, capital VARCHAR, hdi FLOAT)
  SERVER mongo_server OPTIONS (database 'mongo_fdw_regress', collection 'countries');
--Testcase 9:
CREATE FOREIGN TABLE country_elections (_id NAME, "lastElections.type" VARCHAR, "lastElections.date" pg_catalog.TIMESTAMP)
  SERVER mongo_server OPTIONS (database 'mongo_fdw_regress', collection 'countries');
--Testcase 10:
CREATE FOREIGN TABLE main_exports (_id NAME, "mainExports" TEXT[] )
  SERVER mongo_server OPTIONS (database 'mongo_fdw_regress', collection 'countries');
--Testcase 11:
CREATE FOREIGN TABLE test_json ( __doc json)
  SERVER mongo_server OPTIONS (database 'mongo_fdw_regress', collection 'warehouse');
--Testcase 12:
CREATE FOREIGN TABLE test_jsonb ( __doc jsonb)
  SERVER mongo_server OPTIONS (database 'mongo_fdw_regress', collection 'warehouse');
--Testcase 13:
CREATE FOREIGN TABLE test_text ( __doc text)
  SERVER mongo_server OPTIONS (database 'mongo_fdw_regress', collection 'warehouse');
--Testcase 14:
CREATE FOREIGN TABLE test_varchar ( __doc varchar)
  SERVER mongo_server OPTIONS (database 'mongo_fdw_regress', collection 'warehouse');

--Testcase 15:
SET datestyle TO ISO;

-- Retrieve data from foreign table using SELECT statement.
--Testcase 16:
SELECT c1, c2, c3, c4, c5, c6, c7, c8 FROM f_test_tbl1
  ORDER BY c1 DESC, c8;
--Testcase 17:
SELECT DISTINCT c8 FROM f_test_tbl1 ORDER BY 1;
--Testcase 18:
SELECT c2 AS "Employee Name" FROM f_test_tbl1 ORDER BY c2 COLLATE "C";
--Testcase 19:
SELECT c8, c6, c7 FROM f_test_tbl1 ORDER BY 1, 2, 3;
--Testcase 20:
SELECT c1, c2, c3, c4, c5, c6, c7, c8 FROM f_test_tbl1
  WHERE c1 = 100 ORDER BY 1;
--Testcase 21:
SELECT c1, c2, c3, c4, c5, c6, c7, c8 FROM f_test_tbl1
  WHERE c1 = 100 OR c1 = 700 ORDER BY 1;
--Testcase 22:
SELECT c1, c2, c3 FROM f_test_tbl1 WHERE c3 like 'SALESMAN' ORDER BY 1;
--Testcase 23:
SELECT c1, c2, c3 FROM f_test_tbl1 WHERE c1 IN (100, 700) ORDER BY 1;
--Testcase 24:
SELECT c1, c2, c3 FROM f_test_tbl1 WHERE c1 NOT IN (100, 700) ORDER BY 1 LIMIT 5;
--Testcase 25:
SELECT c1, c2, c8 FROM f_test_tbl1 WHERE c8 BETWEEN 10 AND 20 ORDER BY 1;
--Testcase 26:
SELECT c1, c2, c6 FROM f_test_tbl1 ORDER BY 1 OFFSET 5;

-- Retrieve data from foreign table using group by clause.
--Testcase 27:
SELECT c8 "Department", COUNT(c1) "Total Employees" FROM f_test_tbl1
  GROUP BY c8 ORDER BY c8;
--Testcase 28:
SELECT c8, SUM(c6) FROM f_test_tbl1
  GROUP BY c8 HAVING c8 IN (10, 30) ORDER BY c8;
--Testcase 29:
SELECT c8, SUM(c6) FROM f_test_tbl1
  GROUP BY c8 HAVING SUM(c6) > 9400 ORDER BY c8;

-- Retrieve data from foreign table using sub-queries.
--Testcase 30:
SELECT c1, c2, c6 FROM f_test_tbl1
  WHERE c8 <> ALL (SELECT c1 FROM f_test_tbl2 WHERE c1 IN (10, 30, 40))
  ORDER BY c1;
--Testcase 31:
SELECT c1, c2, c3 FROM f_test_tbl2
  WHERE EXISTS (SELECT 1 FROM f_test_tbl1 WHERE f_test_tbl2.c1 = f_test_tbl1.c8)
  ORDER BY 1, 2;
--Testcase 32:
SELECT c1, c2, c3, c4, c5, c6, c7, c8 FROM f_test_tbl1
  WHERE c8 NOT IN (SELECT c1 FROM f_test_tbl2) ORDER BY c1;

-- Retrieve data from foreign table using UNION operator.
--Testcase 33:
SELECT c1, c2 FROM f_test_tbl2 UNION
SELECT c1, c2 FROM f_test_tbl1 ORDER BY c1;

--Testcase 34:
SELECT c1, c2 FROM f_test_tbl2 UNION ALL
SELECT c1, c2 FROM f_test_tbl1 ORDER BY c1;

-- Retrieve data from foreign table using INTERSECT operator.
--Testcase 35:
SELECT c1, c2 FROM f_test_tbl1 WHERE c1 >= 800 INTERSECT
SELECT c1, c2 FROM f_test_tbl1 WHERE c1 >= 400 ORDER BY c1;

--Testcase 36:
SELECT c1, c2 FROM f_test_tbl1 WHERE c1 >= 800 INTERSECT ALL
SELECT c1, c2 FROM f_test_tbl1 WHERE c1 >= 400 ORDER BY c1;

-- Retrieve data from foreign table using EXCEPT operator.
--Testcase 37:
SELECT c1, c2 FROM f_test_tbl1 EXCEPT
SELECT c1, c2 FROM f_test_tbl1 WHERE c1 > 900 ORDER BY c1;

--Testcase 38:
SELECT c1, c2 FROM f_test_tbl1 EXCEPT ALL
SELECT c1, c2 FROM f_test_tbl1 WHERE c1 > 900 ORDER BY c1;

-- Retrieve data from foreign table using CTE (with clause).
--Testcase 39:
WITH
  with_qry AS (SELECT c1, c2, c3 FROM f_test_tbl2)
SELECT e.c2, e.c6, w.c1, w.c2 FROM f_test_tbl1 e, with_qry w
  WHERE e.c8 = w.c1 ORDER BY e.c8, e.c2 COLLATE "C";

--Testcase 40:
WITH
  test_tbl2_costs AS (SELECT d.c2, SUM(c6) test_tbl2_total FROM f_test_tbl1 e, f_test_tbl2 d
    WHERE e.c8 = d.c1 GROUP BY 1),
  avg_cost AS (SELECT SUM(test_tbl2_total)/COUNT(*) avg FROM test_tbl2_costs)
SELECT * FROM test_tbl2_costs
  WHERE test_tbl2_total > (SELECT avg FROM avg_cost) ORDER BY c2 COLLATE "C";

-- Retrieve data from foreign table using window clause.
--Testcase 41:
SELECT c8, c1, c6, AVG(c6) OVER (PARTITION BY c8) FROM f_test_tbl1
  ORDER BY c8, c1;
--Testcase 42:
SELECT c8, c1, c6, COUNT(c6) OVER (PARTITION BY c8) FROM f_test_tbl1
  WHERE c8 IN (10, 30, 40, 50, 60, 70) ORDER BY c8, c1;
--Testcase 43:
SELECT c8, c1, c6, SUM(c6) OVER (PARTITION BY c8) FROM f_test_tbl1
  ORDER BY c8, c1;

-- Views
--Testcase 44:
CREATE VIEW smpl_vw AS
  SELECT c1, c2, c3, c4, c5, c6, c7, c8 FROM f_test_tbl1 ORDER BY c1;
--Testcase 45:
SELECT * FROM smpl_vw ORDER BY 1;

--Testcase 46:
CREATE VIEW comp_vw (s1, s2, s3, s6, s7, s8, d2) AS
  SELECT s.c1, s.c2, s.c3, s.c6, s.c7, s.c8, d.c2
    FROM f_test_tbl2 d, f_test_tbl1 s WHERE d.c1 = s.c8 AND d.c1 = 10
    ORDER BY s.c1;
--Testcase 47:
SELECT * FROM comp_vw ORDER BY 1;

--Testcase 48:
CREATE TEMPORARY VIEW temp_vw AS
  SELECT c1, c2, c3 FROM f_test_tbl2;
--Testcase 49:
SELECT * FROM temp_vw ORDER BY 1, 2;

--Testcase 50:
CREATE VIEW mul_tbl_view AS
  SELECT d.c1 dc1, d.c2 dc2, e.c1 ec1, e.c2 ec2, e.c6 ec6
    FROM f_test_tbl2 d INNER JOIN f_test_tbl1 e ON d.c1 = e.c8 ORDER BY d.c1;
--Testcase 51:
SELECT * FROM mul_tbl_view ORDER BY 1, 2, 3;

-- Foreign-Foreign table joins

-- CROSS JOIN.
--Testcase 52:
SELECT f_test_tbl2.c2, f_test_tbl1.c2
  FROM f_test_tbl2 CROSS JOIN f_test_tbl1 ORDER BY 1, 2;
-- INNER JOIN.
--Testcase 53:
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d, f_test_tbl1 e WHERE d.c1 = e.c8 ORDER BY 1, 3;
--Testcase 54:
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d INNER JOIN f_test_tbl1 e ON d.c1 = e.c8 ORDER BY 1, 3;
-- OUTER JOINS.
--Testcase 55:
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d LEFT OUTER JOIN f_test_tbl1 e ON d.c1 = e.c8 ORDER BY 1, 3;
--Testcase 56:
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d RIGHT OUTER JOIN f_test_tbl1 e ON d.c1 = e.c8 ORDER BY 1, 3;
--Testcase 57:
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d FULL OUTER JOIN f_test_tbl1 e ON d.c1 = e.c8 ORDER BY 1, 3;

-- Local-Foreign table joins.
--Testcase 58:
CREATE TABLE l_test_tbl1 AS
  SELECT c1, c2, c3, c4, c5, c6, c7, c8 FROM f_test_tbl1;
--Testcase 59:
CREATE TABLE l_test_tbl2 AS
  SELECT c1, c2, c3 FROM f_test_tbl2;

-- CROSS JOIN.
--Testcase 60:
SELECT f_test_tbl2.c2, l_test_tbl1.c2 FROM f_test_tbl2 CROSS JOIN l_test_tbl1 ORDER BY 1, 2;
-- INNER JOIN.
--Testcase 61:
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM l_test_tbl2 d, f_test_tbl1 e WHERE d.c1 = e.c8 ORDER BY 1, 3;
--Testcase 62:
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d INNER JOIN l_test_tbl1 e ON d.c1 = e.c8 ORDER BY 1, 3;
-- OUTER JOINS.
--Testcase 63:
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d LEFT OUTER JOIN l_test_tbl1 e ON d.c1 = e.c8 ORDER BY 1, 3;
--Testcase 64:
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d RIGHT OUTER JOIN l_test_tbl1 e ON d.c1 = e.c8 ORDER BY 1, 3;
--Testcase 65:
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d FULL OUTER JOIN l_test_tbl1 e ON d.c1 = e.c8 ORDER BY 1, 3;

-- Retrieve complex data containing Sub-fields, dates, Arrays
--Testcase 66:
SELECT * FROM countries ORDER BY _id;
--Testcase 67:
SELECT * FROM country_elections ORDER BY _id;
--Testcase 68:
SELECT * FROM main_exports ORDER BY _id;

-- Retrieve complex data containing Json objects (__doc tests)
--Testcase 69:
SELECT json_data.key AS key1, json_data.value AS value1
  FROM test_json, json_each_text(test_json.__doc) AS json_data
  WHERE key NOT IN ('_id') ORDER BY json_data.key COLLATE "C";
--Testcase 70:
SELECT json_data.key AS key1, json_data.value AS value1
  FROM test_jsonb, jsonb_each_text(test_jsonb.__doc) AS json_data
  WHERE key NOT IN ('_id') ORDER BY json_data.key COLLATE "C";
--Testcase 71:
SELECT json_data.key AS key1, json_data.value AS value1
  FROM test_text, json_each_text(test_text.__doc::json) AS json_data
  WHERE key NOT IN ('_id') ORDER BY json_data.key COLLATE "C";
--Testcase 72:
SELECT json_data.key AS key1, json_data.value AS value1
  FROM test_varchar, json_each_text(test_varchar.__doc::json) AS json_data
  WHERE key NOT IN ('_id') ORDER BY json_data.key COLLATE "C";

-- Inserts some values in mongo_test collection.
--Testcase 73:
INSERT INTO f_mongo_test VALUES ('0', 1, 'One');
--Testcase 74:
INSERT INTO f_mongo_test VALUES ('0', 2, 'Two');
--Testcase 75:
INSERT INTO f_mongo_test VALUES ('0', 3, 'Three');
--Testcase 76:
INSERT INTO f_mongo_test VALUES ('0', 4, 'Four');
--Testcase 77:
INSERT INTO f_mongo_test VALUES ('0', 5, 'Five');
--Testcase 78:
INSERT INTO f_mongo_test VALUES ('0', 6, 'Six');
--Testcase 79:
INSERT INTO f_mongo_test VALUES ('0', 7, 'Seven');
--Testcase 80:
INSERT INTO f_mongo_test VALUES ('0', 8, 'Eight');
--Testcase 81:
INSERT INTO f_mongo_test VALUES ('0', 9, 'Nine');
--Testcase 82:
INSERT INTO f_mongo_test VALUES ('0', 10, 'Ten');

-- Retrieve Data From foreign tables in functions.
--Testcase 83:
CREATE OR REPLACE FUNCTION test_param_where() RETURNS void AS $$
DECLARE
  n varchar;
BEGIN
  FOR x IN 1..9 LOOP
--Testcase 84:
    SELECT b INTO n FROM f_mongo_test WHERE a = x;
    RAISE NOTICE 'Found number %', n;
  END LOOP;
  return;
END
$$ LANGUAGE plpgsql;

--Testcase 85:
SELECT test_param_where();

-- FDW-103: Parameter expression should work correctly with WHERE clause.
--Testcase 86:
SELECT a, b FROM f_mongo_test WHERE a = (SELECT 2) ORDER BY a;
--Testcase 87:
SELECT a, b FROM f_mongo_test WHERE b = (SELECT 'Seven'::text) ORDER BY a;
-- Create local table and load data into it.
--Testcase 88:
CREATE TABLE l_mongo_test AS SELECT a, b FROM f_mongo_test;
-- Check correlated query.
--Testcase 89:
SELECT a, b FROM l_mongo_test lt
  WHERE lt.b = (SELECT b FROM f_mongo_test ft WHERE lt.b = ft.b)
  ORDER BY a;
--Testcase 90:
SELECT a, b FROM l_mongo_test lt
  WHERE lt.a = (SELECT a FROM f_mongo_test ft WHERE lt.a = ft.a)
  ORDER BY a;
--Testcase 91:
SELECT c1, c8 FROM f_test_tbl1 ft1
  WHERE ft1.c8 = (SELECT c1 FROM f_test_tbl2 ft2 WHERE ft1.c8 = ft2.c1)
  ORDER BY c1 LIMIT 2;

-- FDW-197: Casting target list should give correct result.
--Testcase 92:
SELECT a::float FROM f_mongo_test ORDER BY a LIMIT 2;
--Testcase 93:
SELECT a::boolean FROM f_mongo_test ORDER BY a LIMIT 2;
--Testcase 94:
SELECT a, b::varchar FROM f_mongo_test ORDER BY a LIMIT 3;
--Testcase 95:
SELECT a::float, b::varchar FROM f_mongo_test ORDER BY a LIMIT 2;
--Testcase 96:
SELECT a::real, b::char(20) FROM f_mongo_test ORDER BY a LIMIT 2;
--Testcase 97:
SELECT c1, c2::text FROM f_test_tbl1 ORDER BY c1 LIMIT 2;
--Testcase 98:
SELECT a, LENGTH(b) FROM f_mongo_test ORDER BY 1 LIMIT 2;
--Testcase 99:
SELECT t1.c6::float, t1.c6::int, t1.c5::timestamptz, t1.c3::text, t2.c1::numeric, t2.c3
  FROM f_test_tbl1 t1, f_test_tbl2 t2 WHERE t1.c8 = t2.c1
  ORDER BY t2.c1, t1.c6 LIMIT 5;
--Testcase 100:
SELECT SUM(a::float), SUM(a % 2), a % 2 AS "a % 2"FROM f_mongo_test
  GROUP BY a % 2 ORDER BY 2;
--Testcase 101:
SELECT (c6::float + (c1 * length(c3::text))) AS "c1 + c6", c1, c6
  FROM f_test_tbl1 ORDER BY c1 LIMIT 5;

-- FDW-249; LEFT JOIN LATERAL should not crash
--Testcase 102:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.a, t1.b, t3.a, t1_a FROM f_mongo_test t1 LEFT JOIN LATERAL (
  SELECT t2.a, t1.a AS t1_a FROM f_mongo_test t2) t3 ON t1.a = t3.a ORDER BY 1;
--Testcase 103:
SELECT t1.a, t1.b, t3.a, t1_a FROM f_mongo_test t1 LEFT JOIN LATERAL (
  SELECT t2.a, t1.a AS t1_a FROM f_mongo_test t2) t3 ON t1.a = t3.a ORDER BY 1;
--Testcase 104:
SELECT t1.c1, t3.c1, t3.t1_c8 FROM f_test_tbl1 t1 INNER JOIN LATERAL (
  SELECT t2.c1, t1.c8 AS t1_c8 FROM f_test_tbl2 t2) t3 ON t3.c1 = t3.t1_c8
  ORDER BY 1, 2, 3;
--Testcase 105:
SELECT t1.c1, t3.c1, t3.t1_c8 FROM l_test_tbl1 t1 LEFT JOIN LATERAL (
  SELECT t2.c1, t1.c8 AS t1_c8 FROM f_test_tbl2 t2) t3 ON t3.c1 = t3.t1_c8
  ORDER BY 1, 2, 3;
--Testcase 106:
SELECT c1, c2, (SELECT r FROM (SELECT c1 AS c1) x, LATERAL (SELECT c1 AS r) y)
  FROM f_test_tbl1 ORDER BY 1, 2, 3;
-- LATERAL JOIN with RIGHT should throw error
--Testcase 107:
SELECT t1.c1, t3.c1, t3.t1_c8 FROM f_test_tbl1 t1 RIGHT JOIN LATERAL (
  SELECT t2.c1, t1.c8 AS t1_c8 FROM f_test_tbl2 t2) t3 ON t3.c1 = t3.t1_c8
  ORDER BY 1, 2, 3;

-- FDW-262: Should throw an error when we select system attribute.
--Testcase 108:
SELECT xmin FROM f_test_tbl1;
--Testcase 109:
SELECT ctid, xmax, tableoid FROM f_test_tbl1;
--Testcase 110:
SELECT xmax, c1 FROM f_test_tbl1;
--Testcase 111:
SELECT count(tableoid) FROM f_test_tbl1;

-- FDW-391: Support whole-row reference.
--Testcase 112:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c2, t1 FROM f_test_tbl1 t1
  WHERE c1 = 100 ORDER BY 1;
--Testcase 113:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT d, d.c2, e.c1, e
  FROM f_test_tbl2 d LEFT OUTER JOIN f_test_tbl1 e ON d.c1 = e.c8 ORDER BY 1, 3;

-- Cleanup
--Testcase 114:
DELETE FROM f_mongo_test WHERE a != 0;
--Testcase 115:
DROP TABLE l_test_tbl1;
--Testcase 116:
DROP TABLE l_test_tbl2;
--Testcase 117:
DROP TABLE l_mongo_test;
--Testcase 118:
DROP VIEW smpl_vw;
--Testcase 119:
DROP VIEW comp_vw;
--Testcase 120:
DROP VIEW temp_vw;
--Testcase 121:
DROP VIEW mul_tbl_view;
--Testcase 122:
DROP FUNCTION test_param_where();
--Testcase 123:
DROP FOREIGN TABLE f_mongo_test;
--Testcase 124:
DROP FOREIGN TABLE f_test_tbl1;
--Testcase 125:
DROP FOREIGN TABLE f_test_tbl2;
--Testcase 126:
DROP FOREIGN TABLE countries;
--Testcase 127:
DROP FOREIGN TABLE country_elections;
--Testcase 128:
DROP FOREIGN TABLE main_exports;
--Testcase 129:
DROP FOREIGN TABLE test_json;
--Testcase 130:
DROP FOREIGN TABLE test_jsonb;
--Testcase 131:
DROP FOREIGN TABLE test_text;
--Testcase 132:
DROP FOREIGN TABLE test_varchar;
--Testcase 133:
DROP USER MAPPING FOR public SERVER mongo_server;
--Testcase 134:
DROP SERVER mongo_server;
--Testcase 135:
DROP EXTENSION mongo_fdw;
