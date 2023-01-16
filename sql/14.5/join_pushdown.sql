-- \set MONGO_HOST			`echo \'"$MONGO_HOST"\'`
-- \set MONGO_PORT			`echo \'"$MONGO_PORT"\'`
-- \set MONGO_USER_NAME	`echo \'"$MONGO_USER_NAME"\'`
-- \set MONGO_PASS			`echo \'"$MONGO_PWD"\'` 

-- Before running this file User must create database mongo_fdw_regress on
-- MongoDB with all permission for 'edb' user with 'edb' password and ran
-- mongodb_init.sh file to load collections.

\c contrib_regression

\set ECHO none
\ir sql/parameters.conf
\set ECHO all
--Testcase 1:
SET datestyle TO ISO;

--Testcase 2:
CREATE EXTENSION IF NOT EXISTS mongo_fdw;
--Testcase 3:
CREATE SERVER mongo_server FOREIGN DATA WRAPPER mongo_fdw
  OPTIONS (address :MONGO_HOST, port :MONGO_PORT);
--Testcase 4:
CREATE USER MAPPING FOR public SERVER mongo_server;

--Testcase 5:
CREATE SERVER mongo_server1 FOREIGN DATA WRAPPER mongo_fdw
  OPTIONS (address :MONGO_HOST, port :MONGO_PORT);
--Testcase 6:
CREATE USER MAPPING FOR public SERVER mongo_server1;

-- Create foreign tables.
--Testcase 7:
CREATE FOREIGN TABLE f_test_tbl1 (_id NAME, c1 INTEGER, c2 TEXT, c3 CHAR(9), c4 INTEGER, c5 pg_catalog.Date, c6 DECIMAL, c7 INTEGER, c8 INTEGER)
  SERVER mongo_server OPTIONS (database 'mongo_fdw_regress', collection 'test_tbl1');
--Testcase 8:
CREATE FOREIGN TABLE f_test_tbl2 (_id NAME, c1 INTEGER, c2 TEXT, c3 TEXT)
  SERVER mongo_server OPTIONS (database 'mongo_fdw_regress', collection 'test_tbl2');
--Testcase 9:
CREATE FOREIGN TABLE f_test_tbl3 (_id NAME, c1 INTEGER, c2 TEXT, c3 TEXT)
  SERVER mongo_server1 OPTIONS (database 'mongo_fdw_regress', collection 'test_tbl2');
--Testcase 10:
CREATE FOREIGN TABLE test_text ( __doc text)
  SERVER mongo_server OPTIONS (database 'mongo_fdw_regress', collection 'warehouse');
--Testcase 11:
CREATE FOREIGN TABLE test_varchar ( __doc varchar)
  SERVER mongo_server OPTIONS (database 'mongo_fdw_regress', collection 'warehouse');
--Testcase 12:
CREATE FOREIGN TABLE f_test_tbl4 (_id NAME, c1 INTEGER, c2 TEXT, c3 CHAR(9), c4 INTEGER, c5 pg_catalog.Date, c6 DECIMAL, c7 INTEGER, c8 INTEGER)
  SERVER mongo_server1 OPTIONS (database 'mongo_fdw_regress', collection 'test_tbl1');

--Testcase 13:
INSERT INTO f_test_tbl1 VALUES (0, 1500, 'EMP15', 'FINANCE', 1300, '2000-12-25', 950.0, 400, 60);
--Testcase 14:
INSERT INTO f_test_tbl1 VALUES (0, 1600, 'EMP16', 'ADMIN', 600);
--Testcase 15:
INSERT INTO f_test_tbl2 VALUES (0, 50, 'TESTING', 'NASHIK');
--Testcase 16:
INSERT INTO f_test_tbl2 VALUES (0);


-- Create local table.
--Testcase 17:
CREATE TABLE l_test_tbl1 AS
  SELECT c1, c2, c3, c4, c5, c6, c7, c8 FROM f_test_tbl1;

-- Push down LEFT OUTER JOIN.
--Testcase 18:
EXPLAIN (COSTS OFF)
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d LEFT OUTER JOIN f_test_tbl1 e ON d.c1 = e.c8 ORDER BY 1, 3;
--Testcase 19:
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d LEFT OUTER JOIN f_test_tbl1 e ON d.c1 = e.c8 ORDER BY 1, 3;
--Testcase 20:
EXPLAIN (COSTS OFF)
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl1 e LEFT OUTER JOIN f_test_tbl2 d ON d.c1 = e.c8 ORDER BY 1, 3;
--Testcase 21:
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl1 e LEFT OUTER JOIN f_test_tbl2 d ON e.c8 = d.c1 ORDER BY 1, 3;
--Testcase 22:
EXPLAIN (COSTS OFF)
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d LEFT OUTER JOIN f_test_tbl1 e ON (d.c1 = e.c8 AND e.c4 > d.c1 OR e.c2 < d.c3) ORDER BY 1, 3 OFFSET 50;
--Testcase 23:
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d LEFT OUTER JOIN f_test_tbl1 e ON (d.c1 = e.c8 AND e.c4 > d.c1 OR e.c2 < d.c3) ORDER BY 1, 3 OFFSET 50;
--Testcase 24:
EXPLAIN (COSTS OFF)
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d LEFT OUTER JOIN f_test_tbl1 e ON (d.c1 = e.c8 AND e.c4 > d.c1 AND e.c2 < d.c3) ORDER BY 1, 3;
--Testcase 25:
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d LEFT OUTER JOIN f_test_tbl1 e ON (d.c1 = e.c8 AND e.c4 > d.c1 AND e.c2 < d.c3) ORDER BY 1, 3;
-- Column comparing with 'Constant' pushed down.
--Testcase 26:
EXPLAIN (COSTS OFF)
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d LEFT OUTER JOIN f_test_tbl1 e ON d.c1 = 20 ORDER BY 1, 3;
--Testcase 27:
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d LEFT OUTER JOIN f_test_tbl1 e ON d.c1 = 20 ORDER BY 1, 3;

-- Push down RIGHT OUTER JOIN.
--Testcase 28:
EXPLAIN (COSTS OFF)
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d RIGHT OUTER JOIN f_test_tbl1 e ON d.c1 = e.c8 ORDER BY 1, 3;
--Testcase 29:
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d RIGHT OUTER JOIN f_test_tbl1 e ON d.c1 = e.c8 ORDER BY 1, 3;
--Testcase 30:
EXPLAIN (COSTS OFF)
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl1 e RIGHT OUTER JOIN f_test_tbl2 d ON e.c8 = d.c1 ORDER BY 1, 3;
--Testcase 31:
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl1 e RIGHT OUTER JOIN f_test_tbl2 d ON e.c8 = d.c1 ORDER BY 1, 3;
--Testcase 32:
EXPLAIN (COSTS OFF)
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d RIGHT OUTER JOIN f_test_tbl1 e ON (d.c1 = e.c8 OR e.c4 > d.c1 OR e.c2 < d.c3) ORDER BY 1, 3 OFFSET 60;
--Testcase 33:
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d RIGHT OUTER JOIN f_test_tbl1 e ON (d.c1 = e.c8 OR e.c4 > d.c1 OR e.c2 < d.c3) ORDER BY 1, 3 OFFSET 60;
--Testcase 34:
EXPLAIN (COSTS OFF)
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d LEFT OUTER JOIN f_test_tbl1 e ON ((d.c1 = e.c8 OR e.c4 > d.c1) AND e.c2 < d.c3) ORDER BY 1, 3 OFFSET 60;
--Testcase 35:
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d LEFT OUTER JOIN f_test_tbl1 e ON ((d.c1 = e.c8 OR e.c4 > d.c1) OR e.c2 < d.c3) ORDER BY 1, 3 OFFSET 60;
-- Column comparing with 'Constant' pushed down.
--Testcase 36:
EXPLAIN (COSTS OFF)
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d RIGHT OUTER JOIN f_test_tbl1 e ON d.c1 = 20 ORDER BY 1, 3;
--Testcase 37:
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d RIGHT OUTER JOIN f_test_tbl1 e ON (d.c1 = 20 AND e.c2 = 'EMP1')  ORDER BY 1, 3;

-- Push INNER JOIN.
--Testcase 38:
EXPLAIN (COSTS OFF)
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d INNER JOIN f_test_tbl1 e ON d.c1 = e.c8 ORDER BY 1, 3;
--Testcase 39:
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d INNER JOIN f_test_tbl1 e ON d.c1 = e.c8 ORDER BY 1, 3;
--Testcase 40:
EXPLAIN (COSTS OFF)
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d INNER JOIN f_test_tbl1 e ON ((d.c1 = e.c8 OR e.c4 > d.c1) AND e.c2 < d.c3) ORDER BY 1, 3 OFFSET 60;
--Testcase 41:
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d INNER JOIN f_test_tbl1 e ON ((d.c1 = e.c8 OR e.c4 > d.c1) OR e.c2 < d.c3) ORDER BY 1, 3 OFFSET 60;
--Testcase 42:
EXPLAIN (COSTS OFF)
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d INNER JOIN f_test_tbl1 e ON (d.c1 = e.c8 OR e.c2 < d.c3) ORDER BY 1, 3 OFFSET 60;
--Testcase 43:
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d INNER JOIN f_test_tbl1 e ON (d.c1 = e.c8 OR e.c2 < d.c3) ORDER BY 1, 3 OFFSET 60;

-- Column comparing with 'Constant' pushed down.
--Testcase 44:
EXPLAIN (COSTS OFF)
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d INNER JOIN f_test_tbl1 e ON (d.c1 = e.c8 AND d.c1 = 20 OR e.c2 = 'EMP1') ORDER BY 1, 3;
--Testcase 45:
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d INNER JOIN f_test_tbl1 e ON (d.c1 = e.c8 AND d.c1 = 20 OR e.c2 = 'EMP1')  ORDER BY 1, 3;
-- INNER JOIN with WHERE clause.  Should execute where condition separately
-- (NOT added into join clauses) on remote side.
--Testcase 46:
EXPLAIN (COSTS OFF)
SELECT d.c1, e.c1
  FROM f_test_tbl1 d JOIN f_test_tbl2 e ON (d.c8 = e.c1) WHERE d.c1 = 100 ORDER BY e.c3, d.c1;
--Testcase 47:
SELECT d.c1, e.c1
  FROM f_test_tbl1 d JOIN f_test_tbl2 e ON (d.c8 = e.c1) WHERE d.c1 = 100 ORDER BY e.c3, d.c1;
-- INNER JOIN in which join clause is not pushable but WHERE condition is
-- pushable with join clause 'TRUE'.
--Testcase 48:
EXPLAIN (COSTS OFF)
SELECT d.c1, e.c1
  FROM f_test_tbl1 d JOIN f_test_tbl2 e ON (abs(d.c8) = e.c1) WHERE d.c1 = 100 ORDER BY e.c3, d.c1;
--Testcase 49:
SELECT d.c1, e.c1
  FROM f_test_tbl1 d JOIN f_test_tbl2 e ON (abs(d.c8) = e.c1) WHERE d.c1 = 100 ORDER BY e.c3, d.c1;

-- Local-Foreign table joins.
--Testcase 50:
EXPLAIN (COSTS OFF)
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d LEFT OUTER JOIN l_test_tbl1 e ON d.c1 = e.c8 ORDER BY 1, 3;
--Testcase 51:
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d LEFT OUTER JOIN l_test_tbl1 e ON d.c1 = e.c8 ORDER BY 1, 3;

-- JOIN in sub-query, should be pushed down.
--Testcase 52:
EXPLAIN (COSTS OFF)
SELECT l.c1, l.c6, l.c8
  FROM l_test_tbl1 l
    WHERE l.c1 IN (SELECT f1.c1 FROM f_test_tbl1 f1 LEFT JOIN f_test_tbl2 f2 ON (f1.c8 = f2.c1)) ORDER BY 1, 3;
--Testcase 53:
SELECT l.c1, l.c6, l.c8
  FROM l_test_tbl1 l
    WHERE l.c1 IN (SELECT f1.c1 FROM f_test_tbl1 f1 LEFT JOIN f_test_tbl2 f2 ON (f1.c8 = f2.c1)) ORDER BY 1, 3;
--Testcase 54:
SET enable_hashjoin TO OFF;
--Testcase 55:
SET enable_nestloop TO OFF;
--Testcase 56:
EXPLAIN (COSTS OFF)
SELECT l.c1, l.c6, l.c8
  FROM l_test_tbl1 l
    WHERE l.c1 = (SELECT f1.c1 FROM f_test_tbl1 f1 LEFT JOIN f_test_tbl2 f2 ON (f1.c8 = f2.c1) LIMIT 1) ORDER BY 1, 3;
--Testcase 57:
SELECT l.c1, l.c6, l.c8
  FROM l_test_tbl1 l
    WHERE l.c1 = (SELECT f1.c1 FROM f_test_tbl1 f1 LEFT JOIN f_test_tbl2 f2 ON (f1.c8 = f2.c1) LIMIT 1) ORDER BY 1, 3;
--Testcase 58:
EXPLAIN (COSTS OFF)
SELECT l.c1, l.c6, l.c8
  FROM l_test_tbl1 l
    WHERE l.c1 = (SELECT f1.c1 FROM f_test_tbl1 f1 INNER JOIN f_test_tbl2 f2 ON (f1.c8 = f2.c1) LIMIT 1) ORDER BY 1, 3;
--Testcase 59:
SELECT l.c1, l.c6, l.c8
  FROM l_test_tbl1 l
    WHERE l.c1 = (SELECT f1.c1 FROM f_test_tbl1 f1 INNER JOIN f_test_tbl2 f2 ON (f1.c8 = f2.c1) LIMIT 1) ORDER BY 1, 3;
--Testcase 60:
RESET enable_hashjoin;
--Testcase 61:
RESET enable_nestloop;

-- Execute JOIN through PREPARE statement.
--Testcase 62:
PREPARE pre_stmt_left_join AS
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d LEFT OUTER JOIN f_test_tbl1 e ON (d.c1 = e.c8 OR e.c4 > d.c1) ORDER BY 1, 3 OFFSET 70;
--Testcase 63:
EXPLAIN (COSTS OFF)
EXECUTE pre_stmt_left_join;
--Testcase 64:
EXECUTE pre_stmt_left_join;
--Testcase 65:
PREPARE pre_stmt_inner_join AS
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d INNER JOIN f_test_tbl1 e ON (d.c1 = e.c8 OR e.c4 > d.c1) ORDER BY 1, 3 OFFSET 70;
--Testcase 66:
EXPLAIN (COSTS OFF)
EXECUTE pre_stmt_inner_join;
--Testcase 67:
EXECUTE pre_stmt_inner_join;

-- join + WHERE clause push-down.
--Testcase 68:
EXPLAIN (COSTS OFF)
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d LEFT OUTER JOIN f_test_tbl1 e ON d.c1 = e.c8 WHERE d.c1 = 10 ORDER BY 1, 3;
--Testcase 69:
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d LEFT OUTER JOIN f_test_tbl1 e ON d.c1 = e.c8 WHERE d.c1 = 10 ORDER BY 1, 3;
--Testcase 70:
EXPLAIN (COSTS OFF)
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d RIGHT OUTER JOIN f_test_tbl1 e ON d.c1 = e.c8 WHERE e.c8 = 10 ORDER BY 1, 3;
--Testcase 71:
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d RIGHT OUTER JOIN f_test_tbl1 e ON d.c1 = e.c8 WHERE e.c8 = 10 ORDER BY 1, 3;
--Testcase 72:
EXPLAIN (COSTS OFF)
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d INNER JOIN f_test_tbl1 e ON d.c1 = e.c8 WHERE d.c2 = 'SALES' ORDER BY 1, 3;
--Testcase 73:
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d INNER JOIN f_test_tbl1 e ON d.c1 = e.c8 WHERE d.c2 = 'SALES' ORDER BY 1, 3;
--Testcase 74:
EXPLAIN (COSTS OFF)
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d INNER JOIN f_test_tbl1 e ON d.c1 = e.c8 WHERE e.c2 = 'EMP2' ORDER BY 1, 3;
--Testcase 75:
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d INNER JOIN f_test_tbl1 e ON d.c1 = e.c8 WHERE e.c2 = 'EMP2' ORDER BY 1, 3;
--Testcase 76:
EXPLAIN (COSTS OFF)
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d INNER JOIN f_test_tbl1 e ON (d.c1 = e.c8 AND d.c1 = 20 OR e.c2 = 'EMP1') WHERE d.c1 = 10 OR e.c8 = 30 ORDER BY 1, 3;
--Testcase 77:
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d INNER JOIN f_test_tbl1 e ON (d.c1 = e.c8 AND d.c1 = 20 OR e.c2 = 'EMP1') WHERE d.c1 = 10 OR e.c8 = 30 ORDER BY 1, 3;
--Testcase 78:
EXPLAIN (COSTS OFF)
SELECT d.c1, d.c2, e.c1, e.c2, d.c6, d.c8
  FROM f_test_tbl1 d LEFT JOIN f_test_tbl2 e ON (e.c1 = d.c8 AND (e.c1 = 20 OR d.c2 = 'EMP1')) WHERE e.c1 = 20 AND d.c8 = 20 ORDER BY 1, 3;
--Testcase 79:
SELECT d.c1, d.c2, e.c1, e.c2, d.c6, d.c8
  FROM f_test_tbl1 d LEFT JOIN f_test_tbl2 e ON (e.c1 = d.c8 AND (e.c1 = 20 OR d.c2 = 'EMP1')) WHERE e.c1 = 20 AND d.c8 = 20 ORDER BY 1, 3;
--Testcase 80:
EXPLAIN (COSTS OFF)
SELECT d.c1, d.c2, d.c5, e.c1, e.c2
  FROM f_test_tbl1 d LEFT JOIN f_test_tbl2 e ON (e.c1 = d.c8 AND (d.c5 = '02-22-1981' OR d.c5 = '12-17-1980')) ORDER BY 1, 3;
--Testcase 81:
SELECT d.c1, d.c2, d.c5, e.c1, e.c2
  FROM f_test_tbl1 d LEFT JOIN f_test_tbl2 e ON (e.c1 = d.c8 AND (d.c5 = '02-22-1981' OR d.c5 = '12-17-1980')) ORDER BY 1, 3;
--Testcase 82:
EXPLAIN (COSTS OFF)
SELECT d.c1, d.c2, d.c5, e.c1, e.c2
  FROM f_test_tbl1 d LEFT JOIN f_test_tbl2 e ON (e.c1 = d.c8) WHERE d.c5 = '02-22-1981' ORDER BY 1;
--Testcase 83:
SELECT d.c1, d.c2, d.c5, e.c1, e.c2
  FROM f_test_tbl1 d LEFT JOIN f_test_tbl2 e ON (e.c1 = d.c8) WHERE d.c5 = '02-22-1981' ORDER BY 1;
--Testcase 84:
EXPLAIN (COSTS OFF)
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d LEFT JOIN f_test_tbl1 e ON (d.c1 = e.c8 AND d.c1 = 20 OR e.c2 = 'EMP1') WHERE d.c1 = 10 OR e.c8 = 30 ORDER BY 1, 3;
--Testcase 85:
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d LEFT JOIN f_test_tbl1 e ON (d.c1 = e.c8 AND d.c1 = 20 OR e.c2 = 'EMP1') WHERE d.c1 = 10 OR e.c8 = 30 ORDER BY 1, 3;

-- Natural join, should push-down.
--Testcase 86:
EXPLAIN (COSTS OFF)
SELECT d.c1, d.c2, d.c5, e.c1, e.c2
  FROM f_test_tbl1 d NATURAL JOIN f_test_tbl1 e WHERE e.c1 > d.c8 ORDER BY 1;
--Testcase 87:
SELECT d.c1, d.c2, d.c5, e.c1, e.c2
  FROM f_test_tbl1 d NATURAL JOIN f_test_tbl1 e WHERE e.c1 > d.c8 ORDER BY 1;
-- Self join, should push-down.
--Testcase 88:
EXPLAIN (COSTS OFF)
SELECT d.c1, d.c2, d.c5, e.c1, e.c2
  FROM f_test_tbl1 d INNER JOIN f_test_tbl1 e ON e.c8 = d.c8 ORDER BY 1 OFFSET 65;
--Testcase 89:
SELECT d.c1, d.c2, d.c5, e.c1, e.c2
  FROM f_test_tbl1 d INNER JOIN f_test_tbl1 e ON e.c8 = d.c8 ORDER BY 1 OFFSET 65;

-- Join in CTE.
-- Explain plan difference between v11 (or pre) and later.
--Testcase 90:
EXPLAIN (COSTS false, VERBOSE)
WITH t (c1_1, c1_3, c2_1) AS (
  SELECT d.c1, d.c3, e.c1
    FROM f_test_tbl1 d JOIN f_test_tbl2 e ON (d.c8 = e.c1)
) SELECT c1_1, c2_1 FROM t ORDER BY c1_3, c1_1;
--Testcase 91:
WITH t (c1_1, c1_3, c2_1) AS (
  SELECT d.c1, d.c3, e.c1
    FROM f_test_tbl1 d JOIN f_test_tbl2 e ON (d.c8 = e.c1)
) SELECT c1_1, c2_1 FROM t ORDER BY c1_3, c1_1;

-- Can push down logic operator in WHERE clause.
--Testcase 92:
EXPLAIN (COSTS OFF)
SELECT d.c1, d.c2, d.c5, e.c1, e.c2
  FROM f_test_tbl2 e LEFT JOIN f_test_tbl1 d ON (e.c1 = d.c8) WHERE d.c5 = '02-22-1981' OR d.c5 = '12-17-1980' ORDER BY 1;
--Testcase 93:
SELECT d.c1, d.c2, d.c5, e.c1, e.c2
  FROM f_test_tbl2 e LEFT JOIN f_test_tbl1 d ON (e.c1 = d.c8) WHERE d.c5 = '02-22-1981' OR d.c5 = '12-17-1980' ORDER BY 1;

-- Nested joins(Don't push-down nested join)
--Testcase 94:
SET enable_mergejoin TO OFF;
--Testcase 95:
EXPLAIN (COSTS OFF)
SELECT d.c1, d.c2, d.c5, e.c1, e.c2
  FROM f_test_tbl1 d LEFT JOIN f_test_tbl2 e ON (e.c1 = d.c8) LEFT JOIN f_test_tbl1 f ON (f.c8 = e.c1) ORDER BY d.c1 OFFSET 65 ;
--Testcase 96:
SELECT d.c1, d.c2, d.c5, e.c1, e.c2
  FROM f_test_tbl1 d LEFT JOIN f_test_tbl2 e ON (e.c1 = d.c8) LEFT JOIN f_test_tbl1 f ON (f.c8 = e.c1) ORDER BY d.c1 OFFSET 65;
--Testcase 97:
RESET enable_mergejoin;

-- Not supported expressions won't push-down(e.g. function expression, etc.)
--Testcase 98:
EXPLAIN (COSTS OFF)
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d LEFT OUTER JOIN f_test_tbl1 e ON (ABS(d.c1) = e.c8) ORDER BY 1, 3;
--Testcase 99:
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d LEFT OUTER JOIN f_test_tbl1 e ON (ABS(d.c1) = e.c8) ORDER BY 1, 3;

-- Don't pushdown when whole row reference is involved.
--Testcase 100:
EXPLAIN (COSTS OFF)
SELECT d, e
  FROM f_test_tbl1 d LEFT JOIN f_test_tbl2 e ON (e.c1 = d.c8) LEFT JOIN f_test_tbl1 f ON (f.c8 = e.c1) ORDER BY e.c1 OFFSET 65;

-- Don't pushdown when full document retrieval is involved.
--Testcase 101:
EXPLAIN (COSTS OFF)
SELECT json_data.key AS key1, json_data.value AS value1
  FROM test_text, test_varchar, json_each_text(test_text.__doc::json) AS json_data WHERE key NOT IN ('_id') ORDER BY json_data.key COLLATE "C";
--Testcase 102:
SELECT json_data.key AS key1, json_data.value AS value1
  FROM test_text, test_varchar, json_each_text(test_text.__doc::json) AS json_data WHERE key NOT IN ('_id') ORDER BY json_data.key COLLATE "C";

-- Join two tables from two different foreign servers.
--Testcase 103:
EXPLAIN (COSTS OFF)
SELECT d.c1, e.c1
  FROM f_test_tbl2 d LEFT OUTER JOIN f_test_tbl3 e ON d.c1 = e.c1 ORDER BY 1;

-- SEMI JOIN, not pushed down
--Testcase 104:
EXPLAIN (COSTS OFF)
SELECT d.c2
  FROM f_test_tbl1 d WHERE EXISTS (SELECT 1 FROM f_test_tbl2 e WHERE d.c8 = e.c1) ORDER BY d.c2 LIMIT 10;
--Testcase 105:
SELECT d.c2
  FROM f_test_tbl1 d WHERE EXISTS (SELECT 1 FROM f_test_tbl2 e WHERE d.c8 = e.c1) ORDER BY d.c2 LIMIT 10;

-- ANTI JOIN, not pushed down
--Testcase 106:
EXPLAIN (COSTS OFF)
SELECT d.c2
  FROM f_test_tbl1 d WHERE NOT EXISTS (SELECT 1 FROM f_test_tbl2 e WHERE d.c8 = e.c1) ORDER BY d.c2 LIMIT 10;
--Testcase 107:
SELECT d.c2
  FROM f_test_tbl1 d WHERE NOT EXISTS (SELECT 1 FROM f_test_tbl2 e WHERE d.c8 = e.c1) ORDER BY d.c2 LIMIT 10;

-- FULL OUTER JOIN, should not pushdown.
--Testcase 108:
EXPLAIN (COSTS OFF)
SELECT d.c1, e.c1
  FROM f_test_tbl1 d FULL JOIN f_test_tbl2 e ON (d.c8 = e.c1) ORDER BY d.c2 LIMIT 10;
--Testcase 109:
SELECT d.c1, e.c1
  FROM f_test_tbl1 d FULL JOIN f_test_tbl2 e ON (d.c8 = e.c1) ORDER BY d.c2 LIMIT 10;

-- CROSS JOIN can be pushed down
--Testcase 110:
EXPLAIN (COSTS OFF)
SELECT e.c1, d.c2
  FROM f_test_tbl1 d CROSS JOIN f_test_tbl2 e ORDER BY e.c1, d.c2 LIMIT 10;
--Testcase 111:
SELECT e.c1, d.c2
  FROM f_test_tbl1 d CROSS JOIN f_test_tbl2 e ORDER BY e.c1, d.c2 LIMIT 10;

-- Test partition-wise join
--Testcase 112:
SET enable_partitionwise_join TO on;

-- Create the partition tables
--Testcase 113:
CREATE TABLE fprt1 (_id NAME, c1 INTEGER, c2 INTEGER, c3 TEXT) PARTITION BY RANGE(c1);
--Testcase 114:
CREATE FOREIGN TABLE ftprt1_p1 PARTITION OF fprt1 FOR VALUES FROM (1) TO (4)
  SERVER mongo_server OPTIONS (database 'mongo_fdw_regress', collection 'test1');
--Testcase 115:
CREATE FOREIGN TABLE ftprt1_p2 PARTITION OF fprt1 FOR VALUES FROM (5) TO (8)
  SERVER mongo_server OPTIONS (database 'mongo_fdw_regress', collection 'test2');

--Testcase 116:
CREATE TABLE fprt2 (_id NAME, c1 INTEGER, c2 INTEGER, c3 TEXT) PARTITION BY RANGE(c2);
--Testcase 117:
CREATE FOREIGN TABLE ftprt2_p1 PARTITION OF fprt2 FOR VALUES FROM (1) TO (4)
  SERVER mongo_server OPTIONS (database 'mongo_fdw_regress', collection 'test3');
--Testcase 118:
CREATE FOREIGN TABLE ftprt2_p2 PARTITION OF fprt2 FOR VALUES FROM (5) TO (8)
  SERVER mongo_server OPTIONS (database 'mongo_fdw_regress', collection 'test4');

-- Inner join two tables
-- Different explain plan on v10 as partition-wise join is not supported there.
--Testcase 119:
SET enable_mergejoin TO OFF;
--Testcase 120:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2
  FROM fprt1 t1 INNER JOIN fprt2 t2 ON (t1.c1 = t2.c2) ORDER BY 1,2;
--Testcase 121:
SELECT t1.c1, t2.c2
  FROM fprt1 t1 INNER JOIN fprt2 t2 ON (t1.c1 = t2.c2) ORDER BY 1,2;

-- Inner join three tables
-- Different explain plan on v10 as partition-wise join is not supported there.
--Testcase 122:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2, t3.c2
  FROM fprt1 t1 INNER JOIN fprt2 t2 ON (t1.c1 = t2.c2) INNER JOIN fprt1 t3 ON (t3.c1 = t2.c2) ORDER BY 1,2;
--Testcase 123:
SELECT t1.c1, t2.c2, t3.c2
  FROM fprt1 t1 INNER JOIN fprt2 t2 ON (t1.c1 = t2.c2) INNER JOIN fprt1 t3 ON (t3.c1 = t2.c2) ORDER BY 1,2;
--Testcase 124:
RESET enable_mergejoin;

-- Join with lateral reference
-- Different explain plan on v10 as partition-wise join is not supported there.
--Testcase 125:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t1.c2
  FROM fprt1 t1, LATERAL (SELECT t2.c1, t2.c2 FROM fprt2 t2
  WHERE t1.c1 = t2.c2 AND t1.c2 = t2.c1) q WHERE t1.c1 % 2 = 0 ORDER BY 1,2;
--Testcase 126:
SELECT t1.c1, t1.c2
  FROM fprt1 t1, LATERAL (SELECT t2.c1, t2.c2 FROM fprt2 t2
  WHERE t1.c1 = t2.c2 AND t1.c2 = t2.c1) q WHERE t1.c1 % 2 = 0 ORDER BY 1,2;

-- With PHVs, partitionwise join selected but no join pushdown
-- Table alias in foreign scan is different for v12, v11 and v10.
--Testcase 127:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t1.phv, t2.c2, t2.phv
  FROM (SELECT 't1_phv' phv, * FROM fprt1 WHERE c1 % 2 = 0) t1 LEFT JOIN
    (SELECT 't2_phv' phv, * FROM fprt2 WHERE c2 % 2 = 0) t2 ON (t1.c1 = t2.c2)
  ORDER BY t1.c1, t2.c2;
--Testcase 128:
SELECT t1.c1, t1.phv, t2.c2, t2.phv
  FROM (SELECT 't1_phv' phv, * FROM fprt1 WHERE c1 % 2 = 0) t1 LEFT JOIN
    (SELECT 't2_phv' phv, * FROM fprt2 WHERE c2 % 2 = 0) t2 ON (t1.c1 = t2.c2)
  ORDER BY t1.c1, t2.c2;
--Testcase 129:
RESET enable_partitionwise_join;

-- FDW-445: Support enable_join_pushdown option at server level and table level.
-- Check only boolean values are accepted.
--Testcase 130:
ALTER SERVER mongo_server OPTIONS (ADD enable_join_pushdown 'abc11');

-- Test the option at server level.
--Testcase 131:
ALTER SERVER mongo_server OPTIONS (ADD enable_join_pushdown 'false');
--Testcase 132:
EXPLAIN (COSTS FALSE, VERBOSE)
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d JOIN f_test_tbl1 e ON d.c1 = e.c8 ORDER BY 1, 3;
--Testcase 133:
ALTER SERVER mongo_server OPTIONS (SET enable_join_pushdown 'true');
--Testcase 134:
EXPLAIN (COSTS FALSE, VERBOSE)
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d JOIN f_test_tbl1 e ON d.c1 = e.c8 ORDER BY 1, 3;

-- Test the option with outer rel.
--Testcase 135:
ALTER FOREIGN TABLE f_test_tbl2 OPTIONS (ADD enable_join_pushdown 'false');
--Testcase 136:
EXPLAIN (COSTS FALSE, VERBOSE)
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d JOIN f_test_tbl1 e ON d.c1 = e.c8 ORDER BY 1, 3;

--Testcase 137:
ALTER FOREIGN TABLE f_test_tbl2 OPTIONS (SET enable_join_pushdown 'true');
--Testcase 138:
EXPLAIN (COSTS FALSE, VERBOSE)
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d JOIN f_test_tbl1 e ON d.c1 = e.c8 ORDER BY 1, 3;

-- Test the option with inner rel.
--Testcase 139:
ALTER FOREIGN TABLE f_test_tbl1 OPTIONS (ADD enable_join_pushdown 'false');
--Testcase 140:
EXPLAIN (COSTS FALSE, VERBOSE)
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d JOIN f_test_tbl1 e ON d.c1 = e.c8 ORDER BY 1, 3;

--Testcase 141:
ALTER FOREIGN TABLE f_test_tbl1 OPTIONS (SET enable_join_pushdown 'true');
--Testcase 142:
EXPLAIN (COSTS FALSE, VERBOSE)
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d JOIN f_test_tbl1 e ON d.c1 = e.c8 ORDER BY 1, 3;

-- Test that setting option at table level does not affect the setting at
-- server level.
--Testcase 143:
ALTER FOREIGN TABLE f_test_tbl1 OPTIONS (SET enable_join_pushdown 'false');
--Testcase 144:
ALTER FOREIGN TABLE f_test_tbl2 OPTIONS (SET enable_join_pushdown 'false');
--Testcase 145:
EXPLAIN (COSTS FALSE, VERBOSE)
SELECT d.c1, d.c2, e.c1, e.c2, e.c6, e.c8
  FROM f_test_tbl2 d JOIN f_test_tbl1 e ON d.c1 = e.c8 ORDER BY 1, 3;

--Testcase 146:
EXPLAIN (COSTS FALSE, VERBOSE)
SELECT t1.c1, t2.c2
  FROM f_test_tbl3 t1 JOIN f_test_tbl4 t2 ON (t1.c1 = t2.c8) ORDER BY 1, 2;

--Testcase 147:
DELETE FROM f_test_tbl1 WHERE c8 IS NULL;
--Testcase 148:
DELETE FROM f_test_tbl1 WHERE c8 = 60;
--Testcase 149:
DELETE FROM f_test_tbl2 WHERE c1 IS NULL;
--Testcase 150:
DELETE FROM f_test_tbl2 WHERE c1 = 50;
--Testcase 151:
DROP FOREIGN TABLE f_test_tbl1;
--Testcase 152:
DROP FOREIGN TABLE f_test_tbl2;
--Testcase 153:
DROP FOREIGN TABLE f_test_tbl3;
--Testcase 154:
DROP FOREIGN TABLE f_test_tbl4;
--Testcase 155:
DROP FOREIGN TABLE test_text;
--Testcase 156:
DROP FOREIGN TABLE test_varchar;
--Testcase 157:
DROP TABLE l_test_tbl1;
--Testcase 158:
DROP FOREIGN TABLE  ftprt1_p1;
--Testcase 159:
DROP FOREIGN TABLE  ftprt1_p2;
--Testcase 160:
DROP FOREIGN TABLE  ftprt2_p1;
--Testcase 161:
DROP FOREIGN TABLE  ftprt2_p2;
--Testcase 162:
DROP TABLE IF EXISTS fprt1;
--Testcase 163:
DROP TABLE IF EXISTS fprt2;
--Testcase 164:
DROP USER MAPPING FOR public SERVER mongo_server1;
--Testcase 165:
DROP SERVER mongo_server1;
--Testcase 166:
DROP USER MAPPING FOR public SERVER mongo_server;
--Testcase 167:
DROP SERVER mongo_server;
--Testcase 168:
DROP EXTENSION mongo_fdw;
