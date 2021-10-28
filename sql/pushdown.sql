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

-- Create foreign tables
--Testcase 4:
CREATE FOREIGN TABLE f_mongo_test (_id name, a int, b varchar)
  SERVER mongo_server OPTIONS (database 'mongo_fdw_regress', collection 'mongo_test');
--Testcase 5:
CREATE FOREIGN TABLE f_test_tbl1 (_id name, c1 INTEGER, c2 VARCHAR(10), c3 CHAR(9), c4 INTEGER, c5 pg_catalog.Date, c6 DECIMAL, c7 INTEGER, c8 INTEGER)
  SERVER mongo_server OPTIONS (database 'mongo_fdw_regress', collection 'test_tbl1');
--Testcase 6:
CREATE FOREIGN TABLE f_test_tbl2 (_id name, c1 INTEGER, c2 VARCHAR(14), c3 VARCHAR(13))
  SERVER mongo_server OPTIONS (database 'mongo_fdw_regress', collection 'test_tbl2');
--Testcase 7:
CREATE FOREIGN TABLE f_test_tbl3 (_id name, name TEXT, marks FLOAT ARRAY, pass BOOLEAN)
  SERVER mongo_server OPTIONS (database 'mongo_fdw_regress', collection 'test_tbl3');

-- Inserts some values in mongo_test collection.
--Testcase 8:
INSERT INTO f_mongo_test VALUES ('0', 1, 'One');
--Testcase 9:
INSERT INTO f_mongo_test VALUES ('0', 2, 'Two');
--Testcase 10:
INSERT INTO f_mongo_test VALUES ('0', 3, 'Three');

--Testcase 11:
SET datestyle TO ISO;

-- Sample data
--Testcase 12:
SELECT c1, c2, c3, c4, c5, c6, c7, c8 FROM f_test_tbl1 ORDER BY c1;

-- WHERE clause pushdown
--Testcase 13:
EXPLAIN (VERBOSE, COSTS FALSE)
SELECT c1, c2, c6 AS "salary", c8 FROM f_test_tbl1 e
  WHERE c6 IN (1600, 2450)
  ORDER BY c1;
--Testcase 14:
SELECT c1, c2, c6 AS "salary", c8 FROM f_test_tbl1 e
  WHERE c6 IN (1600, 2450)
  ORDER BY c1;

--Testcase 15:
EXPLAIN (VERBOSE, COSTS FALSE)
SELECT c1, c2, c6 FROM f_test_tbl1 e
  WHERE c6 > 3000
  ORDER BY c1;
--Testcase 16:
SELECT c1, c2, c6 FROM f_test_tbl1 e
  WHERE c6 > 3000
  ORDER BY c1;

--Testcase 17:
EXPLAIN (VERBOSE, COSTS FALSE)
SELECT c1, c2, c6, c8 FROM f_test_tbl1 e
  WHERE c6 = 1500
  ORDER BY c1;
--Testcase 18:
SELECT c1, c2, c6, c8 FROM f_test_tbl1 e
  WHERE c6 = 1500
  ORDER BY c1;

--Testcase 19:
EXPLAIN (VERBOSE, COSTS FALSE)
SELECT c1, c2, c6, c8 FROM f_test_tbl1 e
  WHERE c6 BETWEEN 1000 AND 4000
  ORDER BY c1;
--Testcase 20:
SELECT c1, c2, c6, c8 FROM f_test_tbl1 e
  WHERE c6 BETWEEN 1000 AND 4000
  ORDER BY c1;

--Testcase 21:
EXPLAIN (VERBOSE, COSTS FALSE)
SELECT c1, c2, c4, c6, c8 FROM f_test_tbl1 e
  WHERE c4 IS NOT NULL
  ORDER BY c1;
--Testcase 22:
SELECT c1, c2, c4, c6, c8 FROM f_test_tbl1 e
  WHERE c4 IS NOT NULL
  ORDER BY c1;

--Testcase 23:
EXPLAIN (VERBOSE, COSTS FALSE)
SELECT c1, c2, c5 FROM f_test_tbl1 e
  WHERE c5 <= '1980-12-17'
  ORDER BY c1;
--Testcase 24:
SELECT c1, c2, c5 FROM f_test_tbl1 e
  WHERE c5 <= '1980-12-17'
  ORDER BY c1;

--Testcase 25:
EXPLAIN (VERBOSE, COSTS FALSE)
SELECT c1, c2, c6, c8 FROM f_test_tbl1 e
  WHERE c2 IN ('EMP6', 'EMP12', 'EMP5')
  ORDER BY c1;
--Testcase 26:
SELECT c1, c2, c6, c8 FROM f_test_tbl1 e
  WHERE c2 IN ('EMP6', 'EMP12', 'EMP5')
  ORDER BY c1;

--Testcase 27:
EXPLAIN (VERBOSE, COSTS FALSE)
SELECT c1, c2, c6, c8 FROM f_test_tbl1 e
  WHERE c3 LIKE 'SALESMAN'
  ORDER BY c1;
--Testcase 28:
SELECT c1, c2, c6, c8 FROM f_test_tbl1 e
  WHERE c3 LIKE 'SALESMAN'
  ORDER BY c1;

--Testcase 29:
EXPLAIN (VERBOSE, COSTS FALSE)
SELECT c1, c2, c6, c8 FROM f_test_tbl1 e
  WHERE c3 LIKE 'MANA%'
  ORDER BY c1;
--Testcase 30:
SELECT c1, c2, c6, c8 FROM f_test_tbl1 e
  WHERE c3 LIKE 'MANA%'
  ORDER BY c1;

--Testcase 31:
EXPLAIN (VERBOSE, COSTS FALSE)
SELECT a FROM f_mongo_test
  WHERE a%2 = 1
  ORDER BY a;
--Testcase 32:
SELECT a FROM f_mongo_test
  WHERE a%2 = 1
  ORDER BY a;

--Testcase 33:
EXPLAIN (VERBOSE, COSTS FALSE)
SELECT a, b FROM f_mongo_test
  WHERE a >= 1 AND b LIKE '%O%'
  ORDER BY a;
--Testcase 34:
SELECT a, b FROM f_mongo_test
  WHERE a >= 1 AND b LIKE '%O%'
  ORDER BY a;

--Testcase 35:
EXPLAIN (VERBOSE, COSTS FALSE)
SELECT c1, c2, c5 FROM f_test_tbl1 e
  WHERE c5 <= '1980-12-17' AND c2 IN ('EMP1', 'EMP5', 'EMP10') AND c1 = 100
  ORDER BY c1;
--Testcase 36:
SELECT c1, c2, c5 FROM f_test_tbl1 e
  WHERE c5 <= '1980-12-17' AND c2 IN ('EMP1', 'EMP5', 'EMP10') AND c1 = 100
  ORDER BY c1;

--Testcase 37:
EXPLAIN (VERBOSE, COSTS FALSE)
SELECT c1, c2 FROM f_test_tbl1
  WHERE c2 = 'EMP10';
--Testcase 38:
SELECT c1, c2 FROM f_test_tbl1
  WHERE c2 = 'EMP10';

--Testcase 39:
EXPLAIN (VERBOSE, COSTS FALSE)
SELECT c1, c2 FROM f_test_tbl1
  WHERE c2 < 'EMP10';
--Testcase 40:
SELECT c1, c2 FROM f_test_tbl1
  WHERE c2 < 'EMP10';

-- Should not push down if two columns of same table is
-- involved in single WHERE clause operator expression.
--Testcase 41:
EXPLAIN (VERBOSE, COSTS FALSE)
SELECT c1, c6 FROM f_test_tbl1
  WHERE c1 = c6 AND c1 = 1100
  ORDER BY c1;
--Testcase 42:
SELECT c1, c6 FROM f_test_tbl1
  WHERE c1 = c6 AND c1 = 1100
  ORDER BY c1;

-- Nested operator expression in WHERE clause. Shouldn't push down.
--Testcase 43:
EXPLAIN (VERBOSE, COSTS FALSE)
SELECT c1, c2 FROM f_test_tbl1
  WHERE (c1 > 1000) > FALSE;
--Testcase 44:
SELECT c1, c2 FROM f_test_tbl1
  WHERE (c1 > 1000) > FALSE;
--Testcase 45:
EXPLAIN (VERBOSE, COSTS FALSE)
SELECT c1, c2 FROM f_test_tbl1
  WHERE (c1 > 1000) > 0::BOOLEAN;
--Testcase 46:
SELECT c1, c2 FROM f_test_tbl1
  WHERE (c1 > 1000) > 0::BOOLEAN;

-- Shouldn't push down operators where the constant is an array.
--Testcase 47:
EXPLAIN (VERBOSE, COSTS FALSE)
SELECT name, marks FROM f_test_tbl3
  WHERE marks = ARRAY[23::FLOAT, 24::FLOAT]
  ORDER BY name;
--Testcase 48:
SELECT name, marks FROM f_test_tbl3
  WHERE marks = ARRAY[23::FLOAT, 24::FLOAT]
  ORDER BY name;

-- Pushdown in prepared statement.
--Testcase 49:
PREPARE pre_stmt_f_mongo_test(int) AS
  SELECT b FROM f_mongo_test WHERE a = $1 ORDER BY b;
--Testcase 50:
EXPLAIN (VERBOSE, COSTS FALSE)
EXECUTE pre_stmt_f_mongo_test(1);
--Testcase 51:
EXECUTE pre_stmt_f_mongo_test(1);
--Testcase 52:
EXPLAIN (VERBOSE, COSTS FALSE)
EXECUTE pre_stmt_f_mongo_test(2);
--Testcase 53:
EXECUTE pre_stmt_f_mongo_test(2);

-- FDW-297: Only operator expressions should be pushed down in WHERE clause.
--Testcase 54:
EXPLAIN (VERBOSE, COSTS FALSE)
SELECT name, marks FROM f_test_tbl3
  WHERE pass = true
  ORDER BY name;
--Testcase 55:
SELECT name, marks FROM f_test_tbl3
  WHERE pass = true
  ORDER BY name;

-- Cleanup
--Testcase 56:
DELETE FROM f_mongo_test WHERE a != 0;
--Testcase 57:
DROP FOREIGN TABLE f_mongo_test;
--Testcase 58:
DROP FOREIGN TABLE f_test_tbl1;
--Testcase 59:
DROP FOREIGN TABLE f_test_tbl2;
--Testcase 60:
DROP FOREIGN TABLE f_test_tbl3;
--Testcase 61:
DROP USER MAPPING FOR public SERVER mongo_server;
--Testcase 62:
DROP SERVER mongo_server;
--Testcase 63:
DROP EXTENSION mongo_fdw;
