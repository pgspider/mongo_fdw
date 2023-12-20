-- Before running this file User must create database mongo_fdw_regress and
-- mongo_fdw_regress1 databases on MongoDB with all permission for
-- MONGO_USER_NAME user with MONGO_PASS password and ran mongodb_init.sh file
-- to load collections.
\set VERBOSITY terse
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

-- Create foreign tables and validate
--Testcase 4:
CREATE FOREIGN TABLE f_mongo_test (_id name, a int, b varchar)
  SERVER mongo_server OPTIONS (database 'mongo_fdw_regress', collection 'mongo_test');
--Testcase 5:
SELECT a, b FROM f_mongo_test ORDER BY 1, 2;

--
-- fdw-108: After a change to a pg_foreign_server or pg_user_mapping catalog
-- entry, connection should be invalidated.
--

-- Alter one of the SERVER option
-- Set wrong address for mongo_server
--Testcase 6:
ALTER SERVER mongo_server OPTIONS (SET address '127.0.0.10');
ALTER SERVER mongo_server OPTIONS (SET port '9999');
-- Should fail with an error
--Testcase 7:
INSERT INTO f_mongo_test VALUES ('0', 2, 'RECORD INSERTED');
--Testcase 8:
UPDATE f_mongo_test SET b = 'RECORD UPDATED' WHERE a = 2;
--Testcase 9:
DELETE FROM f_mongo_test WHERE a = 2;
--Testcase 10:
SELECT a, b FROM f_mongo_test ORDER BY 1, 2;
-- Set correct address for mongo_server
--Testcase 11:
ALTER SERVER mongo_server OPTIONS (SET address :MONGO_HOST);
ALTER SERVER mongo_server OPTIONS (SET port :MONGO_PORT);
-- Should able to insert the data
--Testcase 12:
INSERT INTO f_mongo_test VALUES ('0', 2, 'RECORD INSERTED');
--Testcase 13:
DELETE FROM f_mongo_test WHERE a = 2;

-- Drop user mapping and create with invalid username and password for public
-- user mapping
--Testcase 14:
DROP USER MAPPING FOR public SERVER mongo_server;
--Testcase 15:
CREATE USER MAPPING FOR public SERVER mongo_server
  OPTIONS (username 'wrong', password 'wrong');
-- Should fail with an error
--Testcase 16:
INSERT INTO f_mongo_test VALUES ('0', 3, 'RECORD INSERTED');
--Testcase 17:
UPDATE f_mongo_test SET b = 'RECORD UPDATED' WHERE a = 3;
--Testcase 18:
DELETE FROM f_mongo_test WHERE a = 3;
--Testcase 19:
SELECT a, b FROM f_mongo_test ORDER BY 1, 2;
-- Drop user mapping and create without username and password for public
-- user mapping
--Testcase 20:
DROP USER MAPPING FOR public SERVER mongo_server;
--Testcase 21:
CREATE USER MAPPING FOR public SERVER mongo_server;
-- Should able to insert the data
--Testcase 22:
INSERT INTO f_mongo_test VALUES ('0', 3, 'RECORD INSERTED');
--Testcase 23:
DELETE FROM f_mongo_test WHERE a = 3;

-- Cleanup
--Testcase 24:
DROP FOREIGN TABLE f_mongo_test;
--Testcase 25:
DROP USER MAPPING FOR public SERVER mongo_server;
--Testcase 26:
DROP SERVER mongo_server;
--Testcase 27:
DROP EXTENSION mongo_fdw;
