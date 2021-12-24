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

-- Port outside ushort range. Error.
--Testcase 4:
CREATE SERVER mongo_server1 FOREIGN DATA WRAPPER mongo_fdw
  OPTIONS (address :MONGO_HOST, port '65537');
--Testcase 5:
ALTER SERVER mongo_server OPTIONS (SET port '65537');

-- Validate extension, server and mapping details
CREATE OR REPLACE FUNCTION show_details(host TEXT, port TEXT, uid TEXT, pwd TEXT) RETURNS int AS $$
DECLARE
  ext TEXT;
  srv TEXT;
  sopts TEXT;
  uopts TEXT;
BEGIN
  SELECT e.fdwname, srvname, array_to_string(s.srvoptions, ','), array_to_string(u.umoptions, ',')
    INTO ext, srv, sopts, uopts
    FROM pg_foreign_data_wrapper e LEFT JOIN pg_foreign_server s ON e.oid = s.srvfdw LEFT JOIN pg_user_mapping u ON s.oid = u.umserver
    WHERE e.fdwname = 'mongo_fdw'
    ORDER BY 1, 2, 3, 4;

  raise notice 'Extension            : %', ext;
  raise notice 'Server               : %', srv;

  IF strpos(sopts, host) <> 0 AND strpos(sopts, port) <> 0 THEN
    raise notice 'Server_Options       : matched';
  END IF;

  IF strpos(uopts, uid) <> 0 AND strpos(uopts, pwd) <> 0 THEN
    raise notice 'User_Mapping_Options : matched';
  END IF;

  return 1;
END;
$$ language plpgsql;
--Testcase 6:
SELECT show_details(:MONGO_HOST, :MONGO_PORT, :MONGO_USER_NAME, :MONGO_PASS);

-- Create foreign tables and perform basic SQL operations
--Testcase 7:
CREATE FOREIGN TABLE f_mongo_test (_id name, a int, b varchar)
  SERVER mongo_server OPTIONS (database 'mongo_fdw_regress', collection 'mongo_test');
--Testcase 8:
SELECT a, b FROM f_mongo_test ORDER BY 1, 2;
--Testcase 9:
INSERT INTO f_mongo_test VALUES ('0', 2, 'mongo_test insert');
--Testcase 10:
SELECT a, b FROM f_mongo_test ORDER BY 1, 2;
--Testcase 11:
UPDATE f_mongo_test SET b = 'mongo_test update' WHERE a = 2;
--Testcase 12:
SELECT a, b FROM f_mongo_test ORDER BY 1, 2;
--Testcase 13:
DELETE FROM f_mongo_test WHERE a = 2;
--Testcase 14:
SELECT a, b FROM f_mongo_test ORDER BY 1, 2;

-- Test SSL option when MongoDB server running in non-SSL mode.
-- Set non-boolean value, should throw an error.
--Testcase 15:
ALTER SERVER mongo_server OPTIONS (ssl '1');
--Testcase 16:
ALTER SERVER mongo_server OPTIONS (ssl 'x');
-- Check for default value i.e. false
--Testcase 17:
SELECT a, b FROM f_mongo_test ORDER BY 1, 2;
-- Set 'true'.
--Testcase 18:
ALTER SERVER mongo_server OPTIONS (ssl 'true');
-- Results into an error as MongoDB server is running in non-SSL mode.
\set VERBOSITY terse
--Testcase 19:
SELECT a, b FROM f_mongo_test ORDER BY 1, 2;
\set VERBOSITY default
-- Switch back to 'false'.
--Testcase 20:
ALTER SERVER mongo_server OPTIONS (SET ssl 'false');
-- Should now be successful.
--Testcase 21:
SELECT a, b FROM f_mongo_test ORDER BY 1, 2;
--Testcase 22:
DROP FOREIGN TABLE f_mongo_test;
--Testcase 23:
DROP USER MAPPING FOR public SERVER mongo_server;
--Testcase 24:
DROP SERVER mongo_server;

-- Create server with authentication_database option
-- authentication_database options is not supported with legacy driver
-- so below queries will fail when compiled with legacy driver.
--Testcase 25:
CREATE SERVER mongo_server FOREIGN DATA WRAPPER mongo_fdw
  OPTIONS (address :MONGO_HOST, port :MONGO_PORT, authentication_database 'NOT_EXIST_DB');
--Testcase 26:
CREATE USER MAPPING FOR public SERVER mongo_server
  OPTIONS (username :MONGO_USER_NAME, password :MONGO_PASS);
--Testcase 27:
CREATE FOREIGN TABLE f_mongo_test (_id name, a int, b varchar)
  SERVER mongo_server OPTIONS (database 'mongo_fdw_regress', collection 'mongo_test');
-- Below query will fail with authentication error as user cannot be
-- authenticated against given authentication_database.
--Testcase 28:
SELECT a, b FROM f_mongo_test ORDER BY 1, 2;
-- Now changed to valid authentication_database so select query should work.
--Testcase 29:
ALTER SERVER mongo_server
  OPTIONS (SET authentication_database 'mongo_fdw_regress');
--Testcase 30:
SELECT a, b FROM f_mongo_test ORDER BY 1, 2;

-- Cleanup
--Testcase 31:
DROP FOREIGN TABLE f_mongo_test;
--Testcase 32:
DROP USER MAPPING FOR public SERVER mongo_server;
--Testcase 33:
DROP SERVER mongo_server;
--Testcase 34:
DROP EXTENSION mongo_fdw;
