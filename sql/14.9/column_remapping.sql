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

-- Column name remmaping
--Testcase 4:
CREATE FOREIGN TABLE test_json_opt (_id name, json_type text, test_json json, remap_col json)
 SERVER mongo_server OPTIONS (database 'json_regress', collection 'test_json');

-- Invalid column
--Testcase 5:
EXPLAIN VERBOSE
SELECT remap_col FROM test_json_opt;
--Testcase 6:
SELECT remap_col FROM test_json_opt;

--Testcase 7:
EXPLAIN VERBOSE
SELECT test_json FROM test_json_opt;
--Testcase 8:
SELECT test_json FROM test_json_opt;

--Testcase 9:
ALTER FOREIGN TABLE test_json_opt ALTER COLUMN remap_col OPTIONS (column_name 'test_json');
--Testcase 10:
EXPLAIN VERBOSE
SELECT remap_col FROM test_json_opt WHERE json_type = 'scalar';
--Testcase 11:
SELECT remap_col FROM test_json_opt WHERE json_type = 'scalar';

--Testcase 12:
ALTER FOREIGN TABLE test_json_opt ALTER COLUMN remap_col OPTIONS (DROP column_name );
--Testcase 13:
ALTER FOREIGN TABLE test_json_opt ALTER COLUMN remap_col OPTIONS (column_name 'test_json');
--Testcase 14:
EXPLAIN VERBOSE
SELECT remap_col FROM test_json_opt WHERE json_type = 'object';
--Testcase 15:
SELECT remap_col FROM test_json_opt WHERE json_type = 'object';

--Testcase 16:
ALTER FOREIGN TABLE test_json_opt ALTER COLUMN remap_col OPTIONS (DROP column_name );
--Testcase 17:
ALTER FOREIGN TABLE test_json_opt ALTER COLUMN remap_col OPTIONS (column_name 'test_json.field1');
--Testcase 18:
EXPLAIN VERBOSE
SELECT remap_col FROM test_json_opt WHERE json_type = 'object';
--Testcase 19:
SELECT remap_col FROM test_json_opt WHERE json_type = 'object';

-- SELECT json null field
--Testcase 20:
ALTER FOREIGN TABLE test_json_opt ALTER COLUMN remap_col OPTIONS (DROP column_name );
--Testcase 21:
ALTER FOREIGN TABLE test_json_opt ALTER COLUMN remap_col OPTIONS (column_name 'test_json.field3');
--Testcase 22:
EXPLAIN VERBOSE
SELECT remap_col FROM test_json_opt WHERE json_type = 'object';
--Testcase 23:
SELECT remap_col FROM test_json_opt WHERE json_type = 'object';

--Testcase 24:
ALTER FOREIGN TABLE test_json_opt ALTER COLUMN remap_col OPTIONS (DROP column_name );
--Testcase 25:
ALTER FOREIGN TABLE test_json_opt ALTER COLUMN remap_col OPTIONS (column_name 'test_json.field4');
--Testcase 26:
EXPLAIN VERBOSE
SELECT remap_col FROM test_json_opt WHERE json_type = 'object';
--Testcase 27:
SELECT remap_col FROM test_json_opt WHERE json_type = 'object';

--Testcase 28:
ALTER FOREIGN TABLE test_json_opt ALTER COLUMN remap_col OPTIONS (DROP column_name );
--Testcase 29:
ALTER FOREIGN TABLE test_json_opt ALTER COLUMN remap_col OPTIONS (column_name 'test_json.field5');
--Testcase 30:
EXPLAIN VERBOSE
SELECT remap_col FROM test_json_opt WHERE json_type = 'object';
--Testcase 31:
SELECT remap_col FROM test_json_opt WHERE json_type = 'object';

--Testcase 32:
ALTER FOREIGN TABLE test_json_opt ALTER COLUMN remap_col OPTIONS (DROP column_name );
--Testcase 33:
ALTER FOREIGN TABLE test_json_opt ALTER COLUMN remap_col OPTIONS (column_name 'test_json.field6');
--Testcase 34:
EXPLAIN VERBOSE
SELECT remap_col FROM test_json_opt WHERE json_type = 'object';
--Testcase 35:
SELECT remap_col FROM test_json_opt WHERE json_type = 'object';

--Testcase 36:
ALTER FOREIGN TABLE test_json_opt ALTER COLUMN remap_col OPTIONS (DROP column_name );
--Testcase 37:
ALTER FOREIGN TABLE test_json_opt ALTER COLUMN remap_col OPTIONS (column_name 'test_json.field6.f1');
--Testcase 38:
EXPLAIN VERBOSE
SELECT remap_col FROM test_json_opt WHERE json_type = 'object';
--Testcase 39:
SELECT remap_col FROM test_json_opt WHERE json_type = 'object';

--Testcase 40:
ALTER FOREIGN TABLE test_json_opt ALTER COLUMN remap_col OPTIONS (DROP column_name );
--Testcase 41:
ALTER FOREIGN TABLE test_json_opt ALTER COLUMN remap_col OPTIONS (column_name 'test_json');
--Testcase 42:
EXPLAIN VERBOSE
SELECT remap_col FROM test_json_opt WHERE json_type = 'array';
--Testcase 43:
SELECT remap_col FROM test_json_opt WHERE json_type = 'array';

-- Access to array member is unsupported by $project, result is no value
--Testcase 44:
ALTER FOREIGN TABLE test_json_opt ALTER COLUMN remap_col OPTIONS (DROP column_name );
--Testcase 45:
ALTER FOREIGN TABLE test_json_opt ALTER COLUMN remap_col OPTIONS (column_name 'test_json.0');
--Testcase 46:
EXPLAIN VERBOSE
SELECT remap_col FROM test_json_opt WHERE json_type = 'array';
--Testcase 47:
SELECT remap_col FROM test_json_opt WHERE json_type = 'array';


-- Column name remmaping
--Testcase 48:
CREATE FOREIGN TABLE test_jsonb_opt (_id name, json_type text, test_json jsonb, remap_col jsonb)
 SERVER mongo_server OPTIONS (database 'jsonb_regress', collection 'test_jsonb');

-- Invalid column
--Testcase 49:
EXPLAIN VERBOSE
SELECT remap_col FROM test_jsonb_opt;
--Testcase 50:
SELECT remap_col FROM test_jsonb_opt;

--Testcase 51:
EXPLAIN VERBOSE
SELECT test_json FROM test_jsonb_opt;
--Testcase 52:
SELECT test_json FROM test_jsonb_opt;

--Testcase 53:
ALTER FOREIGN TABLE test_jsonb_opt ALTER COLUMN remap_col OPTIONS (column_name 'test_json');
--Testcase 54:
EXPLAIN VERBOSE
SELECT remap_col FROM test_jsonb_opt WHERE json_type = 'scalar';
--Testcase 55:
SELECT remap_col FROM test_jsonb_opt WHERE json_type = 'scalar';

--Testcase 56:
ALTER FOREIGN TABLE test_jsonb_opt ALTER COLUMN remap_col OPTIONS (DROP column_name );
--Testcase 57:
ALTER FOREIGN TABLE test_jsonb_opt ALTER COLUMN remap_col OPTIONS (column_name 'test_json');
--Testcase 58:
EXPLAIN VERBOSE
SELECT remap_col FROM test_jsonb_opt WHERE json_type = 'object';
--Testcase 59:
SELECT remap_col FROM test_jsonb_opt WHERE json_type = 'object';

--Testcase 60:
ALTER FOREIGN TABLE test_jsonb_opt ALTER COLUMN remap_col OPTIONS (DROP column_name );
--Testcase 61:
ALTER FOREIGN TABLE test_jsonb_opt ALTER COLUMN remap_col OPTIONS (column_name 'test_json.field1');
--Testcase 62:
EXPLAIN VERBOSE
SELECT remap_col FROM test_jsonb_opt WHERE json_type = 'object';
--Testcase 63:
SELECT remap_col FROM test_jsonb_opt WHERE json_type = 'object';

-- SELECT json null field
--Testcase 64:
ALTER FOREIGN TABLE test_jsonb_opt ALTER COLUMN remap_col OPTIONS (DROP column_name );
--Testcase 65:
ALTER FOREIGN TABLE test_jsonb_opt ALTER COLUMN remap_col OPTIONS (column_name 'test_json.field3');
--Testcase 66:
EXPLAIN VERBOSE
SELECT remap_col FROM test_jsonb_opt WHERE json_type = 'object';
--Testcase 67:
SELECT remap_col FROM test_jsonb_opt WHERE json_type = 'object';

--Testcase 68:
ALTER FOREIGN TABLE test_jsonb_opt ALTER COLUMN remap_col OPTIONS (DROP column_name );
--Testcase 69:
EXPLAIN VERBOSE
SELECT remap_col FROM test_jsonb_opt WHERE json_type = 'object';
--Testcase 70:
SELECT remap_col FROM test_jsonb_opt WHERE json_type = 'object';

--Testcase 71:
ALTER FOREIGN TABLE test_jsonb_opt ALTER COLUMN remap_col OPTIONS (DROP column_name );
--Testcase 72:
ALTER FOREIGN TABLE test_jsonb_opt ALTER COLUMN remap_col OPTIONS (column_name 'test_json.field5');
--Testcase 73:
EXPLAIN VERBOSE
SELECT remap_col FROM test_jsonb_opt WHERE json_type = 'object';
--Testcase 74:
SELECT remap_col FROM test_jsonb_opt WHERE json_type = 'object';

--Testcase 75:
ALTER FOREIGN TABLE test_jsonb_opt ALTER COLUMN remap_col OPTIONS (DROP column_name );
--Testcase 76:
ALTER FOREIGN TABLE test_jsonb_opt ALTER COLUMN remap_col OPTIONS (column_name 'test_json.field6');
--Testcase 77:
EXPLAIN VERBOSE
SELECT remap_col FROM test_jsonb_opt WHERE json_type = 'object';
--Testcase 78:
SELECT remap_col FROM test_jsonb_opt WHERE json_type = 'object';

--Testcase 79:
ALTER FOREIGN TABLE test_jsonb_opt ALTER COLUMN remap_col OPTIONS (DROP column_name );
--Testcase 80:
ALTER FOREIGN TABLE test_jsonb_opt ALTER COLUMN remap_col OPTIONS (column_name 'test_json.field6.f1');
--Testcase 81:
EXPLAIN VERBOSE
SELECT remap_col FROM test_jsonb_opt WHERE json_type = 'object';
--Testcase 82:
SELECT remap_col FROM test_jsonb_opt WHERE json_type = 'object';

--Testcase 83:
ALTER FOREIGN TABLE test_jsonb_opt ALTER COLUMN remap_col OPTIONS (DROP column_name );
--Testcase 84:
ALTER FOREIGN TABLE test_jsonb_opt ALTER COLUMN remap_col OPTIONS (column_name 'test_json');
--Testcase 85:
EXPLAIN VERBOSE
SELECT remap_col FROM test_jsonb_opt WHERE json_type = 'array';
--Testcase 86:
SELECT remap_col FROM test_jsonb_opt WHERE json_type = 'array';

-- Access to array member is unsupported by $project, result is no value
--Testcase 87:
ALTER FOREIGN TABLE test_jsonb_opt ALTER COLUMN remap_col OPTIONS (DROP column_name );
--Testcase 88:
ALTER FOREIGN TABLE test_jsonb_opt ALTER COLUMN remap_col OPTIONS (column_name 'test_json.0');
--Testcase 89:
EXPLAIN VERBOSE
SELECT remap_col FROM test_jsonb_opt WHERE json_type = 'array';
--Testcase 90:
SELECT remap_col FROM test_jsonb_opt WHERE json_type = 'array';

--Testcase 91:
DROP USER MAPPING FOR public SERVER mongo_server;
--Testcase 92:
DROP SERVER mongo_server CASCADE;
--Testcase 93:
DROP EXTENSION mongo_fdw;
