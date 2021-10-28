-- Before running this file User must create database json_regress
-- databases on MongoDB with all permission for
-- user with password and ran mongodb_init.sh
-- file to load collections.
\set ECHO none
\ir sql/parameters.conf
\set ECHO all
SET datestyle TO ISO;
--Testcase 1:
CREATE EXTENSION IF NOT EXISTS mongo_fdw;
--Testcase 2:
CREATE SERVER mongo_server FOREIGN DATA WRAPPER mongo_fdw
  OPTIONS (address :MONGO_HOST, port :MONGO_PORT);
--Testcase 3:
CREATE USER MAPPING FOR public SERVER mongo_server;


--constructors
-- row_to_json
--Testcase 4:
CREATE FOREIGN TABLE rows (_id name, x int, y text)
 SERVER mongo_server OPTIONS (database 'json_regress', collection 'rows');

--Testcase 5:
EXPLAIN VERBOSE SELECT row_to_json(q,true)
FROM rows q;
--Testcase 6:
SELECT row_to_json(q,true)
FROM rows q;

--Testcase 7:
EXPLAIN VERBOSE SELECT row_to_json(row((select array_agg(x) as d from generate_series(5,10) x)),false);
--Testcase 8:
SELECT row_to_json(row((select array_agg(x) as d from generate_series(5,10) x)),false);

-- anyarray column
analyze rows;

--json_agg
--Testcase 9:
EXPLAIN VERBOSE SELECT json_agg(q ORDER BY x, y)
  FROM rows q;
--Testcase 10:
  SELECT json_agg(q ORDER BY x, y)
  FROM rows q;

--Testcase 11:
UPDATE rows SET x = NULL WHERE x = 1;

--Testcase 12:
EXPLAIN VERBOSE SELECT json_agg(q ORDER BY x NULLS FIRST, y)
  FROM rows q;
--Testcase 13:
  SELECT json_agg(q ORDER BY x NULLS FIRST, y)
  FROM rows q;

-- json extraction functions
--Testcase 14:
CREATE FOREIGN TABLE test_json (_id name, json_type text, test_json json)
 SERVER mongo_server OPTIONS (database 'json_regress', collection 'test_json');

--Testcase 15:
EXPLAIN VERBOSE SELECT test_json -> 'x'
FROM test_json
WHERE json_type = 'scalar';
--Testcase 16:
SELECT test_json -> 'x'
FROM test_json
WHERE json_type = 'scalar';

--Testcase 17:
EXPLAIN VERBOSE SELECT test_json -> 'x'
FROM test_json
WHERE json_type = 'array';
--Testcase 18:
SELECT test_json -> 'x'
FROM test_json
WHERE json_type = 'array';

--Testcase 19:
EXPLAIN VERBOSE SELECT test_json -> 'x'
FROM test_json
WHERE json_type = 'object';
--Testcase 20:
SELECT test_json -> 'x'
FROM test_json
WHERE json_type = 'object';

--Testcase 21:
EXPLAIN VERBOSE SELECT test_json->'field2'
FROM test_json
WHERE json_type = 'object';
--Testcase 22:
SELECT test_json->'field2'
FROM test_json
WHERE json_type = 'object';

--Testcase 23:
EXPLAIN VERBOSE SELECT test_json->>'field2'
FROM test_json
WHERE json_type = 'object';
--Testcase 24:
SELECT test_json->>'field2'
FROM test_json
WHERE json_type = 'object';

--Testcase 25:
EXPLAIN VERBOSE SELECT test_json -> 2
FROM test_json
WHERE json_type = 'scalar';
--Testcase 26:
SELECT test_json -> 2
FROM test_json
WHERE json_type = 'scalar';

--Testcase 27:
EXPLAIN VERBOSE SELECT test_json -> 2
FROM test_json
WHERE json_type = 'array';
--Testcase 28:
SELECT test_json -> 2
FROM test_json
WHERE json_type = 'array';

--Testcase 29:
EXPLAIN VERBOSE SELECT test_json -> -1
FROM test_json
WHERE json_type = 'array';
--Testcase 30:
SELECT test_json -> -1
FROM test_json
WHERE json_type = 'array';

--Testcase 31:
EXPLAIN VERBOSE SELECT test_json -> 2
FROM test_json
WHERE json_type = 'object';
--Testcase 32:
SELECT test_json -> 2
FROM test_json
WHERE json_type = 'object';

--Testcase 33:
EXPLAIN VERBOSE SELECT test_json->>2
FROM test_json
WHERE json_type = 'array';
--Testcase 34:
SELECT test_json->>2
FROM test_json
WHERE json_type = 'array';

--Testcase 35:
EXPLAIN VERBOSE SELECT test_json ->> 6 FROM test_json WHERE json_type = 'array';
--Testcase 36:
SELECT test_json ->> 6 FROM test_json WHERE json_type = 'array';
--Testcase 37:
EXPLAIN VERBOSE SELECT test_json ->> 7 FROM test_json WHERE json_type = 'array';
--Testcase 38:
SELECT test_json ->> 7 FROM test_json WHERE json_type = 'array';

--Testcase 39:
EXPLAIN VERBOSE SELECT test_json ->> 'field4' FROM test_json WHERE json_type = 'object';
--Testcase 40:
SELECT test_json ->> 'field4' FROM test_json WHERE json_type = 'object';
--Testcase 41:
EXPLAIN VERBOSE SELECT test_json ->> 'field5' FROM test_json WHERE json_type = 'object';
--Testcase 42:
SELECT test_json ->> 'field5' FROM test_json WHERE json_type = 'object';
--Testcase 43:
EXPLAIN VERBOSE SELECT test_json ->> 'field6' FROM test_json WHERE json_type = 'object';
--Testcase 44:
SELECT test_json ->> 'field6' FROM test_json WHERE json_type = 'object';

--Testcase 45:
EXPLAIN VERBOSE SELECT json_object_keys(test_json)
FROM test_json
WHERE json_type = 'scalar';
--Testcase 46:
SELECT json_object_keys(test_json)
FROM test_json
WHERE json_type = 'scalar';

--Testcase 47:
EXPLAIN VERBOSE SELECT json_object_keys(test_json)
FROM test_json
WHERE json_type = 'array';
--Testcase 48:
SELECT json_object_keys(test_json)
FROM test_json
WHERE json_type = 'array';

--Testcase 49:
EXPLAIN VERBOSE SELECT json_object_keys(test_json)
FROM test_json
WHERE json_type = 'object';
--Testcase 50:
SELECT json_object_keys(test_json)
FROM test_json
WHERE json_type = 'object';

-- nulls
--Testcase 51:
EXPLAIN VERBOSE select (test_json->'field3') is null as expect_false
from test_json
where json_type = 'object';
--Testcase 52:
select (test_json->'field3') is null as expect_false
from test_json
where json_type = 'object';

--Testcase 53:
EXPLAIN VERBOSE select (test_json->>'field3') is null as expect_true
from test_json
where json_type = 'object';
--Testcase 54:
select (test_json->>'field3') is null as expect_true
from test_json
where json_type = 'object';

--Testcase 55:
EXPLAIN VERBOSE select (test_json->3) is null as expect_false
from test_json
where json_type = 'array';
--Testcase 56:
select (test_json->3) is null as expect_false
from test_json
where json_type = 'array';

--Testcase 57:
EXPLAIN VERBOSE select (test_json->>3) is null as expect_true
from test_json
where json_type = 'array';
--Testcase 58:
select (test_json->>3) is null as expect_true
from test_json
where json_type = 'array';

-- populate_record
--Testcase 59:
create type jpop as (a text, b int, c timestamp);

--Testcase 60:
CREATE DOMAIN js_int_array_1d  AS int[]   CHECK(array_length(VALUE, 1) = 3);
--Testcase 61:
CREATE DOMAIN js_int_array_2d  AS int[][] CHECK(array_length(VALUE, 2) = 3);

--Testcase 62:
CREATE TYPE jsrec AS (
	i int,
	ia _int4,
	ia1 int[],
	ia2 int[][],
	ia3 int[][][],
	ia1d js_int_array_1d,
	ia2d js_int_array_2d,
	t text,
	ta text[],
	c char(10),
	ca char(10)[],
	ts timestamp,
	js json,
	jsb jsonb,
	jsa json[],
	rec jpop,
	reca jpop[]
);

-- test type info caching in json_populate_record()
--Testcase 63:
CREATE FOREIGN TABLE jspoptest (_id name, js json)
 SERVER mongo_server OPTIONS (database 'json_regress', collection 'jspoptest');

--Testcase 64:
SELECT (json_populate_record(NULL::jsrec, js)).* FROM jspoptest;
--Testcase 65:
SELECT (json_populate_record(NULL::jsrec, js)).* FROM jspoptest;

--Testcase 66:
DROP TYPE jsrec;
--Testcase 67:
DROP DOMAIN js_int_array_1d;
--Testcase 68:
DROP DOMAIN js_int_array_2d;


--Testcase 69:
CREATE FOREIGN TABLE foo (_id name, serial_num int, name text, type text)
 SERVER mongo_server OPTIONS (database 'json_regress', collection 'foo');

--Testcase 70:
EXPLAIN VERBOSE SELECT json_build_object('turbines',json_object_agg(serial_num,json_build_object('name',name,'type',type)))
FROM foo;
--Testcase 71:
SELECT json_build_object('turbines',json_object_agg(serial_num,json_build_object('name',name,'type',type)))
FROM foo;

--Testcase 72:
EXPLAIN VERBOSE SELECT json_object_agg(name, type) FROM foo;
--Testcase 73:
SELECT json_object_agg(name, type) FROM foo;
--Testcase 74:
INSERT INTO foo VALUES ('60f02fd48ca7854c1731ed54',999999, NULL, 'bar');
--Testcase 75:
EXPLAIN VERBOSE SELECT json_object_agg(name, type) FROM foo;
--Testcase 76:
SELECT json_object_agg(name, type) FROM foo;

--Testcase 77:
DROP USER MAPPING FOR public SERVER mongo_server;
--Testcase 78:
DROP EXTENSION mongo_fdw CASCADE;






 
