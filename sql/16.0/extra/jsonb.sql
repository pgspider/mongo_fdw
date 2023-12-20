-- Before running this file User must create database jsonb_regress
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

-- test non-error-throwing input
CREATE FOREIGN TABLE pg_input_is_valid_tbl (_id name, x text) SERVER mongo_server OPTIONS (database 'json_regress', collection 'pg_input_is_valid_tbl');
INSERT INTO pg_input_is_valid_tbl VALUES ('0', '{"a":true}');
SELECT pg_input_is_valid(x, 'json') FROM pg_input_is_valid_tbl;
DELETE FROM pg_input_is_valid_tbl;

INSERT INTO pg_input_is_valid_tbl VALUES ('1', '{"a":true');
SELECT pg_input_is_valid(x, 'json') FROM pg_input_is_valid_tbl;
SELECT * FROM pg_input_error_info((SELECT x FROM pg_input_is_valid_tbl), 'json');
DELETE FROM pg_input_is_valid_tbl;

INSERT INTO pg_input_is_valid_tbl VALUES ('2', '{"a":1e1000000}');
SELECT * FROM pg_input_error_info((SELECT x FROM pg_input_is_valid_tbl), 'jsonb');
DELETE FROM pg_input_is_valid_tbl;

-- Multi-line JSON input to check ERROR reporting
CREATE FOREIGN TABLE jsonb_tbl (_id name, x text)
 SERVER mongo_server OPTIONS (database 'json_regress', collection 'jsonb_tbl');
INSERT INTO jsonb_tbl(x) VALUES ('{
		"one": 1,
		"two":"two",
		"three":
		true}');
SELECT x::jsonb FROM jsonb_tbl; -- OK
DELETE FROM jsonb_tbl;

INSERT INTO jsonb_tbl(x) VALUES ('{
		"one": 1,
		"two":,"two",  -- ERROR extraneous comma before field "two"
		"three":
		true}');
SELECT x::jsonb FROM jsonb_tbl;
DELETE FROM jsonb_tbl;


INSERT INTO jsonb_tbl(x) VALUES ('{
		"one": 1,
		"two":"two",
		"averyveryveryveryveryveryveryveryveryverylongfieldname":}');
SELECT x::jsonb FROM jsonb_tbl;
-- ERROR missing value for last field
DELETE FROM jsonb_tbl;

--constructors
-- row_to_json
--Testcase 4:
CREATE FOREIGN TABLE rows (_id name, x int, y text)
 SERVER mongo_server OPTIONS (database 'jsonb_regress', collection 'rows');

--Testcase 5:
EXPLAIN VERBOSE SELECT jsonb_agg(q ORDER BY x, y)
  FROM rows q;
--Testcase 6:
SELECT jsonb_agg(q ORDER BY x, y)
  FROM rows q;

--Testcase 7:
UPDATE rows SET x = NULL WHERE x = 1;

--Testcase 8:
EXPLAIN VERBOSE SELECT jsonb_agg(q ORDER BY x NULLS FIRST, y)
  FROM rows q;
--Testcase 9:
SELECT jsonb_agg(q ORDER BY x NULLS FIRST, y)
  FROM rows q;

-- jsonb extraction functions
--Testcase 11:
CREATE FOREIGN TABLE test_jsonb (json_type text, test_json jsonb)
 SERVER mongo_server OPTIONS (database 'jsonb_regress', collection 'test_jsonb');

--Testcase 12:
EXPLAIN VERBOSE SELECT test_json -> 'x' FROM test_jsonb WHERE json_type = 'scalar';
--Testcase 13:
SELECT test_json -> 'x' FROM test_jsonb WHERE json_type = 'scalar';
--Testcase 14:
EXPLAIN VERBOSE SELECT test_json -> 'x' FROM test_jsonb WHERE json_type = 'array';
--Testcase 15:
SELECT test_json -> 'x' FROM test_jsonb WHERE json_type = 'array';
--Testcase 16:
EXPLAIN VERBOSE SELECT test_json -> 'x' FROM test_jsonb WHERE json_type = 'object';
--Testcase 17:
SELECT test_json -> 'x' FROM test_jsonb WHERE json_type = 'object';
--Testcase 18:
EXPLAIN VERBOSE SELECT test_json -> 'field2' FROM test_jsonb WHERE json_type = 'object';
--Testcase 19:
SELECT test_json -> 'field2' FROM test_jsonb WHERE json_type = 'object';

--Testcase 20:
EXPLAIN VERBOSE SELECT test_json ->> 'field2' FROM test_jsonb WHERE json_type = 'scalar';
--Testcase 21:
SELECT test_json ->> 'field2' FROM test_jsonb WHERE json_type = 'scalar';
--Testcase 22:
EXPLAIN VERBOSE SELECT test_json ->> 'field2' FROM test_jsonb WHERE json_type = 'array';
--Testcase 23:
SELECT test_json ->> 'field2' FROM test_jsonb WHERE json_type = 'array';
--Testcase 24:
EXPLAIN VERBOSE SELECT test_json ->> 'field2' FROM test_jsonb WHERE json_type = 'object';
--Testcase 25:
SELECT test_json ->> 'field2' FROM test_jsonb WHERE json_type = 'object';

--Testcase 26:
EXPLAIN VERBOSE SELECT test_json -> 2 FROM test_jsonb WHERE json_type = 'scalar';
--Testcase 27:
SELECT test_json -> 2 FROM test_jsonb WHERE json_type = 'scalar';
--Testcase 28:
EXPLAIN VERBOSE SELECT test_json -> 2 FROM test_jsonb WHERE json_type = 'array';
--Testcase 29:
SELECT test_json -> 2 FROM test_jsonb WHERE json_type = 'array';
--Testcase 30:
EXPLAIN VERBOSE SELECT test_json -> 9 FROM test_jsonb WHERE json_type = 'array';
--Testcase 31:
SELECT test_json -> 9 FROM test_jsonb WHERE json_type = 'array';
--Testcase 32:
EXPLAIN VERBOSE SELECT test_json -> 2 FROM test_jsonb WHERE json_type = 'object';
--Testcase 33:
SELECT test_json -> 2 FROM test_jsonb WHERE json_type = 'object';

--Testcase 34:
EXPLAIN VERBOSE SELECT test_json ->> 6 FROM test_jsonb WHERE json_type = 'array';
--Testcase 35:
SELECT test_json ->> 6 FROM test_jsonb WHERE json_type = 'array';
--Testcase 36:
EXPLAIN VERBOSE SELECT test_json ->> 7 FROM test_jsonb WHERE json_type = 'array';
--Testcase 37:
SELECT test_json ->> 7 FROM test_jsonb WHERE json_type = 'array';

--Testcase 38:
EXPLAIN VERBOSE SELECT test_json ->> 'field4' FROM test_jsonb WHERE json_type = 'object';
--Testcase 39:
SELECT test_json ->> 'field4' FROM test_jsonb WHERE json_type = 'object';
--Testcase 40:
EXPLAIN VERBOSE SELECT test_json ->> 'field5' FROM test_jsonb WHERE json_type = 'object';
--Testcase 41:
SELECT test_json ->> 'field5' FROM test_jsonb WHERE json_type = 'object';
--Testcase 42:
EXPLAIN VERBOSE SELECT test_json ->> 'field6' FROM test_jsonb WHERE json_type = 'object';
--Testcase 43:
SELECT test_json ->> 'field6' FROM test_jsonb WHERE json_type = 'object';

--Testcase 44:
EXPLAIN VERBOSE SELECT test_json ->> 2 FROM test_jsonb WHERE json_type = 'scalar';
--Testcase 45:
SELECT test_json ->> 2 FROM test_jsonb WHERE json_type = 'scalar';
--Testcase 46:
EXPLAIN VERBOSE SELECT test_json ->> 2 FROM test_jsonb WHERE json_type = 'array';
--Testcase 47:
SELECT test_json ->> 2 FROM test_jsonb WHERE json_type = 'array';
--Testcase 48:
EXPLAIN VERBOSE SELECT test_json ->> 2 FROM test_jsonb WHERE json_type = 'object';
--Testcase 49:
SELECT test_json ->> 2 FROM test_jsonb WHERE json_type = 'object';

--Testcase 50:
EXPLAIN VERBOSE SELECT jsonb_object_keys(test_json) FROM test_jsonb WHERE json_type = 'scalar';
--Testcase 51:
SELECT jsonb_object_keys(test_json) FROM test_jsonb WHERE json_type = 'scalar';
--Testcase 52:
EXPLAIN VERBOSE SELECT jsonb_object_keys(test_json) FROM test_jsonb WHERE json_type = 'array';
--Testcase 53:
SELECT jsonb_object_keys(test_json) FROM test_jsonb WHERE json_type = 'array';
--Testcase 54:
EXPLAIN VERBOSE SELECT jsonb_object_keys(test_json) FROM test_jsonb WHERE json_type = 'object';
--Testcase 55:
SELECT jsonb_object_keys(test_json) FROM test_jsonb WHERE json_type = 'object';

-- nulls
--Testcase 56:
EXPLAIN VERBOSE SELECT (test_json->'field3') IS NULL AS expect_false FROM test_jsonb WHERE json_type = 'object';
--Testcase 57:
SELECT (test_json->'field3') IS NULL AS expect_false FROM test_jsonb WHERE json_type = 'object';
--Testcase 58:
EXPLAIN VERBOSE SELECT (test_json->>'field3') IS NULL AS expect_true FROM test_jsonb WHERE json_type = 'object';
--Testcase 59:
SELECT (test_json->>'field3') IS NULL AS expect_true FROM test_jsonb WHERE json_type = 'object';
--Testcase 60:
EXPLAIN VERBOSE SELECT (test_json->3) IS NULL AS expect_false FROM test_jsonb WHERE json_type = 'array';
--Testcase 61:
SELECT (test_json->3) IS NULL AS expect_false FROM test_jsonb WHERE json_type = 'array';
--Testcase 62:
EXPLAIN VERBOSE SELECT (test_json->>3) IS NULL AS expect_true FROM test_jsonb WHERE json_type = 'array';
--Testcase 63:
SELECT (test_json->>3) IS NULL AS expect_true FROM test_jsonb WHERE json_type = 'array';

-- array exists - array elements should behave as keys
--Testcase 64:
CREATE FOREIGN TABLE testjsonb (_id int4, j jsonb)
 SERVER mongo_server OPTIONS (database 'jsonb_regress', collection 'testjsonb');

--Testcase 65:
EXPLAIN VERBOSE SELECT count(*) from testjsonb  WHERE j->'array' ? 'bar';
--Testcase 66:
SELECT count(*) from testjsonb  WHERE j->'array' ? 'bar';
-- type sensitive array exists - should return no rows (since "exists" only
-- matches strings that are either object keys or array elements)
--Testcase 67:
EXPLAIN VERBOSE SELECT count(*) from testjsonb  WHERE j->'array' ? '5'::text;
--Testcase 68:
SELECT count(*) from testjsonb  WHERE j->'array' ? '5'::text;
-- However, a raw scalar is *contained* within the array
--Testcase 69:
EXPLAIN VERBOSE SELECT count(*) from testjsonb  WHERE j->'array' @> '5'::jsonb;
--Testcase 70:
SELECT count(*) from testjsonb  WHERE j->'array' @> '5'::jsonb;

-- indexing
--Testcase 71:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @> '{"wait":null}';
--Testcase 72:
SELECT count(*) FROM testjsonb WHERE j @> '{"wait":null}';
--Testcase 73:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @> '{"wait":"CC"}';
--Testcase 74:
SELECT count(*) FROM testjsonb WHERE j @> '{"wait":"CC"}';
--Testcase 75:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @> '{"wait":"CC", "public":true}';
--Testcase 76:
SELECT count(*) FROM testjsonb WHERE j @> '{"wait":"CC", "public":true}';
--Testcase 77:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @> '{"age":25}';
--Testcase 78:
SELECT count(*) FROM testjsonb WHERE j @> '{"age":25}';
--Testcase 79:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @> '{"age":25.0}';
--Testcase 80:
SELECT count(*) FROM testjsonb WHERE j @> '{"age":25.0}';
--Testcase 81:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j ? 'public';
--Testcase 82:
SELECT count(*) FROM testjsonb WHERE j ? 'public';
--Testcase 83:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j ? 'bar';
--Testcase 84:
SELECT count(*) FROM testjsonb WHERE j ? 'bar';
--Testcase 85:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j ?| ARRAY['public','disabled'];
--Testcase 86:
SELECT count(*) FROM testjsonb WHERE j ?| ARRAY['public','disabled'];
--Testcase 87:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j ?& ARRAY['public','disabled'];
--Testcase 88:
SELECT count(*) FROM testjsonb WHERE j ?& ARRAY['public','disabled'];
--Testcase 89:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @@ '$.wait == null';
--Testcase 90:
SELECT count(*) FROM testjsonb WHERE j @@ '$.wait == null';
--Testcase 91:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @@ '"CC" == $.wait';
--Testcase 92:
SELECT count(*) FROM testjsonb WHERE j @@ '"CC" == $.wait';
--Testcase 93:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @@ '$.wait == "CC" && true == $.public';
--Testcase 94:
SELECT count(*) FROM testjsonb WHERE j @@ '$.wait == "CC" && true == $.public';
--Testcase 95:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @@ '$.age == 25';
--Testcase 96:
SELECT count(*) FROM testjsonb WHERE j @@ '$.age == 25';
--Testcase 97:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @@ '$.age == 25.0';
--Testcase 98:
SELECT count(*) FROM testjsonb WHERE j @@ '$.age == 25.0';
--Testcase 99:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @@ 'exists($)';
--Testcase 100:
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($)';
--Testcase 101:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @@ 'exists($.public)';
--Testcase 102:
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($.public)';
--Testcase 103:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @@ 'exists($.bar)';
--Testcase 104:
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($.bar)';
--Testcase 105:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @@ 'exists($.public) || exists($.disabled)';
--Testcase 106:
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($.public) || exists($.disabled)';
--Testcase 107:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @@ 'exists($.public) && exists($.disabled)';
--Testcase 108:
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($.public) && exists($.disabled)';
--Testcase 109:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @? '$.wait ? (@ == null)';
--Testcase 110:
SELECT count(*) FROM testjsonb WHERE j @? '$.wait ? (@ == null)';
--Testcase 111:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @? '$.wait ? ("CC" == @)';
--Testcase 112:
SELECT count(*) FROM testjsonb WHERE j @? '$.wait ? ("CC" == @)';
--Testcase 113:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @? '$ ? (@.wait == "CC" && true == @.public)';
--Testcase 114:
SELECT count(*) FROM testjsonb WHERE j @? '$ ? (@.wait == "CC" && true == @.public)';
--Testcase 115:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @? '$.age ? (@ == 25)';
--Testcase 116:
SELECT count(*) FROM testjsonb WHERE j @? '$.age ? (@ == 25)';
--Testcase 117:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @? '$ ? (@.age == 25.0)';
--Testcase 118:
SELECT count(*) FROM testjsonb WHERE j @? '$ ? (@.age == 25.0)';
--Testcase 119:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @? '$';
--Testcase 120:
SELECT count(*) FROM testjsonb WHERE j @? '$';
--Testcase 121:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @? '$.public';
--Testcase 122:
SELECT count(*) FROM testjsonb WHERE j @? '$.public';
--Testcase 123:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @? '$.bar';
--Testcase 124:
SELECT count(*) FROM testjsonb WHERE j @? '$.bar';

--Testcase 125:
CREATE INDEX jidx ON testjsonb USING gin (j);
--Testcase 126:
SET enable_seqscan = off;

--Testcase 127:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @> '{"wait":null}';
--Testcase 128:
SELECT count(*) FROM testjsonb WHERE j @> '{"wait":null}';
--Testcase 129:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @> '{"wait":"CC"}';
--Testcase 130:
SELECT count(*) FROM testjsonb WHERE j @> '{"wait":"CC"}';
--Testcase 131:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @> '{"wait":"CC", "public":true}';
--Testcase 132:
SELECT count(*) FROM testjsonb WHERE j @> '{"wait":"CC", "public":true}';
--Testcase 133:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @> '{"age":25}';
--Testcase 134:
SELECT count(*) FROM testjsonb WHERE j @> '{"age":25}';
--Testcase 135:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @> '{"age":25.0}';
--Testcase 136:
SELECT count(*) FROM testjsonb WHERE j @> '{"age":25.0}';
--Testcase 137:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @> '{"array":["foo"]}';
--Testcase 138:
SELECT count(*) FROM testjsonb WHERE j @> '{"array":["foo"]}';
--Testcase 139:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @> '{"array":["bar"]}';
--Testcase 140:
SELECT count(*) FROM testjsonb WHERE j @> '{"array":["bar"]}';
-- exercise GIN_SEARCH_MODE_ALL
--Testcase 141:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @> '{}';
--Testcase 142:
SELECT count(*) FROM testjsonb WHERE j @> '{}';
--Testcase 143:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j ? 'public';
--Testcase 144:
SELECT count(*) FROM testjsonb WHERE j ? 'public';
--Testcase 145:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j ? 'bar';
--Testcase 146:
SELECT count(*) FROM testjsonb WHERE j ? 'bar';
--Testcase 147:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j ?| ARRAY['public','disabled'];
--Testcase 148:
SELECT count(*) FROM testjsonb WHERE j ?| ARRAY['public','disabled'];
--Testcase 149:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j ?& ARRAY['public','disabled'];
--Testcase 150:
SELECT count(*) FROM testjsonb WHERE j ?& ARRAY['public','disabled'];

--Testcase 151:
EXPLAIN (COSTS OFF)
SELECT count(*) FROM testjsonb WHERE j @@ '$.wait == null';
--Testcase 152:
SELECT count(*) FROM testjsonb WHERE j @@ '$.wait == null';
--Testcase 153:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @@ 'exists($ ? (@.wait == null))';
--Testcase 154:
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($ ? (@.wait == null))';
--Testcase 155:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @@ 'exists($.wait ? (@ == null))';
--Testcase 156:
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($.wait ? (@ == null))';
--Testcase 157:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @@ '"CC" == $.wait';
--Testcase 158:
SELECT count(*) FROM testjsonb WHERE j @@ '"CC" == $.wait';
--Testcase 159:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @@ '$.wait == "CC" && true == $.public';
--Testcase 160:
SELECT count(*) FROM testjsonb WHERE j @@ '$.wait == "CC" && true == $.public';
--Testcase 161:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @@ '$.age == 25';
--Testcase 162:
SELECT count(*) FROM testjsonb WHERE j @@ '$.age == 25';
--Testcase 163:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @@ '$.age == 25.0';
--Testcase 164:
SELECT count(*) FROM testjsonb WHERE j @@ '$.age == 25.0';
--Testcase 165:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @@ '$.array[*] == "foo"';
--Testcase 166:
SELECT count(*) FROM testjsonb WHERE j @@ '$.array[*] == "foo"';
--Testcase 167:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @@ '$.array[*] == "bar"';
--Testcase 168:
SELECT count(*) FROM testjsonb WHERE j @@ '$.array[*] == "bar"';
--Testcase 169:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @@ 'exists($ ? (@.array[*] == "bar"))';
--Testcase 170:
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($ ? (@.array[*] == "bar"))';
--Testcase 171:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @@ 'exists($.array ? (@[*] == "bar"))';
--Testcase 172:
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($.array ? (@[*] == "bar"))';
--Testcase 173:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @@ 'exists($.array[*] ? (@ == "bar"))';
--Testcase 174:
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($.array[*] ? (@ == "bar"))';
--Testcase 175:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @@ 'exists($)';
--Testcase 176:
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($)';
--Testcase 177:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @@ 'exists($.public)';
--Testcase 178:
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($.public)';
--Testcase 179:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @@ 'exists($.bar)';
--Testcase 180:
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($.bar)';
--Testcase 181:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @@ 'exists($.public) || exists($.disabled)';
--Testcase 182:
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($.public) || exists($.disabled)';
--Testcase 183:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @@ 'exists($.public) && exists($.disabled)';
--Testcase 184:
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($.public) && exists($.disabled)';
--Testcase 185:
EXPLAIN (COSTS OFF)
SELECT count(*) FROM testjsonb WHERE j @? '$.wait ? (@ == null)';
--Testcase 186:
SELECT count(*) FROM testjsonb WHERE j @? '$.wait ? (@ == null)';
--Testcase 187:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @? '$.wait ? ("CC" == @)';
--Testcase 188:
SELECT count(*) FROM testjsonb WHERE j @? '$.wait ? ("CC" == @)';
--Testcase 189:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @? '$ ? (@.wait == "CC" && true == @.public)';
--Testcase 190:
SELECT count(*) FROM testjsonb WHERE j @? '$ ? (@.wait == "CC" && true == @.public)';
--Testcase 191:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @? '$.age ? (@ == 25)';
--Testcase 192:
SELECT count(*) FROM testjsonb WHERE j @? '$.age ? (@ == 25)';
--Testcase 193:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @? '$ ? (@.age == 25.0)';
--Testcase 194:
SELECT count(*) FROM testjsonb WHERE j @? '$ ? (@.age == 25.0)';
--Testcase 195:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @? '$ ? (@.array[*] == "bar")';
--Testcase 196:
SELECT count(*) FROM testjsonb WHERE j @? '$ ? (@.array[*] == "bar")';
--Testcase 197:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @? '$.array ? (@[*] == "bar")';
--Testcase 198:
SELECT count(*) FROM testjsonb WHERE j @? '$.array ? (@[*] == "bar")';
--Testcase 199:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @? '$.array[*] ? (@ == "bar")';
--Testcase 200:
SELECT count(*) FROM testjsonb WHERE j @? '$.array[*] ? (@ == "bar")';
--Testcase 201:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @? '$';
--Testcase 202:
SELECT count(*) FROM testjsonb WHERE j @? '$';
--Testcase 203:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @? '$.public';
--Testcase 204:
SELECT count(*) FROM testjsonb WHERE j @? '$.public';
--Testcase 205:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @? '$.bar';
--Testcase 206:
SELECT count(*) FROM testjsonb WHERE j @? '$.bar';

-- array exists - array elements should behave as keys (for GIN index scans too)
--Testcase 207:
CREATE INDEX jidx_array ON testjsonb USING gin((j->'array'));
--Testcase 208:
EXPLAIN VERBOSE SELECT count(*) from testjsonb  WHERE j->'array' ? 'bar';
--Testcase 209:
SELECT count(*) from testjsonb  WHERE j->'array' ? 'bar';
-- type sensitive array exists - should return no rows (since "exists" only
-- matches strings that are either object keys or array elements)
--Testcase 210:
EXPLAIN VERBOSE SELECT count(*) from testjsonb  WHERE j->'array' ? '5'::text;
--Testcase 211:
SELECT count(*) from testjsonb  WHERE j->'array' ? '5'::text;
-- However, a raw scalar is *contained* within the array
--Testcase 212:
EXPLAIN VERBOSE SELECT count(*) from testjsonb  WHERE j->'array' @> '5'::jsonb;
--Testcase 213:
SELECT count(*) from testjsonb  WHERE j->'array' @> '5'::jsonb;

--Testcase 214:
RESET enable_seqscan;

--Testcase 215:
EXPLAIN VERBOSE SELECT count(*) FROM (SELECT (jsonb_each(j)).key FROM testjsonb) AS wow;
--Testcase 216:
SELECT count(*) FROM (SELECT (jsonb_each(j)).key FROM testjsonb) AS wow;
--Testcase 217:
EXPLAIN VERBOSE SELECT key, count(*) FROM (SELECT (jsonb_each(j)).key FROM testjsonb) AS wow GROUP BY key ORDER BY count DESC, key;
--Testcase 218:
SELECT key, count(*) FROM (SELECT (jsonb_each(j)).key FROM testjsonb) AS wow GROUP BY key ORDER BY count DESC, key;

-- sort/hash
--Testcase 219:
EXPLAIN VERBOSE SELECT count(distinct j) FROM testjsonb;
--Testcase 220:
SELECT count(distinct j) FROM testjsonb;
--Testcase 221:
SET enable_hashagg = off;
--Testcase 222:
EXPLAIN VERBOSE SELECT count(*) FROM (SELECT j FROM (SELECT * FROM testjsonb UNION ALL SELECT * FROM testjsonb) js GROUP BY j) js2;
--Testcase 223:
SELECT count(*) FROM (SELECT j FROM (SELECT * FROM testjsonb UNION ALL SELECT * FROM testjsonb) js GROUP BY j) js2;
--Testcase 224:
SET enable_hashagg = on;
--Testcase 225:
SET enable_sort = off;
--Testcase 226:
EXPLAIN VERBOSE SELECT count(*) FROM (SELECT j FROM (SELECT * FROM testjsonb UNION ALL SELECT * FROM testjsonb) js GROUP BY j) js2;
--Testcase 227:
SELECT count(*) FROM (SELECT j FROM (SELECT * FROM testjsonb UNION ALL SELECT * FROM testjsonb) js GROUP BY j) js2;
--Testcase 228:
EXPLAIN VERBOSE SELECT distinct * FROM (values (jsonb '{}' || ''::text),('{}')) v(j);
--Testcase 229:
SELECT distinct * FROM (values (jsonb '{}' || ''::text),('{}')) v(j);
--Testcase 230:
SET enable_sort = on;

--Testcase 231:
RESET enable_hashagg;
--Testcase 232:
RESET enable_sort;

-- btree
--Testcase 233:
CREATE INDEX jidx ON testjsonb USING btree (j);
--Testcase 234:
SET enable_seqscan = off;

--Testcase 235:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j > '{"p":1}';
--Testcase 236:
SELECT count(*) FROM testjsonb WHERE j > '{"p":1}';
--Testcase 237:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j = '{"pos":98, "line":371, "node":"CBA", "indexed":true}';
--Testcase 238:
SELECT count(*) FROM testjsonb WHERE j = '{"pos":98, "line":371, "node":"CBA", "indexed":true}';

--gin path opclass
--Testcase 239:
DROP INDEX jidx;
--Testcase 240:
CREATE INDEX jidx ON testjsonb USING gin (j jsonb_path_ops);
--Testcase 241:
SET enable_seqscan = off;

--Testcase 242:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @> '{"wait":null}';
--Testcase 243:
SELECT count(*) FROM testjsonb WHERE j @> '{"wait":null}';
--Testcase 244:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @> '{"wait":"CC"}';
--Testcase 245:
SELECT count(*) FROM testjsonb WHERE j @> '{"wait":"CC"}';
--Testcase 246:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @> '{"wait":"CC", "public":true}';
--Testcase 247:
SELECT count(*) FROM testjsonb WHERE j @> '{"wait":"CC", "public":true}';
--Testcase 248:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @> '{"age":25}';
--Testcase 249:
SELECT count(*) FROM testjsonb WHERE j @> '{"age":25}';
--Testcase 250:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @> '{"age":25.0}';
--Testcase 251:
SELECT count(*) FROM testjsonb WHERE j @> '{"age":25.0}';
-- exercise GIN_SEARCH_MODE_ALL
--Testcase 252:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @> '{}';
--Testcase 253:
SELECT count(*) FROM testjsonb WHERE j @> '{}';

--Testcase 254:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @@ '$.wait == null';
--Testcase 255:
SELECT count(*) FROM testjsonb WHERE j @@ '$.wait == null';
--Testcase 256:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @@ 'exists($ ? (@.wait == null))';
--Testcase 257:
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($ ? (@.wait == null))';
--Testcase 258:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @@ 'exists($.wait ? (@ == null))';
--Testcase 259:
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($.wait ? (@ == null))';
--Testcase 260:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @@ '"CC" == $.wait';
--Testcase 261:
SELECT count(*) FROM testjsonb WHERE j @@ '"CC" == $.wait';
--Testcase 262:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @@ '$.wait == "CC" && true == $.public';
--Testcase 263:
SELECT count(*) FROM testjsonb WHERE j @@ '$.wait == "CC" && true == $.public';
--Testcase 264:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @@ '$.age == 25';
--Testcase 265:
SELECT count(*) FROM testjsonb WHERE j @@ '$.age == 25';
--Testcase 266:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @@ '$.age == 25.0';
--Testcase 267:
SELECT count(*) FROM testjsonb WHERE j @@ '$.age == 25.0';
--Testcase 268:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @@ '$.array[*] == "foo"';
--Testcase 269:
SELECT count(*) FROM testjsonb WHERE j @@ '$.array[*] == "foo"';
--Testcase 270:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @@ '$.array[*] == "bar"';
--Testcase 271:
SELECT count(*) FROM testjsonb WHERE j @@ '$.array[*] == "bar"';
--Testcase 272:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @@ 'exists($ ? (@.array[*] == "bar"))';
--Testcase 273:
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($ ? (@.array[*] == "bar"))';
--Testcase 274:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @@ 'exists($.array ? (@[*] == "bar"))';
--Testcase 275:
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($.array ? (@[*] == "bar"))';
--Testcase 276:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @@ 'exists($.array[*] ? (@ == "bar"))';
--Testcase 277:
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($.array[*] ? (@ == "bar"))';
--Testcase 278:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @@ 'exists($)';
--Testcase 279:
SELECT count(*) FROM testjsonb WHERE j @@ 'exists($)';

--Testcase 280:
EXPLAIN (COSTS OFF)
SELECT count(*) FROM testjsonb WHERE j @? '$.wait ? (@ == null)';
--Testcase 281:
SELECT count(*) FROM testjsonb WHERE j @? '$.wait ? (@ == null)';
--Testcase 282:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @? '$.wait ? ("CC" == @)';
--Testcase 283:
SELECT count(*) FROM testjsonb WHERE j @? '$.wait ? ("CC" == @)';
--Testcase 284:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @? '$ ? (@.wait == "CC" && true == @.public)';
--Testcase 285:
SELECT count(*) FROM testjsonb WHERE j @? '$ ? (@.wait == "CC" && true == @.public)';
--Testcase 286:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @? '$.age ? (@ == 25)';
--Testcase 287:
SELECT count(*) FROM testjsonb WHERE j @? '$.age ? (@ == 25)';
--Testcase 288:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @? '$ ? (@.age == 25.0)';
--Testcase 289:
SELECT count(*) FROM testjsonb WHERE j @? '$ ? (@.age == 25.0)';
--Testcase 290:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @? '$ ? (@.array[*] == "bar")';
--Testcase 291:
SELECT count(*) FROM testjsonb WHERE j @? '$ ? (@.array[*] == "bar")';
--Testcase 292:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @? '$.array ? (@[*] == "bar")';
--Testcase 293:
SELECT count(*) FROM testjsonb WHERE j @? '$.array ? (@[*] == "bar")';
--Testcase 294:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @? '$.array[*] ? (@ == "bar")';
--Testcase 295:
SELECT count(*) FROM testjsonb WHERE j @? '$.array[*] ? (@ == "bar")';
--Testcase 296:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @? '$';
--Testcase 297:
SELECT count(*) FROM testjsonb WHERE j @? '$';
--Testcase 298:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @? '$.public';
--Testcase 299:
SELECT count(*) FROM testjsonb WHERE j @? '$.public';
--Testcase 300:
EXPLAIN VERBOSE SELECT count(*) FROM testjsonb WHERE j @? '$.bar';
--Testcase 301:
SELECT count(*) FROM testjsonb WHERE j @? '$.bar';

--Testcase 302:
RESET enable_seqscan;
--Testcase 303:
DROP INDEX jidx;

--Testcase 304:
CREATE FOREIGN TABLE foo (_id name, serial_num int, name text, type text)
 SERVER mongo_server OPTIONS (database 'jsonb_regress', collection 'foo');

--Testcase 305:
EXPLAIN VERBOSE SELECT json_build_object('turbines',json_object_agg(serial_num,json_build_object('name',name,'type',type)))
FROM foo;
--Testcase 306:
SELECT json_build_object('turbines',json_object_agg(serial_num,json_build_object('name',name,'type',type)))
FROM foo;

--Testcase 307:
EXPLAIN VERBOSE SELECT json_object_agg(name, type) FROM foo;
--Testcase 308:
SELECT json_object_agg(name, type) FROM foo;
--Testcase 309:
INSERT INTO foo VALUES ('60f0fe64c6d173cad75bf387',999999, NULL, 'bar');
--Testcase 310:
EXPLAIN VERBOSE SELECT json_object_agg(name, type) FROM foo;
--Testcase 311:
SELECT json_object_agg(name, type) FROM foo;


-- populate_record
--Testcase 312:
create type jpop as (a text, b int, c timestamp);

--Testcase 313:
CREATE DOMAIN js_int_array_1d  AS int[]   CHECK(array_length(VALUE, 1) = 3);
--Testcase 314:
CREATE DOMAIN js_int_array_2d  AS int[][] CHECK(array_length(VALUE, 2) = 3);

--Testcase 315:
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
--Testcase 316:
CREATE FOREIGN TABLE jspoptest (_id name, js jsonb)
 SERVER mongo_server OPTIONS (database 'jsonb_regress', collection 'jspoptest');

--Testcase 317:
EXPLAIN VERBOSE SELECT (json_populate_record(NULL::jsrec, js)).* FROM jspoptest;
--Testcase 318:
SELECT (json_populate_record(NULL::jsrec, js)).* FROM jspoptest;

--Testcase 319:
DROP TYPE jsrec;
--Testcase 320:
DROP TYPE jsrec_i_not_null;
--Testcase 321:
DROP DOMAIN js_int_array_1d;
--Testcase 322:
DROP DOMAIN js_int_array_2d;

--Testcase 323:
CREATE FOREIGN TABLE nestjsonb (_id name, j jsonb)
 SERVER mongo_server OPTIONS (database 'jsonb_regress', collection 'nestjsonb');

--Testcase 324:
create index on nestjsonb using gin(j jsonb_path_ops);

--Testcase 325:
set enable_seqscan = on;
--Testcase 326:
set enable_bitmapscan = off;
--Testcase 327:
select * from nestjsonb where j @> '{"a":[[{"x":2}]]}'::jsonb;
--Testcase 328:
select * from nestjsonb where j @> '{"c":3}';
--Testcase 329:
select * from nestjsonb where j @> '[[14]]';
--Testcase 330:
set enable_seqscan = off;
--Testcase 331:
set enable_bitmapscan = on;
--Testcase 332:
select * from nestjsonb where j @> '{"a":[[{"x":2}]]}'::jsonb;
--Testcase 333:
select * from nestjsonb where j @> '{"c":3}';
--Testcase 334:
select * from nestjsonb where j @> '[[14]]';
--Testcase 335:
reset enable_seqscan;
--Testcase 336:
reset enable_bitmapscan;


-- jsonb subscript

INSERT INTO jsonb_tbl(x) VALUES ('123');
SELECT (x::jsonb)['a'] FROM jsonb_tbl;
SELECT (x::jsonb)[0] FROM jsonb_tbl;
SELECT (x::jsonb)[NULL] FROM jsonb_tbl;
DELETE FROM jsonb_tbl;

INSERT INTO jsonb_tbl(x) VALUES ('{"a": 1}');
SELECT (x::jsonb)['a'] FROM jsonb_tbl;
SELECT (x::jsonb)[0] FROM jsonb_tbl;
SELECT (x::jsonb)['not_exist'] FROM jsonb_tbl;
SELECT (x::jsonb)[NULL] FROM jsonb_tbl;
DELETE FROM jsonb_tbl;

INSERT INTO jsonb_tbl(x) VALUES ('[1, "2", null]');
SELECT (x::jsonb)['a'] FROM jsonb_tbl;
SELECT (x::jsonb)[0] FROM jsonb_tbl;
SELECT (x::jsonb)['1'] FROM jsonb_tbl;
SELECT (x::jsonb)[1.0] FROM jsonb_tbl;
SELECT (x::jsonb)[2] FROM jsonb_tbl;
SELECT (x::jsonb)[3] FROM jsonb_tbl;
SELECT (x::jsonb)[-2] FROM jsonb_tbl;
SELECT (x::jsonb)[1]['a'] FROM jsonb_tbl;
SELECT (x::jsonb)[1][0] FROM jsonb_tbl;
DELETE FROM jsonb_tbl;


INSERT INTO jsonb_tbl(x) VALUES ('{"a": 1, "b": "c", "d": [1, 2, 3]}');
SELECT (x::jsonb)['b'] FROM jsonb_tbl;
SELECT (x::jsonb)['d'] FROM jsonb_tbl;
SELECT (x::jsonb)['d'][1] FROM jsonb_tbl;
SELECT (x::jsonb)['d']['a'] FROM jsonb_tbl;
DELETE FROM jsonb_tbl;

INSERT INTO jsonb_tbl(x) VALUES ('{"a": {"a1": {"a2": "aaa"}}, "b": "bbb", "c": "ccc"}');
SELECT (x::jsonb)['a']['a1'] FROM jsonb_tbl;
SELECT (x::jsonb)['a']['a1']['a2'] FROM jsonb_tbl;
SELECT (x::jsonb)['a']['a1']['a2']['a3'] FROM jsonb_tbl;
DELETE FROM jsonb_tbl;

INSERT INTO jsonb_tbl(x) VALUES ('{"a": ["a1", {"b1": ["aaa", "bbb", "ccc"]}], "b": "bb"}');
SELECT (x::jsonb)['a'][1]['b1'] FROM jsonb_tbl;
SELECT (x::jsonb)['a'][1]['b1'][2] FROM jsonb_tbl;
DELETE FROM jsonb_tbl;

-- slices are not supported
INSERT INTO jsonb_tbl(x) VALUES ('{"a": 1}');
SELECT (x::jsonb)['a':'b'] FROM jsonb_tbl;
DELETE FROM jsonb_tbl;

INSERT INTO jsonb_tbl(x) VALUES ('[1, "2", null]');
SELECT (x::jsonb)[1:2] FROM jsonb_tbl;
SELECT (x::jsonb)[:2] FROM jsonb_tbl;
SELECT (x::jsonb)[1:] FROM jsonb_tbl;
SELECT (x::jsonb)[:] FROM jsonb_tbl;
DELETE FROM jsonb_tbl;
DROP FOREIGN TABLE jsonb_tbl;

CREATE FOREIGN TABLE test_jsonb_subscript (
		_id name,
		id int,
		test_json jsonb
) SERVER mongo_server OPTIONS (database 'jsonb_regress', collection 'test_jsonb_subscript');

insert into test_jsonb_subscript(id, test_json) values
(1, '{}'), -- empty jsonb
(2, '{"key": "value"}'); -- jsonb with data

-- update empty jsonb
update test_jsonb_subscript set test_json['a'] = '1' where id = 1;
select id, test_json from test_jsonb_subscript;

-- update jsonb with some data
update test_jsonb_subscript set test_json['a'] = '1' where id = 2;
select id, test_json from test_jsonb_subscript;

-- replace jsonb
update test_jsonb_subscript set test_json['a'] = '"test"';
select id, test_json from test_jsonb_subscript;

-- replace by object
update test_jsonb_subscript set test_json['a'] = '{"b": 1}'::jsonb;
select id, test_json from test_jsonb_subscript;

-- replace by array
update test_jsonb_subscript set test_json['a'] = '[1, 2, 3]'::jsonb;
select id, test_json from test_jsonb_subscript;

-- use jsonb subscription in where clause
select id, test_json from test_jsonb_subscript where test_json['key'] = '"value"';
select id, test_json from test_jsonb_subscript where test_json['key_doesnt_exists'] = '"value"';
select id, test_json from test_jsonb_subscript where test_json['key'] = '"wrong_value"';

-- NULL
update test_jsonb_subscript set test_json[NULL] = '1';
update test_jsonb_subscript set test_json['another_key'] = NULL;
select id, test_json from test_jsonb_subscript;

-- NULL as jsonb source
insert into test_jsonb_subscript(id, test_json) values (3, NULL);
update test_jsonb_subscript set test_json['a'] = '1' where id = 3;
select id, test_json from test_jsonb_subscript;

update test_jsonb_subscript set test_json = NULL where id = 3;
update test_jsonb_subscript set test_json[0] = '1';
select id, test_json from test_jsonb_subscript;

-- Fill the gaps logic
delete from test_jsonb_subscript;
insert into test_jsonb_subscript(id, test_json) values (1, '[0]');

update test_jsonb_subscript set test_json[5] = '1';
select id, test_json from test_jsonb_subscript;

update test_jsonb_subscript set test_json[-4] = '1';
select id, test_json from test_jsonb_subscript;

update test_jsonb_subscript set test_json[-8] = '1';
select id, test_json from test_jsonb_subscript;

-- keep consistent values position
delete from test_jsonb_subscript;
insert into test_jsonb_subscript(id, test_json) values (1, '[]');

update test_jsonb_subscript set test_json[5] = '1';
select id, test_json from test_jsonb_subscript;

-- create the whole path
delete from test_jsonb_subscript;
insert into test_jsonb_subscript(id, test_json) values (1, '{}');
update test_jsonb_subscript set test_json['a'][0]['b'][0]['c'] = '1';
select id, test_json from test_jsonb_subscript;

delete from test_jsonb_subscript;
insert into test_jsonb_subscript(id, test_json) values (1, '{}');
update test_jsonb_subscript set test_json['a'][2]['b'][2]['c'][2] = '1';
select id, test_json from test_jsonb_subscript;

-- create the whole path with already existing keys
delete from test_jsonb_subscript;
insert into test_jsonb_subscript(id, test_json) values (1, '{"b": 1}');
update test_jsonb_subscript set test_json['a'][0] = '2';
select id, test_json from test_jsonb_subscript;

-- the start jsonb is an object, first subscript is treated as a key
delete from test_jsonb_subscript;
insert into test_jsonb_subscript(id, test_json) values (1, '{}');
update test_jsonb_subscript set test_json[0]['a'] = '1';
select id, test_json from test_jsonb_subscript;

-- the start jsonb is an array
delete from test_jsonb_subscript;
insert into test_jsonb_subscript(id, test_json) values (1, '[]');
update test_jsonb_subscript set test_json[0]['a'] = '1';
update test_jsonb_subscript set test_json[2]['b'] = '2';
select id, test_json from test_jsonb_subscript;

-- overwriting an existing path
delete from test_jsonb_subscript;
insert into test_jsonb_subscript(id, test_json) values (1, '{}');
update test_jsonb_subscript set test_json['a']['b'][1] = '1';
update test_jsonb_subscript set test_json['a']['b'][10] = '1';
select id, test_json from test_jsonb_subscript;

delete from test_jsonb_subscript;
insert into test_jsonb_subscript(id, test_json) values (1, '[]');
update test_jsonb_subscript set test_json[0][0][0] = '1';
update test_jsonb_subscript set test_json[0][0][1] = '1';
select id, test_json from test_jsonb_subscript;

delete from test_jsonb_subscript;
insert into test_jsonb_subscript(id, test_json) values (1, '{}');
update test_jsonb_subscript set test_json['a']['b'][10] = '1';
update test_jsonb_subscript set test_json['a'][10][10] = '1';
select id, test_json from test_jsonb_subscript;

-- an empty sub element

delete from test_jsonb_subscript;
insert into test_jsonb_subscript(id, test_json) values (1, '{"a": {}}');
update test_jsonb_subscript set test_json['a']['b']['c'][2] = '1';
select id, test_json from test_jsonb_subscript;

delete from test_jsonb_subscript;
insert into test_jsonb_subscript(id, test_json) values (1, '{"a": []}');
update test_jsonb_subscript set test_json['a'][1]['c'][2] = '1';
select id, test_json from test_jsonb_subscript;

-- trying replace assuming a composite object, but it's an element or a value

delete from test_jsonb_subscript;
insert into test_jsonb_subscript(id, test_json) values (1, '{"a": 1}');
update test_jsonb_subscript set test_json['a']['b'] = '1';
update test_jsonb_subscript set test_json['a']['b']['c'] = '1';
update test_jsonb_subscript set test_json['a'][0] = '1';
update test_jsonb_subscript set test_json['a'][0]['c'] = '1';
update test_jsonb_subscript set test_json['a'][0][0] = '1';

-- trying replace assuming a composite object, but it's a raw scalar

delete from test_jsonb_subscript;
insert into test_jsonb_subscript(id, test_json) values (1, 'null');
update test_jsonb_subscript set test_json[0] = '1';
update test_jsonb_subscript set test_json[0][0] = '1';

delete from test_jsonb_subscript;
DROP FOREIGN TABLE test_jsonb_subscript;

-- try some things with short-header and toasted subscript values
CREATE FOREIGN TABLE test_jsonb_subscript (
		_id name,
		id text,
		test_json jsonb
) SERVER mongo_server OPTIONS (database 'jsonb_regress', collection 'test_jsonb_subscript_text');

insert into test_jsonb_subscript values(1, 'foo', '{"foo": "bar"}');
insert into test_jsonb_subscript
  select 2, s, ('{"' || s || '": "bar"}')::jsonb from repeat('xyzzy', 500) s;
select length(id), test_json[id] from test_jsonb_subscript;
update test_jsonb_subscript set test_json[id] = '"baz"';
select length(id), test_json[id] from test_jsonb_subscript;

--Testcase 337:
DROP USER MAPPING FOR public SERVER mongo_server;
--Testcase 338:
DROP EXTENSION mongo_fdw CASCADE;
