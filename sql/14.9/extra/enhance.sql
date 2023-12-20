-- Before running this file User must create database enhance_regress
-- databases on MongoDB with all permission for
-- user with password and ran mongodb_init.sh
-- file to load collections.
\set ECHO none
\ir sql/parameters.conf
\set ECHO all
SET datestyle TO ISO;
CREATE EXTENSION IF NOT EXISTS mongo_fdw;
CREATE SERVER mongo_server FOREIGN DATA WRAPPER mongo_fdw  OPTIONS (address :MONGO_HOST, port :MONGO_PORT);
CREATE USER MAPPING FOR public SERVER mongo_server;
CREATE FOREIGN TABLE tbl_pushdown (_id int, c1 text, c2 varchar(255), c3 char(255), c4 bigint, c5 float8, c6 int, c7 json, c8 jsonb, c9 timestamp, c10 bool) SERVER mongo_server OPTIONS (database 'enhance_regress', collection 'tbl_pushdown');
--select aggregate funcs with just having
--Testcase 1:
EXPLAIN VERBOSE SELECT count(*), count(c1), min(c2), max(c3), sum(c6) FROM tbl_pushdown HAVING min(c5) < max(c4);
--Testcase 2:
SELECT count(*), count(c1), min(c2), max(c3), sum(c6) FROM tbl_pushdown HAVING min(c5) < max(c4);
--Testcase 3:
EXPLAIN VERBOSE SELECT array_agg(c3 || ' ' || c1) FROM tbl_pushdown HAVING min(c4) <> 0;
--Testcase 4:
SELECT array_agg(c3 || ' ' || c1) FROM tbl_pushdown HAVING min(c4) <> 0;
--Testcase 5:
EXPLAIN VERBOSE SELECT array_agg(c1 || ':' || c2 ORDER BY c1, c2) FROM tbl_pushdown HAVING min(c4) <> 0;
--Testcase 6:
SELECT array_agg(c1 || ':' || c2 ORDER BY c1, c2) FROM tbl_pushdown HAVING min(c4) <> 0;
--Testcase 7:
EXPLAIN VERBOSE SELECT string_agg(c3, ' '), string_agg(c1, ' ') FROM tbl_pushdown HAVING max(c5) > 0;
--Testcase 8:
SELECT string_agg(c3, ' '), string_agg(c1, ' ') FROM tbl_pushdown HAVING max(c5) > 0;
--Testcase 9:
EXPLAIN VERBOSE SELECT string_agg(c3, ':' ORDER BY c3), string_agg(c1, ',' ORDER BY c1) FROM tbl_pushdown HAVING max(c5) >= 0;
--Testcase 10:
SELECT string_agg(c3, ':' ORDER BY c3), string_agg(c1, ',' ORDER BY c1) FROM tbl_pushdown HAVING max(c5) >= 0;
--Testcase 11:
EXPLAIN VERBOSE SELECT string_agg(c3, c1) FROM tbl_pushdown HAVING min(c6) <> min(c4);
--Testcase 12:
SELECT string_agg(c3, c1) FROM tbl_pushdown HAVING min(c6) <> min(c4);
--Testcase 13:
EXPLAIN VERBOSE SELECT avg(c4), stddev(c4), stddev_pop(c5), stddev_samp(c6) FROM tbl_pushdown HAVING min(c6) <> 0;
--Testcase 14:
SELECT avg(c4), stddev(c4), stddev_pop(c5), stddev_samp(c6) FROM tbl_pushdown HAVING min(c6) <> 0;
--Testcase 15:
EXPLAIN VERBOSE SELECT count(DISTINCT c5) FROM tbl_pushdown HAVING min(c6) <> min(c4);
--Testcase 16:
SELECT count(DISTINCT c5) FROM tbl_pushdown HAVING min(c6) <> min(c4);
--non-pushdown aggregate funcs with just having
--Testcase 17:
EXPLAIN VERBOSE SELECT json_agg((c1, '!@!*')), jsonb_agg(('_(*#', c2)), json_object_agg(c1, 'x'), jsonb_object_agg('y', c1) FROM tbl_pushdown HAVING min(c6) <> 0;
--Testcase 18:
SELECT json_agg((c1, '!@!*')), jsonb_agg(('_(*#', c2)), json_object_agg(c1, 'x'), jsonb_object_agg('y', c1) FROM tbl_pushdown HAVING min(c6) <> 0;
--Testcase 19:
EXPLAIN VERBOSE SELECT string_agg(c1::bytea, '\134'::bytea) FROM tbl_pushdown HAVING min(c6) <> 1;
--Testcase 20:
SELECT string_agg(c1::bytea, '\134'::bytea) FROM tbl_pushdown HAVING min(c6) <> 1;
--select aggregate funcs, where, having
--Testcase 21:
EXPLAIN VERBOSE SELECT count(*), count(c1), min(c2), max(c3), sum(c6) FROM tbl_pushdown WHERE c1 IS NOT NULL HAVING min(c5) < max(c4);
--Testcase 22:
SELECT count(*), count(c1), min(c2), max(c3), sum(c6) FROM tbl_pushdown WHERE c1 IS NOT NULL HAVING min(c5) < max(c4);
--Testcase 23:
EXPLAIN VERBOSE SELECT array_agg(c3 || ' ' || c1) FROM tbl_pushdown WHERE c2 != 'aes3r' HAVING min(c4) <> 0;
--Testcase 24:
SELECT array_agg(c3 || ' ' || c1) FROM tbl_pushdown WHERE c2 != 'aes3r' HAVING min(c4) <> 0;
--Testcase 25:
EXPLAIN VERBOSE SELECT array_agg(c1 || ':' || c2 ORDER BY c1, c2) FROM tbl_pushdown WHERE c3 <> ')@' HAVING min(c4) <> 0;
--Testcase 26:
SELECT array_agg(c1 || ':' || c2 ORDER BY c1, c2) FROM tbl_pushdown WHERE c3 <> ')@' HAVING min(c4) <> 0;
--Testcase 27:
EXPLAIN VERBOSE SELECT string_agg(c3, ' '), string_agg(c1, ' ') FROM tbl_pushdown WHERE c1 != c2 HAVING max(c5) > 0;
--Testcase 28:
SELECT string_agg(c3, ' '), string_agg(c1, ' ') FROM tbl_pushdown WHERE c1 != c2 HAVING max(c5) > 0;
--Testcase 29:
EXPLAIN VERBOSE SELECT string_agg(c3, ':' ORDER BY c3), string_agg(c1, ',' ORDER BY c1) FROM tbl_pushdown WHERE c4 <> 0 HAVING max(c5) > 0;
--Testcase 30:
SELECT string_agg(c3, ':' ORDER BY c3), string_agg(c1, ',' ORDER BY c1) FROM tbl_pushdown WHERE c4 <> 0 HAVING max(c5) > 0;
--Testcase 31:
EXPLAIN VERBOSE SELECT string_agg(c3, c1) FROM tbl_pushdown WHERE c4 != 0 HAVING min(c6) <> min(c4);
--Testcase 32:
SELECT string_agg(c3, c1) FROM tbl_pushdown WHERE c4 != 0 HAVING min(c6) <> min(c4);
--Testcase 33:
EXPLAIN VERBOSE SELECT avg(c4), stddev(c4), stddev_pop(c5), stddev_samp(c6) FROM tbl_pushdown WHERE c4 > 0 HAVING min(c6) <> 0;
--Testcase 34:
SELECT avg(c4), stddev(c4), stddev_pop(c5), stddev_samp(c6) FROM tbl_pushdown WHERE c4 > 0 HAVING min(c6) <> 0;
--Testcase 35:
EXPLAIN VERBOSE SELECT count(DISTINCT c5) FROM tbl_pushdown WHERE c6 > 0 HAVING min(c6) <> min(c4);
--Testcase 36:
SELECT count(DISTINCT c5) FROM tbl_pushdown WHERE c6 > 0 HAVING min(c6) <> min(c4);
--non-pushdown aggregate funcs
--Testcase 37:
EXPLAIN VERBOSE SELECT every(c5 > 0) , every(c3 != '##') , every(c1 <> '&2') FROM tbl_pushdown WHERE c6 > 2 GROUP BY c6 HAVING (c6 >> 101) >= 12225;
--Testcase 38:
SELECT every(c5 > 0) , every(c3 != '##') , every(c1 <> '&2') FROM tbl_pushdown WHERE c6 > 2 GROUP BY c6 HAVING (c6 >> 101) >= 12225;
--Testcase 39:
EXPLAIN VERBOSE SELECT bool_and(c5 != 0 OR c4-c5 > 1) , bool_and(c5 != 0) FROM tbl_pushdown WHERE c4 != 0 GROUP BY c6 HAVING NOT (c6 << 97) > 1656;
--Testcase 40:
SELECT bool_and(c5 != 0 OR c4-c5 > 1) , bool_and(c5 != 0) FROM tbl_pushdown WHERE c4 != 0 GROUP BY c6 HAVING NOT (c6 << 97) > 1656;
--aggregate group by
--Testcase 41:
EXPLAIN VERBOSE SELECT c1, c2, c3, c4, c5, c10 FROM tbl_pushdown GROUP BY c1, c2, c3, c4, c5, c10;
--Testcase 42:
SELECT * FROM (
SELECT c1, c2, c3, c4, c5, c10 FROM tbl_pushdown GROUP BY c1, c2, c3, c4, c5, c10
) t ORDER BY 1, 2, 3, 4, 5, 6;
--Testcase 43:
EXPLAIN VERBOSE SELECT c1, avg(c4), c2, sum(c6), c5 FROM tbl_pushdown GROUP BY c1, c2, c5;
--Testcase 44:
SELECT * FROM (
SELECT c1, avg(c4), c2, sum(c6), c5 FROM tbl_pushdown GROUP BY c1, c2, c5
) t ORDER BY 1, 2, 3, 4, 5;
--Testcase 45:
EXPLAIN VERBOSE SELECT avg(c4), stddev(c4), stddev_pop(c5), stddev_samp(c6) FROM tbl_pushdown GROUP BY c1;
--Testcase 46:
SELECT * FROM (
SELECT avg(c4), stddev(c4), stddev_pop(c5), stddev_samp(c6) FROM tbl_pushdown GROUP BY c1
) t ORDER BY 1, 2, 3, 4;
--Testcase 47:
EXPLAIN VERBOSE SELECT array_agg(c1 || ':' || c2) FROM tbl_pushdown GROUP BY c1, c2;
--Testcase 48:
SELECT array_agg(c1 || ':' || c2) FROM tbl_pushdown GROUP BY c1, c2;
--Testcase 49:
EXPLAIN VERBOSE SELECT c1 || 'aef2', c3 || c9, c10, c5+c6 FROM tbl_pushdown GROUP BY 1, 2, 3, 4;
--Testcase 50:
SELECT c1 || 'aef2', c3 || c9, c10, c5+c6 FROM tbl_pushdown GROUP BY 1, 2, 3, 4;
--Testcase 51:
EXPLAIN VERBOSE SELECT c3 || c2, c4/c5+c6, c3 || c1 FROM tbl_pushdown GROUP BY 1, 2, 3;
--Testcase 52:
SELECT c3 || c2, c4/c5+c6, c3 || c1 FROM tbl_pushdown GROUP BY 1, 2, 3;
--aggregate group by, where
--Testcase 53:
EXPLAIN VERBOSE SELECT c1 || 'aef', c2 || c3, c4/12, c5+21, 2*c6 FROM tbl_pushdown WHERE c6 > 0 GROUP BY 1, 2, 3, 4, 5;
--Testcase 54:
SELECT c1 || 'aef', c2 || c3, c4/12, c5+21, 2*c6 FROM tbl_pushdown WHERE c6 > 0 GROUP BY 1, 2, 3, 4, 5;
--Testcase 55:
EXPLAIN VERBOSE SELECT c3 || c1, abs(c4/(c6-c5)), c9, c10 FROM tbl_pushdown WHERE c3 NOT IN ('/pgspider', '/postgres', '/svr') GROUP BY 1, 2, 3, 4;
--Testcase 56:
SELECT c3 || c1, abs(c4/(c6-c5)), c9, c10 FROM tbl_pushdown WHERE c3 NOT IN ('/pgspider', '/postgres', '/svr') GROUP BY 1, 2, 3, 4;
--Testcase 57:
EXPLAIN VERBOSE SELECT c1, avg(c4-c5), sum(c6) FROM tbl_pushdown WHERE c4-c5 > c6 GROUP BY c1;
--Testcase 58:
SELECT c1, avg(c4-c5), sum(c6) FROM tbl_pushdown WHERE c4-c5 > c6 GROUP BY c1;
--Testcase 59:
EXPLAIN VERBOSE SELECT c7->'name', c7->>'key', c7->'events'->1, c7->'events'->>2, c7->'values'->>1 FROM tbl_pushdown WHERE (c7->'values'->>1)::float8 != 0 GROUP BY c7;
--Testcase 60:
SELECT c7->'name', c7->>'key', c7->'events'->1, c7->'events'->>2, c7->'values'->>1 FROM tbl_pushdown WHERE (c7->'values'->>1)::float8 != 0 GROUP BY c7;
--Testcase 61:
EXPLAIN VERBOSE SELECT c8->'teams'->'parent'->'name', c8->'teams'->'parent'->>'id' FROM tbl_pushdown WHERE c8->'teams'->'parent'->>'name' IS NOT NULL GROUP BY c8;
--Testcase 62:
SELECT c8->'teams'->'parent'->'name', c8->'teams'->'parent'->>'id' FROM tbl_pushdown WHERE c8->'teams'->'parent'->>'name' IS NOT NULL GROUP BY c8;
--aggregate group by, having
--Testcase 63:
EXPLAIN VERBOSE SELECT c9, c4, c5, c6 FROM tbl_pushdown GROUP BY 1, 2, 3, 4 HAVING c4 <> 0;
--Testcase 64:
SELECT * FROM (
SELECT c9, c4, c5, c6 FROM tbl_pushdown GROUP BY 1, 2, 3, 4 HAVING c4 <> 0
) t ORDER BY 1, 2, 3, 4;
--Testcase 65:
EXPLAIN VERBOSE SELECT stddev(c4), stddev_pop(c5), stddev_samp(c6) FROM tbl_pushdown GROUP BY c6 HAVING avg(c6) > 0;
--Testcase 66:
SELECT stddev(c4), stddev_pop(c5), stddev_samp(c6) FROM tbl_pushdown GROUP BY c6 HAVING avg(c6) > 0;
--Testcase 67:
EXPLAIN VERBOSE SELECT count(*), count(c1)-1, avg(c6)+3, sum(c4/12) FROM tbl_pushdown GROUP BY c1 HAVING count(c1) > 0;
--Testcase 68:
SELECT count(*), count(c1)-1, avg(c6)+3, sum(c4/12) FROM tbl_pushdown GROUP BY c1 HAVING count(c1) > 0;
--Testcase 69:
EXPLAIN VERBOSE SELECT c1, c2 || c3, c10, c9 FROM tbl_pushdown GROUP BY c1, c2, c3, c4, c5, c6, c9, c10 HAVING c4+c5 > c6;
--Testcase 70:
SELECT c1, c2 || c3, c10, c9 FROM tbl_pushdown GROUP BY c1, c2, c3, c4, c5, c6, c9, c10 HAVING c4+c5 > c6;
--aggregate group by, where, having
--Testcase 71:
EXPLAIN VERBOSE SELECT array_agg(c1 || ' ' || c2) FROM tbl_pushdown WHERE c2 != 'aes3r' GROUP BY c4 HAVING min(c4) <> 0;
--Testcase 72:
SELECT array_agg(c1 || ' ' || c2) FROM tbl_pushdown WHERE c2 != 'aes3r' GROUP BY c4 HAVING min(c4) <> 0;
--Testcase 73:
EXPLAIN VERBOSE SELECT string_agg(c2, ' '), string_agg(c1, ' ') FROM tbl_pushdown WHERE c1 != c2 HAVING max(c5) > 0;
--Testcase 74:
SELECT string_agg(c2, ' '), string_agg(c1, ' ') FROM tbl_pushdown WHERE c1 != c2 HAVING max(c5) > 0;
--Testcase 75:
EXPLAIN VERBOSE SELECT c9, c1, c3, sum(c6)+avg(c4)/2 FROM tbl_pushdown WHERE c1 IS NOT NULL GROUP BY c1, c2, c3, c9 HAVING c2 <> '#@O!';
--Testcase 76:
SELECT * FROM (
SELECT c9, c1, c3, sum(c6)+avg(c4)/2 FROM tbl_pushdown WHERE c1 IS NOT NULL GROUP BY c1, c2, c3, c9 HAVING c2 <> '#@O!'
) t ORDER BY 1, 2, 3, 4;
--Testcase 77:
EXPLAIN VERBOSE SELECT c4 <> 0, c5 < 0, stddev(c5) FROM tbl_pushdown WHERE c4 != 0 GROUP BY c4, c5 HAVING c5 <= c4; 
--Testcase 78:
SELECT c4 <> 0, c5 < 0, stddev(c5) FROM tbl_pushdown WHERE c4 != 0 GROUP BY c4, c5 HAVING c5 <= c4; 
--Testcase 79:
EXPLAIN VERBOSE SELECT c8, c9, c10 FROM tbl_pushdown WHERE c4+c5 > c6*3 GROUP BY c4, c6, c8, c9, c10 HAVING c4 >= c6+23;
--Testcase 80:
SELECT c8, c9, c10 FROM tbl_pushdown WHERE c4+c5 > c6*3 GROUP BY c4, c6, c8, c9, c10 HAVING c4 >= c6+23;
--limit only
--Testcase 81:
EXPLAIN VERBOSE SELECT avg(c4), stddev(c4), stddev_pop(c5), stddev_samp(c6) FROM tbl_pushdown LIMIT 1;
--Testcase 82:
SELECT avg(c4), stddev(c4), stddev_pop(c5), stddev_samp(c6) FROM tbl_pushdown LIMIT 1;
--Testcase 83:
EXPLAIN VERBOSE SELECT c7->'name', c7->>'key', c7->'events'->1, c7->'events'->>2, c7->'values'->>1 FROM tbl_pushdown LIMIT 5;
--Testcase 84:
SELECT c7->'name', c7->>'key', c7->'events'->1, c7->'events'->>2, c7->'values'->>1 FROM tbl_pushdown LIMIT 5;
--Testcase 85:
EXPLAIN VERBOSE SELECT c2 || c1, round(c4/(c6-c5)), c9 FROM tbl_pushdown LIMIT 5;
--Testcase 86:
SELECT c2 || c1, round(c4/(c6-c5)), c9 FROM tbl_pushdown LIMIT 5;
--Testcase 87:
EXPLAIN VERBOSE SELECT c1, c2, c3, c4, c5 FROM tbl_pushdown LIMIT 10;
--Testcase 88:
SELECT c1, c2, c3, c4, c5 FROM tbl_pushdown LIMIT 10;
--Testcase 89:
EXPLAIN VERBOSE SELECT stddev((c7->'values'->>1)::double precision), avg(c4-c5), sum(c6/2) FROM tbl_pushdown LIMIT 2;
--Testcase 90:
SELECT stddev((c7->'values'->>1)::double precision), avg(c4-c5), sum(c6/2) FROM tbl_pushdown LIMIT 2;
--limit, where
--Testcase 91:
EXPLAIN VERBOSE SELECT count(DISTINCT c5), avg(c4), sum(c6)/stddev(c5) FROM tbl_pushdown WHERE c4 NOT IN (12, 23132, -12390123) LIMIT 1;
--Testcase 92:
SELECT count(DISTINCT c5), avg(c4), sum(c6)/stddev(c5) FROM tbl_pushdown WHERE c4 NOT IN (12, 23132, -12390123) LIMIT 1;
--Testcase 93:
EXPLAIN VERBOSE SELECT 2, 23, c1 || '232AF', c4/2, c5+12, c6*12 FROM tbl_pushdown WHERE c4 NOT BETWEEN -100 AND 100 LIMIT 5;
--Testcase 94:
SELECT 2, 23, c1 || '232AF', c4/2, c5+12, c6*12 FROM tbl_pushdown WHERE c4 NOT BETWEEN -100 AND 100 LIMIT 5;
--Testcase 95:
EXPLAIN VERBOSE SELECT 'a3r23', 23, 23.23, '!_@*!@*' FROM tbl_pushdown WHERE c5 IN (12.12, 132.21, 0.12, -53.2, 123123.91231) LIMIT 1;
--Testcase 96:
SELECT 'a3r23', 23, 23.23, '!_@*!@*' FROM tbl_pushdown WHERE c5 IN (12.12, 132.21, 0.12, -53.2, 123123.91231) LIMIT 1;
--Testcase 97:
EXPLAIN VERBOSE SELECT c1, c3, c5, c7 FROM tbl_pushdown WHERE c1 IN ('paintings', 'MOẰNAJFK') LIMIT 0;
--Testcase 98:
SELECT c1, c3, c5, c7 FROM tbl_pushdown WHERE c1 IN ('paintings', 'MOẰNAJFK') LIMIT 0;
--Testcase 99:
EXPLAIN VERBOSE SELECT c9 || '@#', c4 + c5/c6, c6, c10 FROM tbl_pushdown WHERE c4 > 0 LIMIT 5;
--Testcase 100:
SELECT c9 || '@#', c4 + c5/c6, c6, c10 FROM tbl_pushdown WHERE c4 > 0 LIMIT 5;
--limit, where, group by, having
--Testcase 101:
EXPLAIN VERBOSE SELECT array_agg(c3 || ' ' || c1), array_agg(c1 || ':' || c2), stddev_pop(c5), stddev_samp(c6) FROM tbl_pushdown WHERE c5 NOT IN (12.2, -1292.1212) GROUP BY c1 HAVING c1 IS NOT NULL LIMIT 3;
--Testcase 102:
SELECT array_agg(c3 || ' ' || c1), array_agg(c1 || ':' || c2), stddev_pop(c5), stddev_samp(c6) FROM tbl_pushdown WHERE c5 NOT IN (12.2, -1292.1212) GROUP BY c1 HAVING c1 IS NOT NULL LIMIT 3;
--Testcase 103:
EXPLAIN VERBOSE SELECT string_agg(c3, ' '), string_agg(c1, ' ') FROM tbl_pushdown WHERE c6 <= c5  GROUP BY c3 HAVING string_agg(c3, ' ' ORDER BY c3) != 'a3W' LIMIT 1;
--Testcase 104:
SELECT string_agg(c3, ' '), string_agg(c1, ' ') FROM tbl_pushdown WHERE c6 <= c5  GROUP BY c3 HAVING string_agg(c3, ' ' ORDER BY c3) != 'a3W' LIMIT 1;
--Testcase 105:
EXPLAIN VERBOSE SELECT string_agg(c3, ':' ORDER BY c3), string_agg(c1, ',' ORDER BY c1) FROM tbl_pushdown WHERE c4 >= -1212039 GROUP BY c5 HAVING max(c5) > 0 LIMIT 1;
--Testcase 106:
SELECT string_agg(c3, ':' ORDER BY c3), string_agg(c1, ',' ORDER BY c1) FROM tbl_pushdown WHERE c4 >= -1212039 GROUP BY c5 HAVING max(c5) > 0 LIMIT 1;
--Testcase 107:
EXPLAIN VERBOSE SELECT c8->'teams'->'parent'->>'name', c8->'teams'->'parent'->>'id' FROM tbl_pushdown WHERE c8->'teams'->'parent'->>'name' IS NOT NULL GROUP BY c4, c8, c8->'teams'->'parent'->'name', c8->'teams'->'parent'->'id' HAVING c4 <> 0 LIMIT 5;
--Testcase 108:
SELECT c8->'teams'->'parent'->>'name', c8->'teams'->'parent'->>'id' FROM tbl_pushdown WHERE c8->'teams'->'parent'->>'name' IS NOT NULL GROUP BY c4, c8, c8->'teams'->'parent'->'name', c8->'teams'->'parent'->'id' HAVING c4 <> 0 LIMIT 5;
--Testcase 109:
EXPLAIN VERBOSE SELECT c1, avg(c4-c5), sum(c6) FROM tbl_pushdown WHERE c4-c5 > c6 GROUP BY c1, c4, c5, c6 HAVING max(c6) <> avg(c4) + 12 LIMIT 5;
--Testcase 110:
SELECT c1, avg(c4-c5), sum(c6) FROM tbl_pushdown WHERE c4-c5 > c6 GROUP BY c1, c4, c5, c6 HAVING max(c6) <> avg(c4) + 12 LIMIT 5;
--offset only
--Testcase 111:
EXPLAIN VERBOSE SELECT c7, c3, c5, c9 FROM tbl_pushdown OFFSET 1;
--Testcase 112:
SELECT c7, c3, c5, c9 FROM tbl_pushdown OFFSET 1;
--Testcase 113:
EXPLAIN VERBOSE SELECT count(DISTINCT c5), avg(c4), count(c2), sum(c6) FROM tbl_pushdown OFFSET 0;
--Testcase 114:
SELECT count(DISTINCT c5), avg(c4), count(c2), sum(c6) FROM tbl_pushdown OFFSET 0;
--Testcase 115:
EXPLAIN VERBOSE SELECT c1 || 'aef', c2 || c3, c4/12, c5+21, 2*c6 FROM tbl_pushdown OFFSET 3;
--Testcase 116:
SELECT c1 || 'aef', c2 || c3, c4/12, c5+21, 2*c6 FROM tbl_pushdown OFFSET 3;
--Testcase 117:
EXPLAIN VERBOSE SELECT abs(c4/(c6-c5)), c3 || c9, c10, c5+c6 FROM tbl_pushdown OFFSET 1;
--Testcase 118:
SELECT abs(c4/(c6-c5)), c3 || c9, c10, c5+c6 FROM tbl_pushdown OFFSET 1;
--offset, where
--Testcase 119:
EXPLAIN VERBOSE SELECT c3, c9, c4+c5, c4-c5, abs(c6) FROM tbl_pushdown WHERE abs(c4) > abs(c5) OFFSET 1;
--Testcase 120:
SELECT c3, c9, c4+c5, c4-c5, abs(c6) FROM tbl_pushdown WHERE abs(c4) > abs(c5) OFFSET 1;
--Testcase 121:
EXPLAIN VERBOSE SELECT c1 || c2 || c3, c4+c5/c6, c6/c4*2, c7->'events', c8->'teams'->'parent' FROM tbl_pushdown WHERE c7->'events'->>1 = 'update' OFFSET 5;
--Testcase 122:
SELECT c1 || c2 || c3, c4+c5/c6, c6/c4*2, c7->'events', c8->'teams'->'parent' FROM tbl_pushdown WHERE c7->'events'->>1 = 'update' OFFSET 5;
--Testcase 123:
EXPLAIN VERBOSE SELECT c8->'teams'->'parent'->'name', c8->'teams'->'parent'->'id' FROM tbl_pushdown WHERE c8->'teams'->'parent'->>'name' = 'team1' OFFSET 1;
--Testcase 124:
SELECT c8->'teams'->'parent'->'name', c8->'teams'->'parent'->'id' FROM tbl_pushdown WHERE c8->'teams'->'parent'->>'name' = 'team1' OFFSET 1;
--Testcase 125:
EXPLAIN VERBOSE SELECT c7->>'name', c7->>'key', c7->'events'->1, c7->'events'->>2, c7->'values'->>1 FROM tbl_pushdown WHERE c10 = true OFFSET 1;
--Testcase 126:
SELECT c7->>'name', c7->>'key', c7->'events'->1, c7->'events'->>2, c7->'values'->>1 FROM tbl_pushdown WHERE c10 = true OFFSET 1;
--offset, where, group by, having
--Testcase 127:
EXPLAIN VERBOSE SELECT c10, c6+c5-c4, c3 || ' AND ' || c1 FROM tbl_pushdown WHERE c10 != false GROUP BY c1, c3, c4, c5, c6, c10 HAVING c1 IS NOT NULL OFFSET 1;
--Testcase 128:
SELECT c10, c6+c5-c4, c3 || ' AND ' || c1 FROM tbl_pushdown WHERE c10 != false GROUP BY c1, c3, c4, c5, c6, c10 HAVING c1 IS NOT NULL OFFSET 1;
--Testcase 129:
EXPLAIN VERBOSE SELECT c3, c2, c5+c6/c4, c5+12, c6*10 FROM tbl_pushdown WHERE c8->'teams'->'parent'->>'name' = 'team1' GROUP BY c2, c3, c4, c5, c6 HAVING c5 NOT IN (12.12, 132.21, 0.12, -53.2) OFFSET 1;
--Testcase 130:
SELECT c3, c2, c5+c6/c4, c5+12, c6*10 FROM tbl_pushdown WHERE c8->'teams'->'parent'->>'name' = 'team1' GROUP BY c2, c3, c4, c5, c6 HAVING c5 NOT IN (12.12, 132.21, 0.12, -53.2) OFFSET 1;
--Testcase 131:
EXPLAIN VERBOSE SELECT count(c5), avg(c4), c2, c9, c10 FROM tbl_pushdown WHERE NOT c10 = false GROUP BY c2, c9, c10 HAVING c2 IN ('012803192', 'ハンサム', 'ごきげんよう', '~!@#$%^&*()_+') OFFSET 1;
--Testcase 132:
SELECT count(c5), avg(c4), c2, c9, c10 FROM tbl_pushdown WHERE NOT c10 = false GROUP BY c2, c9, c10 HAVING c2 IN ('012803192', 'ハンサム', 'ごきげんよう', '~!@#$%^&*()_+') OFFSET 1;
--Testcase 133:
EXPLAIN VERBOSE SELECT c9 || '@#', c4 + c5/c6, c6, c10 FROM tbl_pushdown WHERE c2 IN ('012803192', 'ハンサム', 'ごきげんよう', '~!@#$%^&*()_+') GROUP BY 1, 2, 3, 4 HAVING NOT c10 = false OFFSET 1;
--Testcase 134:
SELECT c9 || '@#', c4 + c5/c6, c6, c10 FROM tbl_pushdown WHERE c2 IN ('012803192', 'ハンサム', 'ごきげんよう', '~!@#$%^&*()_+') GROUP BY 1, 2, 3, 4 HAVING NOT c10 = false OFFSET 1;
--Testcase 135:
EXPLAIN VERBOSE SELECT _id, c1, c3, sum(c6)+avg(c4)/2 FROM tbl_pushdown WHERE c1 IS NOT NULL GROUP BY _id, c1, c2, c3, c9 HAVING c2 <> '#@O!' ORDER BY 1, 2 OFFSET 1;
--Testcase 136:
SELECT * FROM (
SELECT _id, c1, c3, sum(c6)+avg(c4)/2 FROM tbl_pushdown WHERE c1 IS NOT NULL GROUP BY _id, c1, c2, c3, c9 HAVING c2 <> '#@O!' ORDER BY 1, 2 OFFSET 1
) t ORDER BY 1, 2, 3, 4;
--both limit, offset
--Testcase 137:
EXPLAIN VERBOSE SELECT c7->>'key', c7->>'values', c7->>'events' FROM tbl_pushdown LIMIT 10 OFFSET NULL;
--Testcase 138:
SELECT c7->>'key', c7->>'values', c7->>'events' FROM tbl_pushdown LIMIT 10 OFFSET NULL;
--Testcase 139:
EXPLAIN VERBOSE SELECT c8->'teams'->>'name', c8->'teams'->>'id', c8->'teams'->'privacy' FROM tbl_pushdown LIMIT ALL OFFSET 5;
--Testcase 140:
SELECT c8->'teams'->>'name', c8->'teams'->>'id', c8->'teams'->'privacy' FROM tbl_pushdown LIMIT ALL OFFSET 5;
--Testcase 141:
EXPLAIN VERBOSE SELECT c7->'events'->>1, c7->'events'->>2, c7->'events'->>3, c7->'events'->>4 FROM tbl_pushdown LIMIT NULL OFFSET 1;
--Testcase 142:
SELECT c7->'events'->>1, c7->'events'->>2, c7->'events'->>3, c7->'events'->>4 FROM tbl_pushdown LIMIT NULL OFFSET 1;
--Testcase 143:
EXPLAIN VERBOSE SELECT c4 <> 0, c5 <= 0, (c7->'values'->>1)::float8 >= 0, (c7->'values'->>2)::double precision <> 0 FROM tbl_pushdown LIMIT 5 OFFSET 2;
--Testcase 144:
SELECT c4 <> 0, c5 <= 0, (c7->'values'->>1)::float8 >= 0, (c7->'values'->>2)::double precision <> 0 FROM tbl_pushdown LIMIT 5 OFFSET 2;
--Testcase 145:
EXPLAIN VERBOSE SELECT 'a3r23', 23, 23.23, '!_@*!@*' FROM tbl_pushdown LIMIT 3 OFFSET 1;
--Testcase 146:
SELECT 'a3r23', 23, 23.23, '!_@*!@*' FROM tbl_pushdown LIMIT 3 OFFSET 1;
--Testcase 147:
EXPLAIN VERBOSE SELECT stddev((c7->'values'->>2)::float8), avg(c4-c5), sum(c6/2) FROM tbl_pushdown LIMIT 2 OFFSET 0;
--Testcase 148:
SELECT stddev((c7->'values'->>2)::float8), avg(c4-c5), sum(c6/2) FROM tbl_pushdown LIMIT 2 OFFSET 0;
--Testcase 149:
EXPLAIN VERBOSE SELECT c7#>'{events,1}',c7#>>'{events,1}', c7#>'{events,2}', c7#>>'{events,2}', c7->'events' FROM tbl_pushdown LIMIT 5 OFFSET 1;
--Testcase 150:
SELECT c7#>'{events,1}',c7#>>'{events,1}', c7#>'{events,2}', c7#>>'{events,2}', c7->'events' FROM tbl_pushdown LIMIT 5 OFFSET 1;
--limit, offset, where, group by, having
--Testcase 151:
EXPLAIN VERBOSE SELECT count(c1 || c3), c4, c6 FROM tbl_pushdown WHERE c4-c5 <> 0 GROUP BY c4, c5, c6 HAVING c6 NOT IN (1212, 23, -143, -124) LIMIT 5 OFFSET NULL; 
--Testcase 152:
SELECT count(c1 || c3), c4, c6 FROM tbl_pushdown WHERE c4-c5 <> 0 GROUP BY c4, c5, c6 HAVING c6 NOT IN (1212, 23, -143, -124) LIMIT 5 OFFSET NULL; 
--Testcase 153:
EXPLAIN VERBOSE SELECT c3 || c1, abs(c4/(c6-c5)), c9, c10 FROM tbl_pushdown WHERE c3 NOT IN ('/eew', '/3434', '/svr') GROUP BY c1, c3, c4, c9, c10, c6, c5 HAVING c4 != 0 LIMIT ALL OFFSET NULL;
--Testcase 154:
SELECT * FROM (
SELECT c3 || c1, abs(c4/(c6-c5)), c9, c10 FROM tbl_pushdown WHERE c3 NOT IN ('/eew', '/3434', '/svr') GROUP BY c1, c3, c4, c9, c10, c6, c5 HAVING c4 != 0 LIMIT ALL OFFSET NULL
) t ORDER BY 1, 2, 3, 4;
--Testcase 155:
EXPLAIN VERBOSE SELECT count(*), count(c1), avg(c6)+stddev(c5), sum(c4/12) FROM tbl_pushdown WHERE c4 <> 0 HAVING count(*) >= 0 LIMIT 1 OFFSET 0;
--Testcase 156:
SELECT count(*), count(c1), avg(c6)+stddev(c5), sum(c4/12) FROM tbl_pushdown WHERE c4 <> 0 HAVING count(*) >= 0 LIMIT 1 OFFSET 0;
--Testcase 157:
EXPLAIN VERBOSE SELECT string_agg(c2, ':'), string_agg(c1, ':') FROM tbl_pushdown WHERE c1 != c2 GROUP BY c1, c2 HAVING max(c5) > 0 LIMIT 5 OFFSET 0;
--Testcase 158:
SELECT string_agg(c2, ':'), string_agg(c1, ':') FROM tbl_pushdown WHERE c1 != c2 GROUP BY c1, c2 HAVING max(c5) > 0 LIMIT 5 OFFSET 0;
--Testcase 159:
EXPLAIN VERBOSE SELECT c1 || '_!)#', c2 || c3, c5*c6/c4+23 FROM tbl_pushdown WHERE c4 >= 0 GROUP BY c1, c2, c3, c4, c5, c6 HAVING c5 - c6 > 0 LIMIT 5 OFFSET 1;
--Testcase 160:
SELECT c1 || '_!)#', c2 || c3, c5*c6/c4+23 FROM tbl_pushdown WHERE c4 >= 0 GROUP BY c1, c2, c3, c4, c5, c6 HAVING c5 - c6 > 0 LIMIT 5 OFFSET 1;
--logic operator: select
--Testcase 161:
EXPLAIN VERBOSE SELECT c1 != '#' AND true, c2 = 'ABCSD' OR false, c5 >= 0 AND c6 > 0, c4 <> 0 OR c3 IS NOT NULL FROM tbl_pushdown;
--Testcase 162:
SELECT c1 != '#' AND true, c2 = 'ABCSD' OR false, c5 >= 0 AND c6 > 0, c4 <> 0 OR c3 IS NOT NULL FROM tbl_pushdown;
--Testcase 163:
EXPLAIN VERBOSE SELECT NOT c1 IS NULL, c2 != '#$', NOT c5 <= 0, NOT c4/c6 = 23 FROM tbl_pushdown;
--Testcase 164:
SELECT NOT c1 IS NULL, c2 != '#$', NOT c5 <= 0, NOT c4/c6 = 23 FROM tbl_pushdown;
--Testcase 165:
EXPLAIN VERBOSE SELECT c1 || '#@)!' = '#@)!' OR c2 || c3 != '#@#', NOT c10, c5 <> c6 FROM tbl_pushdown;
--Testcase 166:
SELECT c1 || '#@)!' = '#@)!' OR c2 || c3 != '#@#', NOT c10, c5 <> c6 FROM tbl_pushdown;
--Testcase 167:
EXPLAIN VERBOSE SELECT c1 || c2, c5 > 0, c4 < 0 AND c6 >= 0 FROM tbl_pushdown;
--Testcase 168:
SELECT c1 || c2, c5 > 0, c4 < 0 AND c6 >= 0 FROM tbl_pushdown;
--Testcase 169:
EXPLAIN VERBOSE SELECT c4 <> 0 AND c5 <= 0, (c7->'values'->>1)::float8 >= 0 OR (c7->'values'->>2)::float8 <> 0 FROM tbl_pushdown;
--Testcase 170:
SELECT c4 <> 0 AND c5 <= 0, (c7->'values'->>1)::float8 >= 0 OR (c7->'values'->>2)::float8 <> 0 FROM tbl_pushdown;
--Testcase 171:
EXPLAIN VERBOSE SELECT stddev((c7->'values'->>1)::float8) > 0 AND avg(c4-c5) <> 0, sum(c6/2) <> avg(c4), count(c2) > 2 OR sum(c6) > -1 FROM tbl_pushdown;
--Testcase 172:
SELECT stddev((c7->'values'->>1)::float8) > 0 AND avg(c4-c5) <> 0, sum(c6/2) <> avg(c4), count(c2) > 2 OR sum(c6) > -1 FROM tbl_pushdown;
--logic operator: select, where
--Testcase 173:
EXPLAIN VERBOSE SELECT c9 + '1 hour'::interval > '0001-01-01 00:00:00' OR c1 != '32@', c2 IS NOT NULL AND c3 IS NOT NULL FROM tbl_pushdown WHERE c1 <> c2 AND c3 IS NOT NULL AND c4 >= 0;
--Testcase 174:
SELECT c9 + '1 hour'::interval > '0001-01-01 00:00:00' OR c1 != '32@', c2 IS NOT NULL AND c3 IS NOT NULL FROM tbl_pushdown WHERE c1 <> c2 AND c3 IS NOT NULL AND c4 >= 0;
--Testcase 175:
EXPLAIN VERBOSE SELECT c9 != '0001-01-01 00:00:00' OR c10 = true, c4 + c5/c6 > c6 AND c10 != true FROM tbl_pushdown WHERE c9 != '2000-01-01 00:00:00' OR c4 IN (2, 3, 34);
--Testcase 176:
SELECT c9 != '0001-01-01 00:00:00' OR c10 = true, c4 + c5/c6 > c6 AND c10 != true FROM tbl_pushdown WHERE c9 != '2000-01-01 00:00:00' OR c4 IN (2, 3, 34);
--Testcase 177:
EXPLAIN VERBOSE SELECT NOT c8->'teams'->>'name' = 'NOEJAF203', c8->'teams'->'parent'->>'name' <> 'O)#JA' AND (c8->'teams'->'parent'->>'id')::bigint != 233 FROM tbl_pushdown WHERE c8->'teams'->'parent'->>'name' = 'team1' AND c4 NOT IN (12, 23, -3232);
--Testcase 178:
SELECT NOT c8->'teams'->>'name' = 'NOEJAF203', c8->'teams'->'parent'->>'name' <> 'O)#JA' AND (c8->'teams'->'parent'->>'id')::bigint != 233 FROM tbl_pushdown WHERE c8->'teams'->'parent'->>'name' = 'team1' AND c4 NOT IN (12, 23, -3232);
--logic operator: select, where, having
--Testcase 179:
EXPLAIN VERBOSE SELECT c4-c5, c6 > 0 AND c3 != '#' FROM tbl_pushdown WHERE c1 || c2 != c3 AND c4 >= 0 OR c5 IN (22.12, 233.23) GROUP BY c1, c3, c4, c5, c6 HAVING c1 != '@)@#' OR c1 <> 'AJWEF@QKIP';
--Testcase 180:
SELECT c4-c5, c6 > 0 AND c3 != '#' FROM tbl_pushdown WHERE c1 || c2 != c3 AND c4 >= 0 OR c5 IN (22.12, 233.23) GROUP BY c1, c3, c4, c5, c6 HAVING c1 != '@)@#' OR c1 <> 'AJWEF@QKIP';
--Testcase 181:
EXPLAIN VERBOSE SELECT c1 = 'RA#' OR true, c3, c5+c6, c10 OR true FROM tbl_pushdown WHERE c3 >= '$' OR c1 IS NOT NULL GROUP BY c1, c3, c4, c5, c6, c10 HAVING c4 <= 0 OR c5 <> 0 AND c4-c5 > 0;
--Testcase 182:
SELECT c1 = 'RA#' OR true, c3, c5+c6, c10 OR true FROM tbl_pushdown WHERE c3 >= '$' OR c1 IS NOT NULL GROUP BY c1, c3, c4, c5, c6, c10 HAVING c4 <= 0 OR c5 <> 0 AND c4-c5 > 0;
--Testcase 183:
EXPLAIN VERBOSE SELECT count(c5), avg(c4), c2 >= '@#' OR true, c9, c10 AND true FROM tbl_pushdown WHERE NOT c10 = false OR c4 <> 0 GROUP BY c2, c4, c5, c6, c9, c10 HAVING c2 IN ('012803192', 'ハンサム', 'ごきげんよう', '~!@#$%^&*()_+') OR c5 >= 0 AND c6 <= 0;
--Testcase 184:
SELECT count(c5), avg(c4), c2 >= '@#' OR true, c9, c10 AND true FROM tbl_pushdown WHERE NOT c10 = false OR c4 <> 0 GROUP BY c2, c4, c5, c6, c9, c10 HAVING c2 IN ('012803192', 'ハンサム', 'ごきげんよう', '~!@#$%^&*()_+') OR c5 >= 0 AND c6 <= 0;
--Testcase 185:
EXPLAIN VERBOSE SELECT c7->>'key', c7->>'values', c7->>'events' FROM tbl_pushdown WHERE c6 < c4-c5;
--Testcase 186:
SELECT c7->>'key', c7->>'values', c7->>'events' FROM tbl_pushdown WHERE c6 < c4-c5;
--comparison operator: where
--Testcase 187:
EXPLAIN VERBOSE SELECT c10, c6+c5-c4, c3 || ' OR ' || c1 FROM tbl_pushdown WHERE c10 != false;
--Testcase 188:
SELECT c10, c6+c5-c4, c3 || ' OR ' || c1 FROM tbl_pushdown WHERE c10 != false;
--Testcase 189:
EXPLAIN VERBOSE SELECT c3, c2, c5+c6/c4, c5+12, c6*10 FROM tbl_pushdown WHERE c8->'teams'->'parent'->>'name' = 'team1' OR c5 NOT IN (12.12, 132.21, 0.12, -53.2);
--Testcase 190:
SELECT c3, c2, c5+c6/c4, c5+12, c6*10 FROM tbl_pushdown WHERE c8->'teams'->'parent'->>'name' = 'team1' OR c5 NOT IN (12.12, 132.21, 0.12, -53.2);
--Testcase 191:
EXPLAIN VERBOSE SELECT c7->>'name', c7->>'key', c7->'events'->1, c7->'events'->>2, c7->'values'->>1 FROM tbl_pushdown WHERE c4 != 0;
--Testcase 192:
SELECT c7->>'name', c7->>'key', c7->'events'->1, c7->'events'->>2, c7->'values'->>1 FROM tbl_pushdown WHERE c4 != 0;
--Testcase 193:
EXPLAIN VERBOSE SELECT 2, 23, c1 || '232AF', c4/2, c5+12, c6*12 FROM tbl_pushdown WHERE c4 NOT IN (1221, 2312, 13912);
--Testcase 194:
SELECT 2, 23, c1 || '232AF', c4/2, c5+12, c6*12 FROM tbl_pushdown WHERE c4 NOT IN (1221, 2312, 13912);
--comparison operator: where, having
--Testcase 195:
EXPLAIN VERBOSE SELECT c8->'teams'->'parent'->>'name', c8->'teams'->'parent'->>'id' FROM tbl_pushdown WHERE c8->'teams'->'parent'->>'name' != '@)@!' GROUP BY c4, c8 HAVING c4 <> 0 ;
--Testcase 196:
SELECT c8->'teams'->'parent'->>'name', c8->'teams'->'parent'->>'id' FROM tbl_pushdown WHERE c8->'teams'->'parent'->>'name' != '@)@!' GROUP BY c4, c8 HAVING c4 <> 0 ;
--Testcase 197:
EXPLAIN VERBOSE SELECT c1, avg(c4-c5), sum(c6) FROM tbl_pushdown WHERE c4-c5 > c6 GROUP BY c1, c4, c5, c6 HAVING c6 <> c4 + 12;
--Testcase 198:
SELECT c1, avg(c4-c5), sum(c6) FROM tbl_pushdown WHERE c4-c5 > c6 GROUP BY c1, c4, c5, c6 HAVING c6 <> c4 + 12;
--Testcase 199:
EXPLAIN VERBOSE SELECT c1, c2, c3, c4, c5, c10 FROM tbl_pushdown WHERE c1 || c2 != 'any string match' GROUP BY 1, 2, 3, 4, 5, 6 HAVING c1 != '%$' AND c2 <> 'All';
--Testcase 200:
SELECT c1, c2, c3, c4, c5, c10 FROM tbl_pushdown WHERE c1 || c2 != 'any string match' GROUP BY 1, 2, 3, 4, 5, 6 HAVING c1 != '%$' AND c2 <> 'All';
--Testcase 201:
EXPLAIN VERBOSE SELECT c1, avg(c4), c2, sum(c6), c5 FROM tbl_pushdown WHERE c4-c6 > c4 -c5 GROUP BY c1, c2, c6, c5 HAVING c6 >= 0;
--Testcase 202:
SELECT c1, avg(c4), c2, sum(c6), c5 FROM tbl_pushdown WHERE c4-c6 > c4 -c5 GROUP BY c1, c2, c6, c5 HAVING c6 >= 0;
--Testcase 203:
EXPLAIN VERBOSE SELECT stddev((c7->'values'->>1)::float8), avg((c7->'values'->>2)::float8), avg((c7->'values'->>1)::float8), stddev_samp((c7->'values'->>2)::float8) FROM tbl_pushdown WHERE c4-c6 > c4-c5 GROUP BY c6 HAVING c6 >= 0 LIMIT 5;
--Testcase 204:
SELECT * FROM (
SELECT stddev((c7->'values'->>1)::float8), avg((c7->'values'->>2)::float8), avg((c7->'values'->>1)::float8), stddev_samp((c7->'values'->>2)::float8) FROM tbl_pushdown WHERE c4-c6 > c4-c5 GROUP BY c6 HAVING c6 >= 0 LIMIT 5
) t ORDER BY 1, 2, 3, 4;
--Testcase 205:
EXPLAIN VERBOSE SELECT '!@_!+', 23, c5+c6/c4, c5+12, abs(c4/(c6-c5)) FROM tbl_pushdown WHERE c4/c6 >= 0 OR c5*c6/c4 < 1000 GROUP BY c5, c4, c6, c1 HAVING c5 <> 0 AND c1 NOT IN ('a3awer', '2323ASE') LIMIT 5 OFFSET 1;
--Testcase 206:
SELECT '!@_!+', 23, c5+c6/c4, c5+12, abs(c4/(c6-c5)) FROM tbl_pushdown WHERE c4/c6 >= 0 OR c5*c6/c4 < 1000 GROUP BY c5, c4, c6, c1 HAVING c5 <> 0 AND c1 NOT IN ('a3awer', '2323ASE') LIMIT 5 OFFSET 1;

DROP FOREIGN TABLE tbl_pushdown;
DROP USER MAPPING FOR public SERVER mongo_server;
DROP SERVER mongo_server;
DROP EXTENSION mongo_fdw CASCADE;
