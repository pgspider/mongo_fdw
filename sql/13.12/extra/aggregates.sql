-- Before running this file User must create database aggregates_regress
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


--Testcase 4:
CREATE FOREIGN TABLE onek (
  _id		int4,
  unique1   int4,
  unique2   int4,
  two       int4,
  four      int4,
  ten       int4,
  twenty    int4,
  hundred   int4,
  thousand  int4,
  twothousand int4,
  fivethous int4,
  tenthous  int4,
  odd       int4,
  even      int4,
  stringu1  name,
  stringu2  name,
  string4   name
) SERVER mongo_server OPTIONS (database 'aggregates_regress', collection 'onek');

--Testcase 5:
CREATE FOREIGN TABLE aggtest (
  _id 		int4,
  a         int2,
  b         float4
) SERVER mongo_server OPTIONS (database 'aggregates_regress', collection 'aggtest');

--Testcase 6:
CREATE FOREIGN TABLE student (
  _id 		int4,
  name      text,
  age       int4,
  location  point,
  gpa       float8
) SERVER mongo_server OPTIONS (database 'aggregates_regress', collection 'student');

--Testcase 7:
CREATE FOREIGN TABLE tenk1 (
  _id		int4,
  unique1   int4,
  unique2   int4,
  two       int4,
  four      int4,
  ten       int4,
  twenty    int4,
  hundred   int4,
  thousand  int4,
  twothousand int4,
  fivethous int4,
  tenthous  int4,
  odd       int4,
  even      int4,
  stringu1  name,
  stringu2  name,
  string4   name
) SERVER mongo_server OPTIONS (database 'aggregates_regress', collection 'tenk');

--Testcase 8:
CREATE FOREIGN TABLE INT8_TBL (
  _id 		int4,
  q1        int8,
  q2        int8
) SERVER mongo_server OPTIONS (database 'aggregates_regress', collection 'int8_tbl');

--Testcase 9:
CREATE FOREIGN TABLE INT4_TBL (_id int4, f1 int4)
 SERVER mongo_server OPTIONS (database 'aggregates_regress', collection 'int4_tbl');

-- avoid bit-exact output here because operations may not be bit-exact.
--Testcase 10:
SET extra_float_digits = 0;

--Testcase 11:
SELECT avg(four) AS avg_1 FROM onek;

--Testcase 12:
SELECT avg(a) AS avg_32 FROM aggtest WHERE a < 100;

-- In 7.1, avg(float4) is computed using float8 arithmetic.
-- Round the result to 3 digits to avoid platform-specific results.

--Testcase 13:
EXPLAIN VERBOSE SELECT avg(b)::numeric(10,3) AS avg_107_943 FROM aggtest;
--Testcase 14:
SELECT avg(b)::numeric(10,3) AS avg_107_943 FROM aggtest;

--Testcase 15:
EXPLAIN VERBOSE SELECT sum(four) AS sum_1500 FROM onek;
--Testcase 16:
SELECT sum(four) AS sum_1500 FROM onek;
--Testcase 17:
EXPLAIN VERBOSE SELECT sum(a) AS sum_198 FROM aggtest;
--Testcase 18:
SELECT sum(a) AS sum_198 FROM aggtest;
--Testcase 19:
EXPLAIN VERBOSE SELECT sum(b) AS avg_431_773 FROM aggtest;
--Testcase 20:
SELECT sum(b) AS avg_431_773 FROM aggtest;

--Testcase 21:
EXPLAIN VERBOSE SELECT max(four) AS max_3 FROM onek;
--Testcase 22:
SELECT max(four) AS max_3 FROM onek;
--Testcase 23:
EXPLAIN VERBOSE SELECT max(a) AS max_100 FROM aggtest;
--Testcase 24:
SELECT max(a) AS max_100 FROM aggtest;
--Testcase 25:
EXPLAIN VERBOSE SELECT max(aggtest.b) AS max_324_78 FROM aggtest;
--Testcase 26:
SELECT max(aggtest.b) AS max_324_78 FROM aggtest;

--Testcase 27:
EXPLAIN VERBOSE SELECT stddev_pop(b) FROM aggtest;
--Testcase 28:
SELECT stddev_pop(b) FROM aggtest;
--Testcase 29:
EXPLAIN VERBOSE SELECT stddev_samp(b) FROM aggtest;
--Testcase 30:
SELECT stddev_samp(b) FROM aggtest;
--Testcase 31:
EXPLAIN VERBOSE SELECT var_pop(b) FROM aggtest;
--Testcase 32:
SELECT var_pop(b) FROM aggtest;
--Testcase 33:
EXPLAIN VERBOSE SELECT var_samp(b) FROM aggtest;
--Testcase 34:
SELECT var_samp(b) FROM aggtest;

--Testcase 35:
EXPLAIN VERBOSE SELECT stddev_pop(b::numeric) FROM aggtest;
--Testcase 36:
SELECT stddev_pop(b::numeric) FROM aggtest;
--Testcase 37:
EXPLAIN VERBOSE SELECT stddev_samp(b::numeric) FROM aggtest;
--Testcase 38:
SELECT stddev_samp(b::numeric) FROM aggtest;
--Testcase 39:
EXPLAIN VERBOSE SELECT var_pop(b::numeric) FROM aggtest;
--Testcase 40:
SELECT var_pop(b::numeric) FROM aggtest;
--Testcase 41:
EXPLAIN VERBOSE SELECT var_samp(b::numeric) FROM aggtest;
--Testcase 42:
SELECT var_samp(b::numeric) FROM aggtest;

-- SQL2003 binary aggregates
--Testcase 43:
EXPLAIN VERBOSE SELECT regr_count(b, a) FROM aggtest;
--Testcase 44:
SELECT regr_count(b, a) FROM aggtest;
--Testcase 45:
EXPLAIN VERBOSE SELECT regr_sxx(b, a) FROM aggtest;
--Testcase 46:
SELECT regr_sxx(b, a) FROM aggtest;
--Testcase 47:
EXPLAIN VERBOSE SELECT regr_syy(b, a) FROM aggtest;
--Testcase 48:
SELECT regr_syy(b, a) FROM aggtest;
--Testcase 49:
EXPLAIN VERBOSE SELECT regr_sxy(b, a) FROM aggtest;
--Testcase 50:
SELECT regr_sxy(b, a) FROM aggtest;
--Testcase 51:
EXPLAIN VERBOSE SELECT regr_avgx(b, a), regr_avgy(b, a) FROM aggtest;
--Testcase 52:
SELECT regr_avgx(b, a), regr_avgy(b, a) FROM aggtest;
--Testcase 53:
EXPLAIN VERBOSE SELECT regr_r2(b, a) FROM aggtest;
--Testcase 54:
SELECT regr_r2(b, a) FROM aggtest;
--Testcase 55:
EXPLAIN VERBOSE SELECT regr_slope(b, a), regr_intercept(b, a) FROM aggtest;
--Testcase 56:
SELECT regr_slope(b, a), regr_intercept(b, a) FROM aggtest;
--Testcase 57:
EXPLAIN VERBOSE SELECT covar_pop(b, a), covar_samp(b, a) FROM aggtest;
--Testcase 58:
SELECT covar_pop(b, a), covar_samp(b, a) FROM aggtest;
--Testcase 59:
EXPLAIN VERBOSE SELECT corr(b, a) FROM aggtest;
--Testcase 60:
SELECT corr(b, a) FROM aggtest;

-- test accum and combine functions directly
--Testcase 61:
CREATE FOREIGN TABLE regr_test(_id int4, x float8, y float8)
 SERVER mongo_server OPTIONS (database 'aggregates_regress', collection 'regr_test');

--Testcase 62:
EXPLAIN VERBOSE SELECT count(*), sum(x), regr_sxx(y,x), sum(y),regr_syy(y,x), regr_sxy(y,x)
FROM regr_test WHERE x IN (10,20,30,80);
--Testcase 63:
SELECT count(*), sum(x), regr_sxx(y,x), sum(y),regr_syy(y,x), regr_sxy(y,x)
FROM regr_test WHERE x IN (10,20,30,80);
--Testcase 64:
EXPLAIN VERBOSE SELECT count(*), sum(x), regr_sxx(y,x), sum(y),regr_syy(y,x), regr_sxy(y,x)
FROM regr_test;
--Testcase 65:
SELECT count(*), sum(x), regr_sxx(y,x), sum(y),regr_syy(y,x), regr_sxy(y,x)
FROM regr_test;
--Testcase 66:
EXPLAIN VERBOSE SELECT count(*), sum(x), regr_sxx(y,x), sum(y),regr_syy(y,x), regr_sxy(y,x)
FROM regr_test WHERE x IN (10,20,30);
--Testcase 67:
SELECT count(*), sum(x), regr_sxx(y,x), sum(y),regr_syy(y,x), regr_sxy(y,x)
FROM regr_test WHERE x IN (10,20,30);
--Testcase 68:
EXPLAIN VERBOSE SELECT count(*), sum(x), regr_sxx(y,x), sum(y),regr_syy(y,x), regr_sxy(y,x)
FROM regr_test WHERE x IN (80,100);
--Testcase 69:
SELECT count(*), sum(x), regr_sxx(y,x), sum(y),regr_syy(y,x), regr_sxy(y,x)
FROM regr_test WHERE x IN (80,100);

--Testcase 70:
DROP TABLE regr_test;

-- test count, distinct
--Testcase 71:
EXPLAIN VERBOSE SELECT count(four) AS cnt_1000 FROM onek;
--Testcase 72:
SELECT count(four) AS cnt_1000 FROM onek;
--Testcase 73:
EXPLAIN VERBOSE SELECT count(DISTINCT four) AS cnt_4 FROM onek;
--Testcase 74:
SELECT count(DISTINCT four) AS cnt_4 FROM onek;

--Testcase 75:
EXPLAIN VERBOSE select ten, count(*), sum(four) from onek
group by ten order by ten;
--Testcase 76:
select ten, count(*), sum(four) from onek
group by ten order by ten;

--Testcase 77:
EXPLAIN VERBOSE select ten, count(four), sum(DISTINCT four) from onek
group by ten order by ten;
--Testcase 78:
select ten, count(four), sum(DISTINCT four) from onek
group by ten order by ten;

-- test for outer-level aggregates

-- this should work
--Testcase 79:
EXPLAIN VERBOSE select ten, sum(distinct four) from onek a
group by ten
having exists (select 1 from onek b where sum(distinct a.four) = b.four);
--Testcase 80:
select ten, sum(distinct four) from onek a
group by ten
having exists (select 1 from onek b where sum(distinct a.four) = b.four);

-- this should fail because subquery has an agg of its own in WHERE
--Testcase 81:
EXPLAIN VERBOSE select ten, sum(distinct four) from onek a
group by ten
having exists (select 1 from onek b
               where sum(distinct a.four + b.four) = b.four);
--Testcase 82:
select ten, sum(distinct four) from onek a
group by ten
having exists (select 1 from onek b
               where sum(distinct a.four + b.four) = b.four);

-- Test handling of sublinks within outer-level aggregates.
-- Per bug report from Daniel Grace.
-- select
--   (select max((select i.unique2 from tenk1 i where i.unique1 = o.unique1)))
-- from tenk1 o;   -----> BUG

-- Basic cases
--Testcase 83:
explain (costs off)
  select min(unique1) from tenk1;
--Testcase 84:
select min(unique1) from tenk1;
--Testcase 85:
explain (costs off)
  select max(unique1) from tenk1;
--Testcase 86:
select max(unique1) from tenk1;
--Testcase 87:
explain (costs off)
  select max(unique1) from tenk1 where unique1 < 42;
--Testcase 88:
select max(unique1) from tenk1 where unique1 < 42;
--Testcase 89:
explain (costs off)
  select max(unique1) from tenk1 where unique1 > 42;
--Testcase 90:
select max(unique1) from tenk1 where unique1 > 42;

-- the planner may choose a generic aggregate here if parallel query is
-- enabled, since that plan will be parallel safe and the "optimized"
-- plan, which has almost identical cost, will not be.  we want to test
-- the optimized plan, so temporarily disable parallel query.
begin;
--Testcase 91:
set local max_parallel_workers_per_gather = 0;
--Testcase 92:
explain (costs off)
  select max(unique1) from tenk1 where unique1 > 42000;
--Testcase 93:
select max(unique1) from tenk1 where unique1 > 42000;
rollback;

-- multi-column index (uses tenk1_thous_tenthous)
--Testcase 94:
explain (costs off)
  select max(tenthous) from tenk1 where thousand = 33;
--Testcase 95:
select max(tenthous) from tenk1 where thousand = 33;
--Testcase 96:
explain (costs off)
  select min(tenthous) from tenk1 where thousand = 33;
--Testcase 97:
select min(tenthous) from tenk1 where thousand = 33;

-- check parameter propagation into an indexscan subquery
--Testcase 98:
explain (costs off)
  select f1, (select min(unique1) from tenk1 where unique1 > f1) AS gt
    from int4_tbl;
--Testcase 99:
select f1, (select min(unique1) from tenk1 where unique1 > f1) AS gt
  from int4_tbl;

-- check some cases that were handled incorrectly in 8.3.0
--Testcase 100:
explain (costs off)
  select distinct max(unique2) from tenk1;
--Testcase 101:
select distinct max(unique2) from tenk1;
--Testcase 102:
explain (costs off)
  select max(unique2) from tenk1 order by 1;
--Testcase 103:
select max(unique2) from tenk1 order by 1;
--Testcase 104:
explain (costs off)
  select max(unique2) from tenk1 order by max(unique2);
--Testcase 105:
select max(unique2) from tenk1 order by max(unique2);
--Testcase 106:
explain (costs off)
  select max(unique2) from tenk1 order by max(unique2)+1;
--Testcase 107:
select max(unique2) from tenk1 order by max(unique2)+1;
--Testcase 108:
explain (costs off)
  select max(unique2), generate_series(1,3) as g from tenk1 order by g desc;
--Testcase 109:
select max(unique2), generate_series(1,3) as g from tenk1 order by g desc;

-- interesting corner case: constant gets optimized into a seqscan
--Testcase 110:
explain (costs off)
  select max(100) from tenk1;
--Testcase 111:
select max(100) from tenk1;

-- Test combinations of DISTINCT and/or ORDER BY

--Testcase 112:
EXPLAIN VERBOSE select array_agg(q1 order by q2)
  from INT8_TBL;
--Testcase 113:
select array_agg(q1 order by q2)
  from INT8_TBL;
--Testcase 114:
EXPLAIN VERBOSE select array_agg(q1 order by q1)
  from INT8_TBL;
--Testcase 115:
select array_agg(q1 order by q1)
  from INT8_TBL;
--Testcase 116:
EXPLAIN VERBOSE select array_agg(q1 order by q1 desc)
  from INT8_TBL;
--Testcase 117:
select array_agg(q1 order by q1 desc)
  from INT8_TBL;
--Testcase 118:
EXPLAIN VERBOSE select array_agg(q2 order by q1 desc)
  from INT8_TBL;
--Testcase 119:
select array_agg(q2 order by q1 desc)
  from INT8_TBL;

--Testcase 120:
EXPLAIN VERBOSE select array_agg(distinct f1)
  from INT4_TBL;
--Testcase 121:
select array_agg(distinct f1)
  from INT4_TBL;
--Testcase 122:
EXPLAIN VERBOSE select array_agg(distinct f1 order by f1)
  from INT4_TBL;
--Testcase 123:
select array_agg(distinct f1 order by f1)
  from INT4_TBL;
--Testcase 124:
EXPLAIN VERBOSE select array_agg(distinct f1 order by f1 desc)
  from INT4_TBL;
--Testcase 125:
select array_agg(distinct f1 order by f1 desc)
  from INT4_TBL;
--Testcase 126:
EXPLAIN VERBOSE select array_agg(distinct f1 order by f1 desc nulls last)
  from INT4_TBL;
--Testcase 127:
select array_agg(distinct f1 order by f1 desc nulls last)
  from INT4_TBL;

-- string_agg tests
--Testcase 128:
CREATE FOREIGN TABLE string_agg1(_id int4, a1 text, a2 text)
 SERVER mongo_server OPTIONS (database 'aggregates_regress', collection 'string_agg1');
--Testcase 129:
CREATE FOREIGN TABLE string_agg2(_id int4, a1 text, a2 text)
 SERVER mongo_server OPTIONS (database 'aggregates_regress', collection 'string_agg2');
--Testcase 130:
CREATE FOREIGN TABLE string_agg3(_id int4, a1 text, a2 text)
 SERVER mongo_server OPTIONS (database 'aggregates_regress', collection 'string_agg3');
--Testcase 131:
CREATE FOREIGN TABLE string_agg4(_id int4, a1 text, a2 text)
 SERVER mongo_server OPTIONS (database 'aggregates_regress', collection 'string_agg4');

--Testcase 132:
EXPLAIN VERBOSE select string_agg(a1,',') from string_agg1;
--Testcase 133:
select string_agg(a1,',') from string_agg1;
--Testcase 134:
EXPLAIN VERBOSE select string_agg(a1,',') from string_agg2;
--Testcase 135:
select string_agg(a1,',') from string_agg2;
--Testcase 136:
EXPLAIN VERBOSE select string_agg(a1,'AB') from string_agg3;
--Testcase 137:
select string_agg(a1,'AB') from string_agg3;
--Testcase 138:
EXPLAIN VERBOSE select string_agg(a1,',') from string_agg4;
--Testcase 139:
select string_agg(a1,',') from string_agg4;

-- check some implicit casting cases, as per bug #5564

--Testcase 140:
CREATE FOREIGN TABLE VARCHAR_TBL (_id int4, f1 varchar(4))
 SERVER mongo_server OPTIONS (database 'aggregates_regress', collection 'varchar_tbl');

--Testcase 141:
EXPLAIN VERBOSE select string_agg(distinct f1, ',' order by f1) from varchar_tbl;  -- ok
--Testcase 142:
select string_agg(distinct f1, ',' order by f1) from varchar_tbl;  -- ok
--Testcase 143:
EXPLAIN VERBOSE select string_agg(distinct f1::text, ',' order by f1) from varchar_tbl;  -- not ok
--Testcase 144:
select string_agg(distinct f1::text, ',' order by f1) from varchar_tbl;  -- not ok
--Testcase 145:
EXPLAIN VERBOSE select string_agg(distinct f1, ',' order by f1::text) from varchar_tbl;  -- not ok
--Testcase 146:
select string_agg(distinct f1, ',' order by f1::text) from varchar_tbl;  -- not ok
--Testcase 147:
EXPLAIN VERBOSE select string_agg(distinct f1::text, ',' order by f1::text) from varchar_tbl;  -- ok
--Testcase 148:
select string_agg(distinct f1::text, ',' order by f1::text) from varchar_tbl;  -- ok

-- FILTER tests

--Testcase 149:
EXPLAIN VERBOSE select min(unique1) filter (where unique1 > 100) from tenk1;
--Testcase 150:
select min(unique1) filter (where unique1 > 100) from tenk1;

--Testcase 151:
EXPLAIN VERBOSE select sum(1/ten) filter (where ten > 0) from tenk1;
--Testcase 152:
select sum(1/ten) filter (where ten > 0) from tenk1;

--Testcase 153:
EXPLAIN VERBOSE select ten, sum(distinct four) filter (where four::text ~ '123') from onek a
group by ten;
--Testcase 154:
select ten, sum(distinct four) filter (where four::text ~ '123') from onek a
group by ten;

--Testcase 155:
EXPLAIN VERBOSE select ten, sum(distinct four) filter (where four > 10) from onek a
group by ten
having exists (select 1 from onek b where sum(distinct a.four) = b.four);
--Testcase 156:
select ten, sum(distinct four) filter (where four > 10) from onek a
group by ten
having exists (select 1 from onek b where sum(distinct a.four) = b.four);

-- subquery in FILTER clause (PostgreSQL extension)
--Testcase 157:
EXPLAIN VERBOSE select sum(unique1) FILTER (WHERE
  unique1 IN (SELECT unique1 FROM onek where unique1 < 100)) FROM tenk1;
--Testcase 158:
select sum(unique1) FILTER (WHERE
  unique1 IN (SELECT unique1 FROM onek where unique1 < 100)) FROM tenk1;

-- test coverage for aggregate combine/serial/deserial functions
BEGIN ISOLATION LEVEL REPEATABLE READ;

--Testcase 159:
SET parallel_setup_cost = 0;
--Testcase 160:
SET parallel_tuple_cost = 0;
--Testcase 161:
SET min_parallel_table_scan_size = 0;
--Testcase 162:
SET max_parallel_workers_per_gather = 4;
--Testcase 163:
SET parallel_leader_participation = off;
--Testcase 164:
SET enable_indexonlyscan = off;

-- variance(int4) covers numeric_poly_combine
-- sum(int8) covers int8_avg_combine
-- regr_count(float8, float8) covers int8inc_float8_float8 and aggregates with > 1 arg
--Testcase 165:
EXPLAIN (COSTS OFF, VERBOSE)
SELECT variance(unique1::int4), sum(unique1::int8), regr_count(unique1::float8, unique1::float8)
FROM (SELECT * FROM tenk1
      UNION ALL SELECT * FROM tenk1
      UNION ALL SELECT * FROM tenk1
      UNION ALL SELECT * FROM tenk1) u;

--Testcase 166:
SELECT variance(unique1::int4), sum(unique1::int8), regr_count(unique1::float8, unique1::float8)
FROM (SELECT * FROM tenk1
      UNION ALL SELECT * FROM tenk1
      UNION ALL SELECT * FROM tenk1
      UNION ALL SELECT * FROM tenk1) u;

-- variance(int8) covers numeric_combine
-- avg(numeric) covers numeric_avg_combine
--Testcase 167:
EXPLAIN (COSTS OFF, VERBOSE)
SELECT variance(unique1::int8), avg(unique1::numeric)
FROM (SELECT * FROM tenk1
      UNION ALL SELECT * FROM tenk1
      UNION ALL SELECT * FROM tenk1
      UNION ALL SELECT * FROM tenk1) u;

--Testcase 168:
SELECT variance(unique1::int8), avg(unique1::numeric)
FROM (SELECT * FROM tenk1
      UNION ALL SELECT * FROM tenk1
      UNION ALL SELECT * FROM tenk1
      UNION ALL SELECT * FROM tenk1) u;

ROLLBACK;

-- Make sure that generation of HashAggregate for uniqification purposes
-- does not lead to array overflow due to unexpected duplicate hash keys
-- see CAFeeJoKKu0u+A_A9R9316djW-YW3-+Gtgvy3ju655qRHR3jtdA@mail.gmail.com
--Testcase 169:
explain (costs off)
  select 1 from tenk1
   where (hundred, thousand) in (select twothousand, twothousand from onek);

--
-- Hash Aggregation Spill tests
--

--Testcase 170:
set enable_sort=false;
--Testcase 171:
set work_mem='64kB';

--Testcase 172:
select unique1, count(*), sum(twothousand) from tenk1
group by unique1
having sum(fivethous) > 4975
order by sum(twothousand);

--Testcase 173:
set work_mem to default;
--Testcase 174:
set enable_sort to default;

--Testcase 175:
DROP USER MAPPING FOR public SERVER mongo_server;
--Testcase 176:
DROP EXTENSION mongo_fdw CASCADE;
