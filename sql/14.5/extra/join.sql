-- Before running this file User must create database join_regress
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

--
-- JOIN
-- Test JOIN clauses
--

--Testcase 4:
CREATE FOREIGN TABLE J1_TBL (_id int4, i int, j int, t text)
 SERVER mongo_server OPTIONS (database 'join_regress', collection 'J1_TBL');
--Testcase 5:
CREATE FOREIGN TABLE J2_TBL (_id int4, i int, k int)
 SERVER mongo_server OPTIONS (database 'join_regress', collection 'J2_TBL');

-- useful in some tests below
--Testcase 6:
create temp table onerow();
--Testcase 7:
insert into onerow default values;
analyze onerow;

--
-- CORRELATION NAMES
-- Make sure that table/column aliases are supported
-- before diving into more complex join syntax.
--

--Testcase 8:
EXPLAIN VERBOSE SELECT *
  FROM J1_TBL AS tx;
--Testcase 9:
SELECT *
  FROM J1_TBL AS tx;

--Testcase 10:
EXPLAIN VERBOSE SELECT *
  FROM J1_TBL tx;
--Testcase 11:
SELECT *
  FROM J1_TBL tx;

--Testcase 12:
EXPLAIN VERBOSE SELECT *
  FROM J1_TBL AS t1 (id, a, b, c);
--Testcase 13:
SELECT *
  FROM J1_TBL AS t1 (id, a, b, c);

--Testcase 14:
EXPLAIN VERBOSE SELECT *
  FROM J1_TBL t1 (id, a, b, c);
--Testcase 15:
SELECT *
  FROM J1_TBL t1 (id, a, b, c);

--Testcase 16:
EXPLAIN VERBOSE SELECT *
  FROM J1_TBL t1 (id1, a, b, c), J2_TBL t2 (id2, d, e);
--Testcase 17:
SELECT *
  FROM J1_TBL t1 (id1, a, b, c), J2_TBL t2 (id2, d, e);

--Testcase 18:
EXPLAIN VERBOSE SELECT t1.a, t2.e
  FROM J1_TBL t1 (id1, a, b, c), J2_TBL t2 (id2, d, e)
  WHERE t1.a = t2.d;
--Testcase 19:
SELECT t1.a, t2.e
  FROM J1_TBL t1 (id1, a, b, c), J2_TBL t2 (id2, d, e)
  WHERE t1.a = t2.d;


--
-- CROSS JOIN
-- Qualifications are not allowed on cross joins,
-- which degenerate into a standard unqualified inner join.
--

--Testcase 20:
EXPLAIN VERBOSE SELECT *
  FROM J1_TBL CROSS JOIN J2_TBL;
--Testcase 21:
SELECT *
  FROM J1_TBL CROSS JOIN J2_TBL;

-- ambiguous column
--Testcase 22:
EXPLAIN VERBOSE SELECT i, k, t
  FROM J1_TBL CROSS JOIN J2_TBL;
--Testcase 23:
SELECT i, k, t
  FROM J1_TBL CROSS JOIN J2_TBL;

-- resolve previous ambiguity by specifying the table name
--Testcase 24:
EXPLAIN VERBOSE SELECT t1.i, k, t
  FROM J1_TBL t1 CROSS JOIN J2_TBL t2;
--Testcase 25:
SELECT t1.i, k, t
  FROM J1_TBL t1 CROSS JOIN J2_TBL t2;

--Testcase 26:
EXPLAIN VERBOSE SELECT ii, tt, kk
  FROM (J1_TBL CROSS JOIN J2_TBL)
    AS tx (idd1, ii, jj, tt, idd2, ii2, kk);
--Testcase 27:
SELECT ii, tt, kk
  FROM (J1_TBL CROSS JOIN J2_TBL)
    AS tx (idd1, ii, jj, tt, idd2, ii2, kk);

--Testcase 28:
EXPLAIN VERBOSE SELECT tx.ii, tx.jj, tx.kk
  FROM (J1_TBL t1 (id1, a, b, c) CROSS JOIN J2_TBL t2 (id2, d, e))
    AS tx (idd1, ii, jj, tt, idd2, ii2, kk);
--Testcase 29:
SELECT tx.ii, tx.jj, tx.kk
  FROM (J1_TBL t1 (id1, a, b, c) CROSS JOIN J2_TBL t2 (id2, d, e))
    AS tx (idd1, ii, jj, tt, idd2, ii2, kk);

--Testcase 30:
EXPLAIN VERBOSE SELECT *
  FROM J1_TBL CROSS JOIN J2_TBL a CROSS JOIN J2_TBL b;
--Testcase 31:
SELECT *
  FROM J1_TBL CROSS JOIN J2_TBL a CROSS JOIN J2_TBL b;


--
--
-- Inner joins (equi-joins)
--
--

--
-- Inner joins (equi-joins) with USING clause
-- The USING syntax changes the shape of the resulting table
-- by including a column in the USING clause only once in the result.
--

-- Inner equi-join on specified column
--Testcase 32:
EXPLAIN VERBOSE SELECT *
  FROM J1_TBL INNER JOIN J2_TBL USING (i);
--Testcase 33:
SELECT *
  FROM J1_TBL INNER JOIN J2_TBL USING (i);

-- Same as above, slightly different syntax
--Testcase 34:
EXPLAIN VERBOSE SELECT *
  FROM J1_TBL JOIN J2_TBL USING (i);
--Testcase 35:
SELECT *
  FROM J1_TBL JOIN J2_TBL USING (i);

--Testcase 36:
EXPLAIN VERBOSE SELECT *
  FROM J1_TBL t1 (id1, a, b, c) JOIN J2_TBL t2 (id2, a, d) USING (a)
  ORDER BY a, d;
--Testcase 37:
SELECT *
  FROM J1_TBL t1 (id1, a, b, c) JOIN J2_TBL t2 (id2, a, d) USING (a)
  ORDER BY a, d;

--Testcase 38:
EXPLAIN VERBOSE SELECT *
  FROM J1_TBL t1 (id1, a, b, c) JOIN J2_TBL t2 (id2, a, b) USING (b)
  ORDER BY b, t1.a;
--Testcase 39:
SELECT *
  FROM J1_TBL t1 (id1, a, b, c) JOIN J2_TBL t2 (id2, a, b) USING (b)
  ORDER BY b, t1.a;

-- test join using aliases
SELECT * FROM J1_TBL JOIN J2_TBL USING (i) WHERE J1_TBL.t = 'one';  -- ok
SELECT * FROM J1_TBL JOIN J2_TBL USING (i) AS x WHERE J1_TBL.t = 'one';  -- ok
SELECT * FROM (J1_TBL JOIN J2_TBL USING (i)) AS x WHERE J1_TBL.t = 'one';  -- error
SELECT * FROM J1_TBL JOIN J2_TBL USING (i) AS x WHERE x.i = 1;  -- ok
SELECT * FROM J1_TBL JOIN J2_TBL USING (i) AS x WHERE x.t = 'one';  -- error
SELECT * FROM (J1_TBL JOIN J2_TBL USING (i) AS x) AS xx WHERE x.i = 1;  -- error (XXX could use better hint)
SELECT * FROM J1_TBL a1 JOIN J2_TBL a2 USING (i) AS a1;  -- error
SELECT x.* FROM J1_TBL JOIN J2_TBL USING (i) AS x WHERE J1_TBL.t = 'one';
SELECT ROW(x.*) FROM J1_TBL JOIN J2_TBL USING (i) AS x WHERE J1_TBL.t = 'one';
SELECT row_to_json(x.*) FROM J1_TBL JOIN J2_TBL USING (i) AS x WHERE J1_TBL.t = 'one';

--
-- NATURAL JOIN
-- Inner equi-join on all columns with the same name
--

--Testcase 40:
EXPLAIN VERBOSE SELECT *
  FROM J1_TBL NATURAL JOIN J2_TBL;
--Testcase 41:
SELECT *
  FROM J1_TBL NATURAL JOIN J2_TBL;

--Testcase 42:
EXPLAIN VERBOSE SELECT *
  FROM J1_TBL t1 (id1, a, b, c) NATURAL JOIN J2_TBL t2 (id2, a, d);
--Testcase 43:
SELECT *
  FROM J1_TBL t1 (id1, a, b, c) NATURAL JOIN J2_TBL t2 (id2, a, d);

--Testcase 44:
EXPLAIN VERBOSE SELECT *
  FROM J1_TBL t1 (id1, a, b, c) NATURAL JOIN J2_TBL t2 (id2, d, a);
--Testcase 45:
SELECT *
  FROM J1_TBL t1 (id1, a, b, c) NATURAL JOIN J2_TBL t2 (id2, d, a);

-- mismatch number of columns
-- currently, Postgres will fill in with underlying names
--Testcase 46:
EXPLAIN VERBOSE SELECT *
  FROM J1_TBL t1 (a, b) NATURAL JOIN J2_TBL t2 (a);

--Testcase 47:
SELECT *
  FROM J1_TBL t1 (a, b) NATURAL JOIN J2_TBL t2 (a);


--
-- Inner joins (equi-joins)
--

--Testcase 48:
EXPLAIN VERBOSE SELECT *
  FROM J1_TBL JOIN J2_TBL ON (J1_TBL.i = J2_TBL.i);
--Testcase 49:
SELECT *
  FROM J1_TBL JOIN J2_TBL ON (J1_TBL.i = J2_TBL.i);

--Testcase 50:
EXPLAIN VERBOSE SELECT *
  FROM J1_TBL JOIN J2_TBL ON (J1_TBL.i = J2_TBL.k);
--Testcase 51:
SELECT *
  FROM J1_TBL JOIN J2_TBL ON (J1_TBL.i = J2_TBL.k);


--
-- Non-equi-joins
--

--Testcase 52:
EXPLAIN VERBOSE SELECT *
  FROM J1_TBL JOIN J2_TBL ON (J1_TBL.i <= J2_TBL.k);

--Testcase 53:
SELECT *
  FROM J1_TBL JOIN J2_TBL ON (J1_TBL.i <= J2_TBL.k);


--
-- Outer joins
-- Note that OUTER is a noise word
--

--Testcase 54:
EXPLAIN VERBOSE SELECT *
  FROM J1_TBL LEFT OUTER JOIN J2_TBL USING (i)
  ORDER BY i, k, t;
--Testcase 55:
SELECT *
  FROM J1_TBL LEFT OUTER JOIN J2_TBL USING (i)
  ORDER BY i, k, t;

--Testcase 56:
EXPLAIN VERBOSE SELECT *
  FROM J1_TBL LEFT JOIN J2_TBL USING (i)
  ORDER BY i, k, t;
--Testcase 57:
SELECT *
  FROM J1_TBL LEFT JOIN J2_TBL USING (i)
  ORDER BY i, k, t;

--Testcase 58:
EXPLAIN VERBOSE SELECT *
  FROM J1_TBL RIGHT OUTER JOIN J2_TBL USING (i);
--Testcase 59:
SELECT *
  FROM J1_TBL RIGHT OUTER JOIN J2_TBL USING (i);

--Testcase 60:
EXPLAIN VERBOSE SELECT *
  FROM J1_TBL RIGHT JOIN J2_TBL USING (i);
--Testcase 61:
SELECT *
  FROM J1_TBL RIGHT JOIN J2_TBL USING (i);

--Testcase 62:
EXPLAIN VERBOSE SELECT *
  FROM J1_TBL FULL OUTER JOIN J2_TBL USING (i)
  ORDER BY i, k, t;
--Testcase 63:
SELECT *
  FROM J1_TBL FULL OUTER JOIN J2_TBL USING (i)
  ORDER BY i, k, t;

--Testcase 64:
EXPLAIN VERBOSE SELECT *
  FROM J1_TBL FULL JOIN J2_TBL USING (i)
  ORDER BY i, k, t;
--Testcase 65:
SELECT *
  FROM J1_TBL FULL JOIN J2_TBL USING (i)
  ORDER BY i, k, t;

--Testcase 66:
EXPLAIN VERBOSE SELECT *
  FROM J1_TBL LEFT JOIN J2_TBL USING (i) WHERE (k = 1);
--Testcase 67:
SELECT *
  FROM J1_TBL LEFT JOIN J2_TBL USING (i) WHERE (k = 1);

--Testcase 68:
EXPLAIN VERBOSE SELECT *
  FROM J1_TBL LEFT JOIN J2_TBL USING (i) WHERE (i = 1);
--Testcase 69:
SELECT *
  FROM J1_TBL LEFT JOIN J2_TBL USING (i) WHERE (i = 1);

--
-- semijoin selectivity for <>
--

--Testcase 70:
CREATE FOREIGN TABLE tenk1 (
  _id 		int4,
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
) SERVER mongo_server OPTIONS (database 'join_regress', collection 'tenk');

--Testcase 71:
CREATE FOREIGN TABLE tenk2 (
  _id 		int4,
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
) SERVER mongo_server OPTIONS (database 'join_regress', collection 'tenk');

--Testcase 72:
CREATE FOREIGN TABLE int4_tbl (_id int4, f1 int4)
 SERVER mongo_server OPTIONS (database 'join_regress', collection 'int4_tbl');

--Testcase 73:
select * from int4_tbl;
--Testcase 74:
explain (costs off)
select * from int4_tbl i4, tenk1 a
where exists(select * from tenk1 b
             where a.twothousand = b.twothousand and a.fivethous <> b.fivethous)
      and i4.f1 = a.tenthous;


--
-- More complicated constructs
--

--
-- Multiway full join
--

--Testcase 75:
CREATE FOREIGN TABLE t1 (_id int4, name TEXT, n INTEGER)
 SERVER mongo_server OPTIONS (database 'join_regress', collection 't1');
--Testcase 76:
CREATE FOREIGN TABLE t2 (_id int4, name TEXT, n INTEGER)
 SERVER mongo_server OPTIONS (database 'join_regress', collection 't2');
--Testcase 77:
CREATE FOREIGN TABLE t3 (_id int4, name TEXT, n INTEGER)
 SERVER mongo_server OPTIONS (database 'join_regress', collection 't3');

--Testcase 78:
EXPLAIN VERBOSE SELECT * FROM t1 FULL JOIN t2 USING (name) FULL JOIN t3 USING (name);

--Testcase 79:
SELECT * FROM t1 FULL JOIN t2 USING (name) FULL JOIN t3 USING (name);

--
-- Test interactions of join syntax and subqueries
--

-- Basic cases (we expect planner to pull up the subquery here)
--Testcase 80:
EXPLAIN VERBOSE SELECT * FROM
(SELECT * FROM t2) as s2
INNER JOIN
(SELECT * FROM t3) s3
USING (name);

--Testcase 81:
SELECT * FROM
(SELECT * FROM t2) as s2
INNER JOIN
(SELECT * FROM t3) s3
USING (name);

--Testcase 82:
EXPLAIN VERBOSE SELECT * FROM
(SELECT * FROM t2) as s2
LEFT JOIN
(SELECT * FROM t3) s3
USING (name);

--Testcase 83:
SELECT * FROM
(SELECT * FROM t2) as s2
LEFT JOIN
(SELECT * FROM t3) s3
USING (name);

--Testcase 84:
EXPLAIN VERBOSE SELECT * FROM
(SELECT * FROM t2) as s2
FULL JOIN
(SELECT * FROM t3) s3
USING (name);

--Testcase 85:
SELECT * FROM
(SELECT * FROM t2) as s2
FULL JOIN
(SELECT * FROM t3) s3
USING (name);

-- Cases with non-nullable expressions in subquery results;
-- make sure these go to null as expected
--Testcase 86:
EXPLAIN VERBOSE SELECT * FROM
(SELECT name, n as s2_n, 2 as s2_2 FROM t2) as s2
NATURAL INNER JOIN
(SELECT name, n as s3_n, 3 as s3_2 FROM t3) s3;

--Testcase 87:
SELECT * FROM
(SELECT name, n as s2_n, 2 as s2_2 FROM t2) as s2
NATURAL INNER JOIN
(SELECT name, n as s3_n, 3 as s3_2 FROM t3) s3;

--Testcase 88:
EXPLAIN VERBOSE SELECT * FROM
(SELECT name, n as s2_n, 2 as s2_2 FROM t2) as s2
NATURAL LEFT JOIN
(SELECT name, n as s3_n, 3 as s3_2 FROM t3) s3;

--Testcase 89:
SELECT * FROM
(SELECT name, n as s2_n, 2 as s2_2 FROM t2) as s2
NATURAL LEFT JOIN
(SELECT name, n as s3_n, 3 as s3_2 FROM t3) s3;

--Testcase 90:
EXPLAIN VERBOSE SELECT * FROM
(SELECT name, n as s2_n, 2 as s2_2 FROM t2) as s2
NATURAL FULL JOIN
(SELECT name, n as s3_n, 3 as s3_2 FROM t3) s3;

--Testcase 91:
SELECT * FROM
(SELECT name, n as s2_n, 2 as s2_2 FROM t2) as s2
NATURAL FULL JOIN
(SELECT name, n as s3_n, 3 as s3_2 FROM t3) s3;

--Testcase 92:
EXPLAIN VERBOSE SELECT * FROM
(SELECT name, n as s1_n, 1 as s1_1 FROM t1) as s1
NATURAL INNER JOIN
(SELECT name, n as s2_n, 2 as s2_2 FROM t2) as s2
NATURAL INNER JOIN
(SELECT name, n as s3_n, 3 as s3_2 FROM t3) s3;

--Testcase 93:
SELECT * FROM
(SELECT name, n as s1_n, 1 as s1_1 FROM t1) as s1
NATURAL INNER JOIN
(SELECT name, n as s2_n, 2 as s2_2 FROM t2) as s2
NATURAL INNER JOIN
(SELECT name, n as s3_n, 3 as s3_2 FROM t3) s3;

--Testcase 94:
EXPLAIN VERBOSE SELECT * FROM
(SELECT name, n as s1_n, 1 as s1_1 FROM t1) as s1
NATURAL FULL JOIN
(SELECT name, n as s2_n, 2 as s2_2 FROM t2) as s2
NATURAL FULL JOIN
(SELECT name, n as s3_n, 3 as s3_2 FROM t3) s3;

--Testcase 95:
SELECT * FROM
(SELECT name, n as s1_n, 1 as s1_1 FROM t1) as s1
NATURAL FULL JOIN
(SELECT name, n as s2_n, 2 as s2_2 FROM t2) as s2
NATURAL FULL JOIN
(SELECT name, n as s3_n, 3 as s3_2 FROM t3) s3;

--Testcase 96:
EXPLAIN VERBOSE SELECT * FROM
(SELECT name, n as s1_n FROM t1) as s1
NATURAL FULL JOIN
  (SELECT * FROM
    (SELECT name, n as s2_n FROM t2) as s2
    NATURAL FULL JOIN
    (SELECT name, n as s3_n FROM t3) as s3
  ) ss2;

--Testcase 97:
SELECT * FROM
(SELECT name, n as s1_n FROM t1) as s1
NATURAL FULL JOIN
  (SELECT * FROM
    (SELECT name, n as s2_n FROM t2) as s2
    NATURAL FULL JOIN
    (SELECT name, n as s3_n FROM t3) as s3
  ) ss2;

--Testcase 98:
EXPLAIN VERBOSE SELECT * FROM
(SELECT name, n as s1_n FROM t1) as s1
NATURAL FULL JOIN
  (SELECT * FROM
    (SELECT name, n as s2_n, 2 as s2_2 FROM t2) as s2
    NATURAL FULL JOIN
    (SELECT name, n as s3_n FROM t3) as s3
  ) ss2;

--Testcase 99:
SELECT * FROM
(SELECT name, n as s1_n FROM t1) as s1
NATURAL FULL JOIN
  (SELECT * FROM
    (SELECT name, n as s2_n, 2 as s2_2 FROM t2) as s2
    NATURAL FULL JOIN
    (SELECT name, n as s3_n FROM t3) as s3
  ) ss2;


-- Constants as join keys can also be problematic
--Testcase 100:
EXPLAIN VERBOSE SELECT * FROM
  (SELECT name, n as s1_n FROM t1) as s1
FULL JOIN
  (SELECT name, 2 as s2_n FROM t2) as s2
ON (s1_n = s2_n);
--Testcase 101:
SELECT * FROM
  (SELECT name, n as s1_n FROM t1) as s1
FULL JOIN
  (SELECT name, 2 as s2_n FROM t2) as s2
ON (s1_n = s2_n);


-- Test for propagation of nullability constraints into sub-joins

--Testcase 102:
CREATE FOREIGN TABLE x (_id int4, x1 int, x2 int)
 SERVER mongo_server OPTIONS (database 'join_regress', collection 'x');
--Testcase 103:
CREATE FOREIGN TABLE y (_id int4, y1 int, y2 int)
 SERVER mongo_server OPTIONS (database 'join_regress', collection 'y');

--Testcase 104:
select * from x;
--Testcase 105:
select * from y;

--Testcase 106:
EXPLAIN VERBOSE select * from x left join y on (x1 = y1 and x2 is not null);
--Testcase 107:
select * from x left join y on (x1 = y1 and x2 is not null);
--Testcase 108:
EXPLAIN VERBOSE select * from x left join y on (x1 = y1 and y2 is not null);
--Testcase 109:
select * from x left join y on (x1 = y1 and y2 is not null);

--Testcase 110:
EXPLAIN VERBOSE select * from (x left join y on (x1 = y1)) left join x xx(idx, xx1,xx2)
on (x1 = xx1);select * from (x left join y on (x1 = y1)) left join x xx(idx, xx1,xx2)
on (x1 = xx1);
--Testcase 111:
EXPLAIN VERBOSE select * from (x left join y on (x1 = y1)) left join x xx(idx, xx1,xx2)
on (x1 = xx1 and x2 is not null);
--Testcase 112:
select * from (x left join y on (x1 = y1)) left join x xx(idx, xx1,xx2)
on (x1 = xx1 and x2 is not null);
--Testcase 113:
EXPLAIN VERBOSE select * from (x left join y on (x1 = y1)) left join x xx(idx, xx1,xx2)
on (x1 = xx1 and y2 is not null);
--Testcase 114:
select * from (x left join y on (x1 = y1)) left join x xx(idx, xx1,xx2)
on (x1 = xx1 and y2 is not null);
--Testcase 115:
EXPLAIN VERBOSE select * from (x left join y on (x1 = y1)) left join x xx(idx, xx1,xx2)
on (x1 = xx1 and xx2 is not null);
--Testcase 116:
select * from (x left join y on (x1 = y1)) left join x xx(idx, xx1,xx2)
on (x1 = xx1 and xx2 is not null);
-- these should NOT give the same answers as above
EXPLAIN VERBOSE select * from (x left join y on (x1 = y1)) left join x xx(idx, xx1,xx2)
on (x1 = xx1) where (x2 is not null);
--Testcase 117:
select * from (x left join y on (x1 = y1)) left join x xx(idx, xx1,xx2)
on (x1 = xx1) where (x2 is not null);
--Testcase 118:
EXPLAIN VERBOSE select * from (x left join y on (x1 = y1)) left join x xx(idx, xx1,xx2)
on (x1 = xx1) where (y2 is not null);
--Testcase 119:
select * from (x left join y on (x1 = y1)) left join x xx(idx, xx1,xx2)
on (x1 = xx1) where (y2 is not null);
--Testcase 120:
EXPLAIN VERBOSE select * from (x left join y on (x1 = y1)) left join x xx(idx, xx1,xx2)
on (x1 = xx1) where (xx2 is not null);
--Testcase 121:
select * from (x left join y on (x1 = y1)) left join x xx(idx, xx1,xx2)
on (x1 = xx1) where (xx2 is not null);

--
-- regression test: check handling of empty-FROM subquery underneath outer join
--
--Testcase 122:
CREATE FOREIGN TABLE int8_tbl (_id int4, q1 int8, q2 int8)
 SERVER mongo_server OPTIONS (database 'join_regress', collection 'int8_tbl');

--Testcase 123:
explain (costs off)
select * from int8_tbl i1 left join (int8_tbl i2 join
  (select 123 as x) ss on i2.q1 = x) on i1.q2 = i2.q2
order by 1, 2;

--Testcase 124:
select * from int8_tbl i1 left join (int8_tbl i2 join
  (select 123 as x) ss on i2.q1 = x) on i1.q2 = i2.q2
order by 1, 2;


--
-- regression test for 8.1 merge right join bug
--
--Testcase 125:
CREATE FOREIGN TABLE tt1 (_id int4, tt1_id int4, joincol int4)
 SERVER mongo_server OPTIONS (database 'join_regress', collection 'tt1');
--Testcase 126:
CREATE FOREIGN TABLE tt2 (_id int4, tt2_id int4, joincol int4)
 SERVER mongo_server OPTIONS (database 'join_regress', collection 'tt2');

--Testcase 127:
set enable_hashjoin to off;
--Testcase 128:
set enable_nestloop to off;

-- these should give the same results

--Testcase 129:
EXPLAIN VERBOSE select tt1.*, tt2.* from tt1 left join tt2 on tt1.joincol = tt2.joincol;
--Testcase 130:
select tt1.*, tt2.* from tt1 left join tt2 on tt1.joincol = tt2.joincol;

--Testcase 131:
EXPLAIN VERBOSE select tt1.*, tt2.* from tt2 right join tt1 on tt1.joincol = tt2.joincol;
--Testcase 132:
select tt1.*, tt2.* from tt2 right join tt1 on tt1.joincol = tt2.joincol;

--Testcase 133:
reset enable_hashjoin;
--Testcase 134:
reset enable_nestloop;

--
-- regression test for 8.2 bug with improper re-ordering of left joins
--

--Testcase 135:
CREATE FOREIGN TABLE tt3 (_id int4, f1 int, f2 text)
 SERVER mongo_server OPTIONS (database 'join_regress', collection 'tt3');
--Testcase 136:
CREATE FOREIGN TABLE tt4 (_id int4, f1 int)
 SERVER mongo_server OPTIONS (database 'join_regress', collection 'tt4');

--Testcase 137:
EXPLAIN VERBOSE SELECT a.f1
FROM tt4 a
LEFT JOIN (
        SELECT b.f1
        FROM tt3 b LEFT JOIN tt3 c ON (b.f1 = c.f1)
        WHERE c.f1 IS NULL
) AS d ON (a.f1 = d.f1)
WHERE d.f1 IS NULL;
--Testcase 138:
SELECT a.f1
FROM tt4 a
LEFT JOIN (
        SELECT b.f1
        FROM tt3 b LEFT JOIN tt3 c ON (b.f1 = c.f1)
        WHERE c.f1 IS NULL
) AS d ON (a.f1 = d.f1)
WHERE d.f1 IS NULL;

--
-- regression test for problems of the sort depicted in bug #3588
--

--Testcase 139:
CREATE FOREIGN TABLE xx (_id int4, pkxx int)
 SERVER mongo_server OPTIONS (database 'join_regress', collection 'xx');
--Testcase 140:
CREATE FOREIGN TABLE yy (_id int4, pkyy int, pkxx int)
 SERVER mongo_server OPTIONS (database 'join_regress', collection 'yy');

--Testcase 141:
select yy.pkyy as yy_pkyy, yy.pkxx as yy_pkxx, yya.pkyy as yya_pkyy,
       xxa.pkxx as xxa_pkxx, xxb.pkxx as xxb_pkxx
from yy
     left join (SELECT * FROM yy where pkyy = 101) as yya ON yy.pkyy = yya.pkyy
     left join xx xxa on yya.pkxx = xxa.pkxx
     left join xx xxb on coalesce (xxa.pkxx, 1) = xxb.pkxx;


--
-- regression test for improper pushing of constants across outer-join clauses
-- (as seen in early 8.2.x releases)
--

--Testcase 142:
CREATE FOREIGN TABLE zt1 (_id int4, f1 int)
 SERVER mongo_server OPTIONS (database 'join_regress', collection 'zt1');
--Testcase 143:
CREATE FOREIGN TABLE zt2 (_id int4, f2 int)
 SERVER mongo_server OPTIONS (database 'join_regress', collection 'zt2');
--Testcase 144:
CREATE FOREIGN TABLE zt3 (_id int4, f3 int)
 SERVER mongo_server OPTIONS (database 'join_regress', collection 'zt3');

--Testcase 145:
EXPLAIN VERBOSE select * from
  zt2 left join zt3 on (f2 = f3)
      left join zt1 on (f3 = f1)
where f2 = 53;
--Testcase 146:
select * from
  zt2 left join zt3 on (f2 = f3)
      left join zt1 on (f3 = f1)
where f2 = 53;

--Testcase 147:
create temp view zv1 as select *,'dummy'::text AS junk from zt1;

--Testcase 148:
EXPLAIN VERBOSE select * from
  zt2 left join zt3 on (f2 = f3)
      left join zv1 on (f3 = f1)
where f2 = 53;
--Testcase 149:
select * from
  zt2 left join zt3 on (f2 = f3)
      left join zv1 on (f3 = f1)
where f2 = 53;

--
-- regression test for improper extraction of OR indexqual conditions
-- (as seen in early 8.3.x releases)
--

--Testcase 150:
EXPLAIN VERBOSE select a.unique2, a.ten, b.tenthous, b.unique2, b.hundred
from tenk1 a left join tenk1 b on a.unique2 = b.tenthous
where a.unique1 = 42 and
      ((b.unique2 is null and a.ten = 2) or b.hundred = 3);
--Testcase 151:
select a.unique2, a.ten, b.tenthous, b.unique2, b.hundred
from tenk1 a left join tenk1 b on a.unique2 = b.tenthous
where a.unique1 = 42 and
      ((b.unique2 is null and a.ten = 2) or b.hundred = 3);

--
-- test proper positioning of one-time quals in EXISTS (8.4devel bug)
--
--Testcase 152:
prepare foo(bool) as
  select count(*) from tenk1 a left join tenk1 b
    on (a.unique2 = b.unique1 and exists
        (select 1 from tenk1 c where c.thousand = b.unique2 and $1));
--Testcase 153:
execute foo(true);
--Testcase 154:
execute foo(false);

--
-- test NULL behavior of whole-row Vars, per bug #5025
--

--Testcase 155:
EXPLAIN VERBOSE select t1.q2, count(t2.*)
from int8_tbl t1 left join int8_tbl t2 on (t1.q2 = t2.q1)
group by t1.q2 order by 1;
--Testcase 156:
select t1.q2, count(t2.*)
from int8_tbl t1 left join int8_tbl t2 on (t1.q2 = t2.q1)
group by t1.q2 order by 1;

--Testcase 157:
EXPLAIN VERBOSE select t1.q2, count(t2.*)
from int8_tbl t1 left join (select * from int8_tbl) t2 on (t1.q2 = t2.q1)
group by t1.q2 order by 1;
--Testcase 158:
select t1.q2, count(t2.*)
from int8_tbl t1 left join (select * from int8_tbl) t2 on (t1.q2 = t2.q1)
group by t1.q2 order by 1;

--Testcase 159:
EXPLAIN VERBOSE select t1.q2, count(t2.*)
from int8_tbl t1 left join (select * from int8_tbl offset 0) t2 on (t1.q2 = t2.q1)
group by t1.q2 order by 1;
--Testcase 160:
select t1.q2, count(t2.*)
from int8_tbl t1 left join (select * from int8_tbl offset 0) t2 on (t1.q2 = t2.q1)
group by t1.q2 order by 1;

--Testcase 161:
EXPLAIN VERBOSE select t1.q2, count(t2.*)
from int8_tbl t1 left join
  (select q1, case when q2=1 then 1 else q2 end as q2 from int8_tbl) t2
  on (t1.q2 = t2.q1)
group by t1.q2 order by 1;
--Testcase 162:
select t1.q2, count(t2.*)
from int8_tbl t1 left join
  (select q1, case when q2=1 then 1 else q2 end as q2 from int8_tbl) t2
  on (t1.q2 = t2.q1)
group by t1.q2 order by 1;

--
-- test incorrect failure to NULL pulled-up subexpressions
--

begin;

--Testcase 163:
CREATE FOREIGN TABLE a (_id int4, code char)
 SERVER mongo_server OPTIONS (database 'join_regress', collection 'a');
--Testcase 164:
CREATE FOREIGN TABLE b (_id int4, a char, num integer)
 SERVER mongo_server OPTIONS (database 'join_regress', collection 'b');
--Testcase 165:
CREATE FOREIGN TABLE c (_id int4, name char, a char)
 SERVER mongo_server OPTIONS (database 'join_regress', collection 'c');

--Testcase 166:
EXPLAIN VERBOSE select c.name, ss.code, ss.b_cnt, ss.const
from c left join
  (select a.code, coalesce(b_grp.cnt, 0) as b_cnt, -1 as const
   from a left join
     (select count(1) as cnt, b.a from b group by b.a) as b_grp
     on a.code = b_grp.a
  ) as ss
  on (c.a = ss.code)
order by c.name;

--Testcase 167:
select c.name, ss.code, ss.b_cnt, ss.const
from c left join
  (select a.code, coalesce(b_grp.cnt, 0) as b_cnt, -1 as const
   from a left join
     (select count(1) as cnt, b.a from b group by b.a) as b_grp
     on a.code = b_grp.a
  ) as ss
  on (c.a = ss.code)
order by c.name;

rollback;

--
-- test incorrect handling of placeholders that only appear in targetlists,
-- per bug #6154
--

--Testcase 168:
SELECT * FROM
( SELECT 1 as key1 ) sub1
LEFT JOIN
( SELECT sub3.key3, sub4.value2, COALESCE(sub4.value2, 66) as value3 FROM
    ( SELECT 1 as key3 ) sub3
    LEFT JOIN
    ( SELECT sub5.key5, COALESCE(sub6.value1, 1) as value2 FROM
        ( SELECT 1 as key5 ) sub5
        LEFT JOIN
        ( SELECT 2 as key6, 42 as value1 ) sub6
        ON sub5.key5 = sub6.key6
    ) sub4
    ON sub4.key5 = sub3.key3
) sub2
ON sub1.key1 = sub2.key3;

-- test the path using join aliases, too
--Testcase 169:
SELECT * FROM
( SELECT 1 as key1 ) sub1
LEFT JOIN
( SELECT sub3.key3, value2, COALESCE(value2, 66) as value3 FROM
    ( SELECT 1 as key3 ) sub3
    LEFT JOIN
    ( SELECT sub5.key5, COALESCE(sub6.value1, 1) as value2 FROM
        ( SELECT 1 as key5 ) sub5
        LEFT JOIN
        ( SELECT 2 as key6, 42 as value1 ) sub6
        ON sub5.key5 = sub6.key6
    ) sub4
    ON sub4.key5 = sub3.key3
) sub2
ON sub1.key1 = sub2.key3;

--
-- nested nestloops can require nested PlaceHolderVars
--

--Testcase 170:
CREATE FOREIGN TABLE nt1 (_id int4, id int, a1 boolean, a2 boolean)
 SERVER mongo_server OPTIONS (database 'join_regress', collection 'nt1');
--Testcase 171:
CREATE FOREIGN TABLE nt2 (_id int4, id int, nt1_id int, b1 boolean, b2 boolean)
 SERVER mongo_server OPTIONS (database 'join_regress', collection 'nt2');
--Testcase 172:
CREATE FOREIGN TABLE nt3 (_id int4, id int, nt2_id int, c1 boolean)
 SERVER mongo_server OPTIONS (database 'join_regress', collection 'nt3');

--Testcase 173:
explain (costs off)
select nt3.id
from nt3 as nt3
  left join
    (select nt2.*, (nt2.b1 and ss1.a3) AS b3
     from nt2 as nt2
       left join
         (select nt1.*, (nt1.id is not null) as a3 from nt1) as ss1
         on ss1.id = nt2.nt1_id
    ) as ss2
    on ss2.id = nt3.nt2_id
where nt3.id = 1 and ss2.b3;

--Testcase 174:
select nt3.id
from nt3 as nt3
  left join
    (select nt2.*, (nt2.b1 and ss1.a3) AS b3
     from nt2 as nt2
       left join
         (select nt1.*, (nt1.id is not null) as a3 from nt1) as ss1
         on ss1.id = nt2.nt1_id
    ) as ss2
    on ss2.id = nt3.nt2_id
where nt3.id = 1 and ss2.b3;

--
-- test case where a PlaceHolderVar is propagated into a subquery
--

--Testcase 175:
explain (costs off)
select * from
  int8_tbl t1 left join
  (select q1 as x, 42 as y from int8_tbl t2) ss
  on t1.q2 = ss.x
where
  1 = (select 1 from int8_tbl t3 where ss.y is not null limit 1)
order by 1,2;

--Testcase 176:
select * from
  int8_tbl t1 left join
  (select q1 as x, 42 as y from int8_tbl t2) ss
  on t1.q2 = ss.x
where
  1 = (select 1 from int8_tbl t3 where ss.y is not null limit 1)
order by 1,2;

--
-- variant where a PlaceHolderVar is needed at a join, but not above the join
--

explain (costs off)
select * from
  int4_tbl as i41,
  lateral
    (select 1 as x from
      (select i41.f1 as lat,
              i42.f1 as loc from
         int8_tbl as i81, int4_tbl as i42) as ss1
      right join int4_tbl as i43 on (i43.f1 > 1)
      where ss1.loc = ss1.lat) as ss2
where i41.f1 > 0;

select * from
  int4_tbl as i41,
  lateral
    (select 1 as x from
      (select i41.f1 as lat,
              i42.f1 as loc from
         int8_tbl as i81, int4_tbl as i42) as ss1
      right join int4_tbl as i43 on (i43.f1 > 1)
      where ss1.loc = ss1.lat) as ss2
where i41.f1 > 0;

--
-- test for ability to use a cartesian join when necessary
--
--Testcase 177:
CREATE FOREIGN TABLE q1 (_id int4, q1 int)
 SERVER mongo_server OPTIONS (database 'join_regress', collection 'q1');
--Testcase 178:
CREATE FOREIGN TABLE q2 (_id int4, q2 int)
 SERVER mongo_server OPTIONS (database 'join_regress', collection 'q2');
analyze q1;
analyze q2;

--
-- test a corner case in which we shouldn't apply the star-schema optimization
--

--Testcase 179:
explain (costs off)
select t1.unique2, t1.stringu1, t2.unique1, t2.stringu2 from
  tenk1 t1
  inner join int4_tbl i1
    left join (select v1.x2, v2.y1, 11 AS d1
               from (select 1,0 from onerow) v1(x1,x2)
               left join (select 3,1 from onerow) v2(y1,y2)
               on v1.x1 = v2.y2) subq1
    on (i1.f1 = subq1.x2)
  on (t1.unique2 = subq1.d1)
  left join tenk1 t2
  on (subq1.y1 = t2.unique1)
where t1.unique2 < 42 and t1.stringu1 > t2.stringu2;

--Testcase 180:
select t1.unique2, t1.stringu1, t2.unique1, t2.stringu2 from
  tenk1 t1
  inner join int4_tbl i1
    left join (select v1.x2, v2.y1, 11 AS d1
               from (select 1,0 from onerow) v1(x1,x2)
               left join (select 3,1 from onerow) v2(y1,y2)
               on v1.x1 = v2.y2) subq1
    on (i1.f1 = subq1.x2)
  on (t1.unique2 = subq1.d1)
  left join tenk1 t2
  on (subq1.y1 = t2.unique1)
where t1.unique2 < 42 and t1.stringu1 > t2.stringu2;

-- this variant is foldable by the remove-useless-RESULT-RTEs code

--Testcase 181:
explain (costs off)
select t1.unique2, t1.stringu1, t2.unique1, t2.stringu2 from
  tenk1 t1
  inner join int4_tbl i1
    left join (select v1.x2, v2.y1, 11 AS d1
               from (values(1,0)) v1(x1,x2)
               left join (values(3,1)) v2(y1,y2)
               on v1.x1 = v2.y2) subq1
    on (i1.f1 = subq1.x2)
  on (t1.unique2 = subq1.d1)
  left join tenk1 t2
  on (subq1.y1 = t2.unique1)
where t1.unique2 < 42 and t1.stringu1 > t2.stringu2;

--Testcase 182:
select t1.unique2, t1.stringu1, t2.unique1, t2.stringu2 from
  tenk1 t1
  inner join int4_tbl i1
    left join (select v1.x2, v2.y1, 11 AS d1
               from (values(1,0)) v1(x1,x2)
               left join (values(3,1)) v2(y1,y2)
               on v1.x1 = v2.y2) subq1
    on (i1.f1 = subq1.x2)
  on (t1.unique2 = subq1.d1)
  left join tenk1 t2
  on (subq1.y1 = t2.unique1)
where t1.unique2 < 42 and t1.stringu1 > t2.stringu2;

-- Here's a variant that we can't fold too aggressively, though,
-- or we end up with noplace to evaluate the lateral PHV
--Testcase 183:
explain (verbose, costs off)
select * from
  (select 1 as x) ss1 left join (select 2 as y) ss2 on (true),
  lateral (select ss2.y as z limit 1) ss3;
--Testcase 184:
select * from
  (select 1 as x) ss1 left join (select 2 as y) ss2 on (true),
  lateral (select ss2.y as z limit 1) ss3;

--
-- test inlining of immutable functions
--
--Testcase 185:
create function f_immutable_int4(i integer) returns integer as
$$ begin return i; end; $$ language plpgsql immutable;

-- test inlining of immutable functions with PlaceHolderVars
--Testcase 186:
explain (costs off)
select nt3.id
from nt3 as nt3
  left join
    (select nt2.*, (nt2.b1 or i4 = 42) AS b3
     from nt2 as nt2
       left join
         f_immutable_int4(0) i4
         on i4 = nt2.nt1_id
    ) as ss2
    on ss2.id = nt3.nt2_id
where nt3.id = 1 and ss2.b3;

--Testcase 187:
drop function f_immutable_int4(int);

--
-- test placement of movable quals in a parameterized join tree
--
--Testcase 188:
explain (costs off)
select * from tenk1 t1 left join
  (tenk1 t2 join tenk1 t3 on t2.thousand = t3.unique2)
  on t1.hundred = t2.hundred and t1.ten = t3.ten
where t1.unique1 = 1;

--Testcase 189:
explain (costs off)
select * from tenk1 t1 left join
  (tenk1 t2 join tenk1 t3 on t2.thousand = t3.unique2)
  on t1.hundred = t2.hundred and t1.ten + t2.ten = t3.ten
where t1.unique1 = 1;

--Testcase 190:
explain (costs off)
select count(*) from
  tenk1 a join tenk1 b on a.unique1 = b.unique2
  left join tenk1 c on a.unique2 = b.unique1 and c.thousand = a.thousand
  join int4_tbl on b.thousand = f1;

--Testcase 191:
select count(*) from
  tenk1 a join tenk1 b on a.unique1 = b.unique2
  left join tenk1 c on a.unique2 = b.unique1 and c.thousand = a.thousand
  join int4_tbl on b.thousand = f1;

--Testcase 192:
explain (costs off)
select b.unique1 from
  tenk1 a join tenk1 b on a.unique1 = b.unique2
  left join tenk1 c on b.unique1 = 42 and c.thousand = a.thousand
  join int4_tbl i1 on b.thousand = f1
  right join int4_tbl i2 on i2.f1 = b.tenthous
  order by 1;

--Testcase 193:
select b.unique1 from
  tenk1 a join tenk1 b on a.unique1 = b.unique2
  left join tenk1 c on b.unique1 = 42 and c.thousand = a.thousand
  join int4_tbl i1 on b.thousand = f1
  right join int4_tbl i2 on i2.f1 = b.tenthous
  order by 1;

--Testcase 194:
explain (costs off)
select * from
(
  select unique1, q1, coalesce(unique1, -1) + q1 as fault
  from int8_tbl left join tenk1 on (q2 = unique2)
) ss
where fault = 122
order by fault;

--Testcase 195:
select * from
(
  select unique1, q1, coalesce(unique1, -1) + q1 as fault
  from int8_tbl left join tenk1 on (q2 = unique2)
) ss
where fault = 122
order by fault;

--Testcase 196:
explain (costs off)
select * from
(values (1, array[10,20]), (2, array[20,30])) as v1(v1x,v1ys)
left join (values (1, 10), (2, 20)) as v2(v2x,v2y) on v2x = v1x
left join unnest(v1ys) as u1(u1y) on u1y = v2y;

--Testcase 197:
select * from
(values (1, array[10,20]), (2, array[20,30])) as v1(v1x,v1ys)
left join (values (1, 10), (2, 20)) as v2(v2x,v2y) on v2x = v1x
left join unnest(v1ys) as u1(u1y) on u1y = v2y;

--
-- test handling of potential equivalence clauses above outer joins
--
--Testcase 198:
explain (costs off)
select q1, unique2, thousand, hundred
  from int8_tbl a left join tenk1 b on q1 = unique2
  where coalesce(thousand,123) = q1 and q1 = coalesce(hundred,123);

--Testcase 199:
select q1, unique2, thousand, hundred
  from int8_tbl a left join tenk1 b on q1 = unique2
  where coalesce(thousand,123) = q1 and q1 = coalesce(hundred,123);

--Testcase 200:
explain (costs off)
select f1, unique2, case when unique2 is null then f1 else 0 end
  from int4_tbl a left join tenk1 b on f1 = unique2
  where (case when unique2 is null then f1 else 0 end) = 0;

--Testcase 201:
select f1, unique2, case when unique2 is null then f1 else 0 end
  from int4_tbl a left join tenk1 b on f1 = unique2
  where (case when unique2 is null then f1 else 0 end) = 0;

--
-- another case with equivalence clauses above outer joins (bug #8591)
--

--Testcase 202:
explain (costs off)
select a.unique1, b.unique1, c.unique1, coalesce(b.twothousand, a.twothousand)
  from tenk1 a left join tenk1 b on b.thousand = a.unique1                        left join tenk1 c on c.unique2 = coalesce(b.twothousand, a.twothousand)
  where a.unique2 < 10 and coalesce(b.twothousand, a.twothousand) = 44;

--Testcase 203:
select a.unique1, b.unique1, c.unique1, coalesce(b.twothousand, a.twothousand)
  from tenk1 a left join tenk1 b on b.thousand = a.unique1                        left join tenk1 c on c.unique2 = coalesce(b.twothousand, a.twothousand)
  where a.unique2 < 10 and coalesce(b.twothousand, a.twothousand) = 44;

--
-- check handling of join aliases when flattening multiple levels of subquery
--
--Testcase 204:
explain (verbose, costs off)
select foo1.join_key as foo1_id, foo3.join_key AS foo3_id, bug_field from
  (values (0),(1)) foo1(join_key)
left join
  (select join_key, bug_field from
    (select ss1.join_key, ss1.bug_field from
      (select f1 as join_key, 666 as bug_field from int4_tbl i1) ss1
    ) foo2
   left join
    (select unique2 as join_key from tenk1 i2) ss2
   using (join_key)
  ) foo3
using (join_key);

--Testcase 205:
select foo1.join_key as foo1_id, foo3.join_key AS foo3_id, bug_field from
  (values (0),(1)) foo1(join_key)
left join
  (select join_key, bug_field from
    (select ss1.join_key, ss1.bug_field from
      (select f1 as join_key, 666 as bug_field from int4_tbl i1) ss1
    ) foo2
   left join
    (select unique2 as join_key from tenk1 i2) ss2
   using (join_key)
  ) foo3
using (join_key);

--
-- test successful handling of nested outer joins with degenerate join quals
--
--Testcase 206:
CREATE FOREIGN TABLE text_tbl (_id int4, f1 text)
 SERVER mongo_server OPTIONS (database 'join_regress', collection 'text_tbl');

--Testcase 207:
explain (verbose, costs off)
select t1.* from
  text_tbl t1
  left join (select *, '***'::text as d1 from int8_tbl i8b1) b1
    left join int8_tbl i8
      left join (select *, null::int as d2 from int8_tbl i8b2) b2
      on (i8.q1 = b2.q1)
    on (b2.d2 = b1.q2)
  on (t1.f1 = b1.d1)
  left join int4_tbl i4
  on (i8.q2 = i4.f1);

--Testcase 208:
select t1.* from
  text_tbl t1
  left join (select *, '***'::text as d1 from int8_tbl i8b1) b1
    left join int8_tbl i8
      left join (select *, null::int as d2 from int8_tbl i8b2) b2
      on (i8.q1 = b2.q1)
    on (b2.d2 = b1.q2)
  on (t1.f1 = b1.d1)
  left join int4_tbl i4
  on (i8.q2 = i4.f1);

--Testcase 209:
explain (verbose, costs off)
select t1.* from
  text_tbl t1
  left join (select *, '***'::text as d1 from int8_tbl i8b1) b1
    left join int8_tbl i8
      left join (select *, null::int as d2 from int8_tbl i8b2, int4_tbl i4b2) b2
      on (i8.q1 = b2.q1)
    on (b2.d2 = b1.q2)
  on (t1.f1 = b1.d1)
  left join int4_tbl i4
  on (i8.q2 = i4.f1);

--Testcase 210:
select t1.* from
  text_tbl t1
  left join (select *, '***'::text as d1 from int8_tbl i8b1) b1
    left join int8_tbl i8
      left join (select *, null::int as d2 from int8_tbl i8b2, int4_tbl i4b2) b2
      on (i8.q1 = b2.q1)
    on (b2.d2 = b1.q2)
  on (t1.f1 = b1.d1)
  left join int4_tbl i4
  on (i8.q2 = i4.f1);

--Testcase 211:
explain (verbose, costs off)
select t1.* from
  text_tbl t1
  left join (select *, '***'::text as d1 from int8_tbl i8b1) b1
    left join int8_tbl i8
      left join (select *, null::int as d2 from int8_tbl i8b2, int4_tbl i4b2
                 where q1 = f1) b2
      on (i8.q1 = b2.q1)
    on (b2.d2 = b1.q2)
  on (t1.f1 = b1.d1)
  left join int4_tbl i4
  on (i8.q2 = i4.f1);

--Testcase 212:
select t1.* from
  text_tbl t1
  left join (select *, '***'::text as d1 from int8_tbl i8b1) b1
    left join int8_tbl i8
      left join (select *, null::int as d2 from int8_tbl i8b2, int4_tbl i4b2
                 where q1 = f1) b2
      on (i8.q1 = b2.q1)
    on (b2.d2 = b1.q2)
  on (t1.f1 = b1.d1)
  left join int4_tbl i4
  on (i8.q2 = i4.f1);

--Testcase 213:
explain (verbose, costs off)
select * from
  text_tbl t1
  inner join int8_tbl i8
  on i8.q2 = 456
  right join text_tbl t2
  on t1.f1 = 'doh!'
  left join int4_tbl i4
  on i8.q1 = i4.f1;

--Testcase 214:
select * from
  text_tbl t1
  inner join int8_tbl i8
  on i8.q2 = 456
  right join text_tbl t2
  on t1.f1 = 'doh!'
  left join int4_tbl i4
  on i8.q1 = i4.f1;

--
-- test for appropriate join order in the presence of lateral references
--

--Testcase 215:
explain (verbose, costs off)
select * from
  text_tbl t1
  left join int8_tbl i8
  on i8.q2 = 123,
  lateral (select i8.q1, t2.f1 from text_tbl t2 limit 1) as ss
where t1.f1 = ss.f1;

--Testcase 216:
select * from
  text_tbl t1
  left join int8_tbl i8
  on i8.q2 = 123,
  lateral (select i8.q1, t2.f1 from text_tbl t2 limit 1) as ss
where t1.f1 = ss.f1;

--Testcase 217:
explain (verbose, costs off)
select * from
  text_tbl t1
  left join int8_tbl i8
  on i8.q2 = 123,
  lateral (select i8.q1, t2.f1 from text_tbl t2 limit 1) as ss1,
  lateral (select ss1.* from text_tbl t3 limit 1) as ss2
where t1.f1 = ss2.f1;

--Testcase 218:
select * from
  text_tbl t1
  left join int8_tbl i8
  on i8.q2 = 123,
  lateral (select i8.q1, t2.f1 from text_tbl t2 limit 1) as ss1,
  lateral (select ss1.* from text_tbl t3 limit 1) as ss2
where t1.f1 = ss2.f1;

--Testcase 219:
explain (verbose, costs off)
select 1 from
  text_tbl as tt1
  inner join text_tbl as tt2 on (tt1.f1 = 'foo')
  left join text_tbl as tt3 on (tt3.f1 = 'foo')
  left join text_tbl as tt4 on (tt3.f1 = tt4.f1),
  lateral (select tt4.f1 as c0 from text_tbl as tt5 limit 1) as ss1
where tt1.f1 = ss1.c0;

--Testcase 220:
select 1 from
  text_tbl as tt1
  inner join text_tbl as tt2 on (tt1.f1 = 'foo')
  left join text_tbl as tt3 on (tt3.f1 = 'foo')
  left join text_tbl as tt4 on (tt3.f1 = tt4.f1),
  lateral (select tt4.f1 as c0 from text_tbl as tt5 limit 1) as ss1
where tt1.f1 = ss1.c0;

--
-- check a case in which a PlaceHolderVar forces join order
--

--Testcase 221:
explain (verbose, costs off)
select ss2.* from
  int4_tbl i41
  left join int8_tbl i8
    join (select i42.f1 as c1, i43.f1 as c2, 42 as c3
          from int4_tbl i42, int4_tbl i43) ss1
    on i8.q1 = ss1.c2
  on i41.f1 = ss1.c1,
  lateral (select i41.*, i8.*, ss1.* from text_tbl limit 1) ss2
where ss1.c2 = 0;

--Testcase 222:
select ss2.* from
  int4_tbl i41
  left join int8_tbl i8
    join (select i42.f1 as c1, i43.f1 as c2, 42 as c3
          from int4_tbl i42, int4_tbl i43) ss1
    on i8.q1 = ss1.c2
  on i41.f1 = ss1.c1,
  lateral (select i41.*, i8.*, ss1.* from text_tbl limit 1) ss2
where ss1.c2 = 0;

--
-- test successful handling of full join underneath left join (bug #14105)
--

--Testcase 223:
explain (costs off)
select * from
  (select 1 as id) as xx
  left join
    (tenk1 as a1 full join (select 1 as id) as yy on (a1.unique1 = yy.id))
  on (xx.id = coalesce(yy.id));

--Testcase 224:
select * from
  (select 1 as id) as xx
  left join
    (tenk1 as a1 full join (select 1 as id) as yy on (a1.unique1 = yy.id))
  on (xx.id = coalesce(yy.id));

--
-- test ability to push constants through outer join clauses
--

--Testcase 225:
explain (costs off)
  select * from int4_tbl a left join tenk1 b on f1 = unique2 where f1 = 0;

--Testcase 226:
explain (costs off)
  select * from tenk1 a full join tenk1 b using(unique2) where unique2 = 42;

--
-- test join removal
--
begin;

--Testcase 227:
CREATE FOREIGN TABLE a1 (_id int4, id int, b_id int)
 SERVER mongo_server OPTIONS (database 'join_regress', collection 'a1');
--Testcase 228:
CREATE FOREIGN TABLE b1 (_id int4, id int, c_id int)
 SERVER mongo_server OPTIONS (database 'join_regress', collection 'b1');
--Testcase 229:
CREATE FOREIGN TABLE c1 (_id int4, id int)
 SERVER mongo_server OPTIONS (database 'join_regress', collection 'c1');
--Testcase 230:
CREATE FOREIGN TABLE d1 (_id int4, a int, b int)
 SERVER mongo_server OPTIONS (database 'join_regress', collection 'd1');

-- all three cases should be optimizable into a simple seqscan
--Testcase 231:
explain (costs off) SELECT a1.* FROM a1 LEFT JOIN b1 ON a1.b_id = b1.id;
--Testcase 232:
explain (costs off) SELECT b1.* FROM b1 LEFT JOIN c1 ON b1.c_id = c1.id;
--Testcase 233:
explain (costs off)
  SELECT a1.* FROM a1 LEFT JOIN (b1 left join c1 on b1.c_id = c1.id)
  ON (a1.b_id = b1.id);

-- check optimization of outer join within another special join
--Testcase 234:
explain (costs off)
select id from a1 where id in (
	select b1.id from b1 left join c1 on b1.id = c1.id
);

-- check that join removal works for a left join when joining a subquery
-- that is guaranteed to be unique by its GROUP BY clause
--Testcase 235:
explain (costs off)
select d1.* from d1 left join (select * from b1 group by b1.id, b1.c_id, b1._id) s
  on d1.a = s.id and d1.b = s.c_id;

-- similarly, but keying off a DISTINCT clause
--Testcase 236:
explain (costs off)
select d1.* from d1 left join (select distinct * from b1) s
  on d1.a = s.id and d1.b = s.c_id;

-- join removal is not possible when the GROUP BY contains a column that is
-- not in the join condition.  (Note: as of 9.6, we notice that b.id is a
-- primary key and so drop b.c_id from the GROUP BY of the resulting plan;
-- but this happens too late for join removal in the outer plan level.)
--Testcase 237:
explain (costs off)
select d1.* from d1 left join (select * from b1 group by b1.id, b1.c_id, b1._id) s
  on d1.a = s.id;

-- similarly, but keying off a DISTINCT clause
--Testcase 238:
explain (costs off)
select d1.* from d1 left join (select distinct * from b1) s
  on d1.a = s.id;

-- check join removal works when uniqueness of the join condition is enforced
-- by a UNION
--Testcase 239:
explain (costs off)
select d1.* from d1 left join (select id from a1 union select id from b1) s
  on d1.a = s.id;

-- check join removal with a cross-type comparison operator
--Testcase 240:
explain (costs off)
select i8.* from int8_tbl i8 left join (select f1 from int4_tbl group by f1) i4
  on i8.q1 = i4.f1;

-- check join removal with lateral references
--Testcase 241:
explain (costs off)
select 1 from (select a1.id FROM a1 left join b1 on a1.b_id = b1.id) q,
			  lateral generate_series(1, q.id) gs(i) where q.id = gs.i;

rollback;


--Testcase 242:
CREATE FOREIGN TABLE parent (_id int4, k int, pd int)
 SERVER mongo_server OPTIONS (database 'join_regress', collection 'parent');
--Testcase 243:
CREATE FOREIGN TABLE child (_id int4, k int, cd int)
 SERVER mongo_server OPTIONS (database 'join_regress', collection 'child');

-- this case is optimizable
--Testcase 244:
select p.* from parent p left join child c on (p.k = c.k);
--Testcase 245:
explain (costs off)
  select p.* from parent p left join child c on (p.k = c.k);

-- this case is not
--Testcase 246:
select p.*, linked from parent p
  left join (select c.*, true as linked from child c) as ss
  on (p.k = ss.k);
--Testcase 247:
explain (costs off)
  select p.*, linked from parent p
    left join (select c.*, true as linked from child c) as ss
    on (p.k = ss.k);

-- check for a 9.0rc1 bug: join removal breaks pseudoconstant qual handling
--Testcase 248:
select p.* from
  parent p left join child c on (p.k = c.k)
  where p.k = 1 and p.k = 2;
--Testcase 249:
explain (costs off)
select p.* from
  parent p left join child c on (p.k = c.k)
  where p.k = 1 and p.k = 2;

--Testcase 250:
select p.* from
  (parent p left join child c on (p.k = c.k)) join parent x on p.k = x.k
  where p.k = 1 and p.k = 2;
--Testcase 251:
explain (costs off)
select p.* from
  (parent p left join child c on (p.k = c.k)) join parent x on p.k = x.k
  where p.k = 1 and p.k = 2;

-- bug 5255: this is not optimizable by join removal
begin;

--Testcase 252:
CREATE FOREIGN TABLE a2 (_id int4, id int)
 SERVER mongo_server OPTIONS (database 'join_regress', collection 'a2');
--Testcase 253:
CREATE FOREIGN TABLE b2 (_id int4, id int, a_id int)
 SERVER mongo_server OPTIONS (database 'join_regress', collection 'b2');

--Testcase 254:
SELECT * FROM b2 LEFT JOIN a2 ON (b2.a_id = a2.id) WHERE (a2.id IS NULL OR a2.id > 0);
--Testcase 255:
SELECT b2.* FROM b2 LEFT JOIN a2 ON (b2.a_id = a2.id) WHERE (a2.id IS NULL OR a2.id > 0);

rollback;

-- another join removal bug: this is not optimizable, either
begin;

--Testcase 256:
CREATE FOREIGN TABLE innertab (_id int4, id int8, dat1 int8)
 SERVER mongo_server OPTIONS (database 'join_regress', collection 'innertab');

--Testcase 257:
SELECT * FROM
    (SELECT 1 AS x) ss1
  LEFT JOIN
    (SELECT q1, q2, COALESCE(dat1, q1) AS y
     FROM int8_tbl LEFT JOIN innertab ON q2 = id) ss2
  ON true;

rollback;

-- test case to expose miscomputation of required relid set for a PHV
explain (verbose, costs off)
select i8.*, ss.v, t.unique2
  from int8_tbl i8
    left join int4_tbl i4 on i4.f1 = 1
    left join lateral (select i4.f1 + 1 as v) as ss on true
    left join tenk1 t on t.unique2 = ss.v
where q2 = 456;

select i8.*, ss.v, t.unique2
  from int8_tbl i8
    left join int4_tbl i4 on i4.f1 = 1
    left join lateral (select i4.f1 + 1 as v) as ss on true
    left join tenk1 t on t.unique2 = ss.v
where q2 = 456;

-- and check a related issue where we miscompute required relids for
-- a PHV that's been translated to a child rel
create temp table parttbl (_id name, a integer primary key) partition by range (a);
create foreign table parttbl1 (_id name, a integer)
  server mongo_server options (database 'join_regress', collection 'parttbl1');

alter table parttbl attach partition parttbl1 for values from (1) to (100);

-- cannot insert via parent table
insert into parttbl1(a) values (11), (12);

explain (costs off)
select * from
  (select *, 12 as phv from parttbl) as ss
  right join int4_tbl on true
where ss.a = ss.phv and f1 = 0;

select * from
  (select *, 12 as phv from parttbl) as ss
  right join int4_tbl on true
where ss.a = ss.phv and f1 = 0;

delete from parttbl1;
delete from parttbl;

drop foreign table parttbl1;
drop table parttbl;

-- lateral injecting a strange outer join condition
--Testcase 258:
explain (costs off)
  select * from int8_tbl a,
    int8_tbl x left join lateral (select a.q1 from int4_tbl y) ss(z)
      on x.q2 = ss.z
  order by a.q1, a.q2, x.q1, x.q2, ss.z;
--Testcase 259:
select * from int8_tbl a,
  int8_tbl x left join lateral (select a.q1 from int4_tbl y) ss(z)
    on x.q2 = ss.z
  order by a.q1, a.q2, x.q1, x.q2, ss.z;

-- lateral references requiring pullup
--Testcase 260:
select * from
  int8_tbl x left join (select q1,coalesce(q2,0) q2 from int8_tbl) y on x.q2 = y.q1,
  lateral (values(x.q1,y.q1,y.q2)) v(xq1,yq1,yq2);
--Testcase 261:
select * from
  int8_tbl x left join (select q1,coalesce(q2,0) q2 from int8_tbl) y on x.q2 = y.q1,
  lateral (select x.q1,y.q1,y.q2) v(xq1,yq1,yq2);
--Testcase 262:
select x.* from
  int8_tbl x left join (select q1,coalesce(q2,0) q2 from int8_tbl) y on x.q2 = y.q1,
  lateral (select x.q1,y.q1,y.q2) v(xq1,yq1,yq2);
--Testcase 263:
select v.* from
  (int8_tbl x left join (select q1,coalesce(q2,0) q2 from int8_tbl) y on x.q2 = y.q1)
  left join int4_tbl z on z.f1 = x.q2,
  lateral (select x.q1,y.q1 union all select x.q2,y.q2) v(vx,vy);
--Testcase 264:
select v.* from
  (int8_tbl x left join (select q1,(select coalesce(q2,0)) q2 from int8_tbl) y on x.q2 = y.q1)
  left join int4_tbl z on z.f1 = x.q2,
  lateral (select x.q1,y.q1 union all select x.q2,y.q2) v(vx,vy);
--Testcase 265:
select v.* from
  (int8_tbl x left join (select q1,(select coalesce(q2,0)) q2 from int8_tbl) y on x.q2 = y.q1)
  left join int4_tbl z on z.f1 = x.q2,
  lateral (select x.q1,y.q1 from onerow union all select x.q2,y.q2 from onerow) v(vx,vy);

--Testcase 266:
explain (verbose, costs off)
select * from
  int8_tbl a left join
  lateral (select *, a.q2 as x from int8_tbl b) ss on a.q2 = ss.q1;
--Testcase 267:
select * from
  int8_tbl a left join
  lateral (select *, a.q2 as x from int8_tbl b) ss on a.q2 = ss.q1;
--Testcase 268:
explain (verbose, costs off)
select * from
  int8_tbl a left join
  lateral (select *, coalesce(a.q2, 42) as x from int8_tbl b) ss on a.q2 = ss.q1;
--Testcase 269:
select * from
  int8_tbl a left join
  lateral (select *, coalesce(a.q2, 42) as x from int8_tbl b) ss on a.q2 = ss.q1;

-- lateral can result in join conditions appearing below their
-- real semantic level

--Testcase 270:
CREATE FOREIGN TABLE INT2_TBL(_id int4, f1 int2) 
 SERVER mongo_server OPTIONS (database 'join_regress', collection 'int2_tbl');

--Testcase 271:
explain (verbose, costs off)
select * from int4_tbl i left join
  lateral (select * from int2_tbl j where i.f1 = j.f1) k on true;
--Testcase 272:
select * from int4_tbl i left join
  lateral (select * from int2_tbl j where i.f1 = j.f1) k on true;
--Testcase 273:
explain (verbose, costs off)
select * from int4_tbl i left join
  lateral (select coalesce(i) from int2_tbl j where i.f1 = j.f1) k on true;
--Testcase 274:
select * from int4_tbl i left join
  lateral (select coalesce(i) from int2_tbl j where i.f1 = j.f1) k on true;
--Testcase 275:
explain (verbose, costs off)
select * from int4_tbl a,
  lateral (
    select * from int4_tbl b left join int8_tbl c on (b.f1 = q1 and a.f1 = q2)
  ) ss;
--Testcase 276:
select * from int4_tbl a,
  lateral (
    select * from int4_tbl b left join int8_tbl c on (b.f1 = q1 and a.f1 = q2)
  ) ss;

-- lateral reference in a PlaceHolderVar evaluated at join level
--Testcase 277:
explain (verbose, costs off)
select * from
  int8_tbl c left join (
    int8_tbl a left join (select q1, coalesce(q2,42) as x from int8_tbl b) ss1
      on a.q2 = ss1.q1
    cross join
    lateral (select q1, coalesce(ss1.x,q2) as y from int8_tbl d) ss2
  ) on c.q2 = ss2.q1,
  lateral (select ss2.y offset 0) ss3;

-- case that breaks the old ph_may_need optimization
--Testcase 278:
explain (verbose, costs off)
select c.*,a.*,ss1.q1,ss2.q1,ss3.* from
  int8_tbl c left join (
    int8_tbl a left join
      (select q1, coalesce(q2,f1) as x from int8_tbl b, int4_tbl b2
       where q1 < f1) ss1
      on a.q2 = ss1.q1
    cross join
    lateral (select q1, coalesce(ss1.x,q2) as y from int8_tbl d) ss2
  ) on c.q2 = ss2.q1,
  lateral (select * from int4_tbl i where ss2.y > f1) ss3;

-- check dummy rels with lateral references (bug #15694)
--Testcase 279:
explain (verbose, costs off)
select * from int8_tbl i8 left join lateral
  (select *, i8.q2 from int4_tbl where false) ss on true;
--Testcase 280:
explain (verbose, costs off)
select * from int8_tbl i8 left join lateral
  (select *, i8.q2 from int4_tbl i1, int4_tbl i2 where false) ss on true;

--
-- test that foreign key join estimation performs sanely for outer joins
--

begin;

--Testcase 281:
CREATE FOREIGN TABLE fkest (_id int4, a int, b int, c int)
 SERVER mongo_server OPTIONS (database 'join_regress', collection 'fkest');
--Testcase 282:
CREATE FOREIGN TABLE fkest1 (_id int4, a int, b int)
 SERVER mongo_server OPTIONS (database 'join_regress', collection 'fkest1');

--Testcase 283:
explain (costs off)
select *
from fkest f
  left join fkest1 f1 on f.a = f1.a and f.b = f1.b
  left join fkest1 f2 on f.a = f2.a and f.b = f2.b
  left join fkest1 f3 on f.a = f3.a and f.b = f3.b
where f.c = 1;

rollback;

-- NULL comparison
-- PostgreSQL treats a NULL value is larger than a non-NULL value
-- But a NULL value is smaller than a non-NULL value in MongoDB
--Testcase 286:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM x LEFT JOIN y ON (x.x1 = y.y1 AND x.x2 < y.y2);
--Testcase 287:
SELECT * FROM x LEFT JOIN y ON (x.x1 = y.y1 AND x.x2 < y.y2);
--Testcase 288:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM x LEFT JOIN y ON (x.x1 = y.y1 AND x.x2 <= y.y2);
--Testcase 289:
SELECT * FROM x LEFT JOIN y ON (x.x1 = y.y1 AND x.x2 <= y.y2);
--Testcase 290:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM x LEFT JOIN y ON (x.x1 = y.y1 AND x.x2 > y.y2);
--Testcase 291:
SELECT * FROM x LEFT JOIN y ON (x.x1 = y.y1 AND x.x2 > y.y2);
--Testcase 292:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM x LEFT JOIN y ON (x.x1 = y.y1 AND x.x2 >= y.y2);
--Testcase 293:
SELECT * FROM x LEFT JOIN y ON (x.x1 = y.y1 AND x.x2 >= y.y2);
--Testcase 294:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM x LEFT JOIN y ON (x.x1 = y.y1 AND x.x2 != y.y2);
--Testcase 295:
SELECT * FROM x LEFT JOIN y ON (x.x1 = y.y1 AND x.x2 != y.y2);
--Testcase 296:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM x LEFT JOIN y ON (x.x1 = y.y1 AND x.x2 IS NOT NULL);
--Testcase 297:
SELECT * FROM x LEFT JOIN y ON (x.x1 = y.y1 AND x.x2 IS NOT NULL);
--Testcase 298:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM x LEFT JOIN y ON (x.x1 = y.y1 AND y.y2 IS NOT NULL);
--Testcase 299:
SELECT * FROM x LEFT JOIN y ON (x.x1 = y.y1 AND y.y2 IS NOT NULL);

--Testcase 300:
DROP USER MAPPING FOR public SERVER mongo_server;
--Testcase 301:
DROP EXTENSION mongo_fdw CASCADE;

