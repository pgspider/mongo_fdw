# MongoDB Foreign Data Wrapper for PostgreSQL

This PostgreSQL extension implements a Foreign Data Wrapper (FDW) for
[MongoDB][1].

Please note that this version of mongo_fdw works with PostgreSQL and EDB
Postgres Advanced Server 9.6, 10, 11, 12, 13, and 14.

Installation
------------
The MongoDB FDW depends on the official MongoDB C Driver version 0.8 and
includes it as a git submodule. If you are cloning this repository for
the first time, be sure to pass the --recursive option to git clone in
order to initialize the driver submodule to a usable state.

If checked out this project before and for some reason your submodule
is not up-to-date, run git submodule update --init.

When you type `cmake`, the C driver's source code also gets automatically
compiled and linked.

Note: Make sure you have permission to "/usr/local"
(default installation location) folder.

1. To build on POSIX-compliant systems you need to ensure the
   `pg_config` executable is in your path when you run `make`. This
   executable is typically in your PostgreSQL installation's `bin`
   directory. For example:

    ```
    $ export PATH=/usr/local/pgsql/bin/:$PATH
    ```

2. Compile the code using make.

    ```
    $ make USE_PGXS=1
    ```

3.  Finally install the foreign data wrapper.

    ```
    $ make USE_PGXS=1 install
    ```

4. Running regression test.

    ```
    $ make USE_PGXS=1 installcheck
    ```
   However, make sure to set the `MONGO_HOST`, `MONGO_PORT`, `MONGO_USER_NAME`,
   and `MONGO_PWD` environment variables correctly. The default settings can be
   found in the `mongodb_init.sh` script.


If you run into any issues, please [let us know][2].

Enhancements
-----------
The following enhancements are added to the latest version of mongo_fdw:

### Write-able FDW
The previous version was only read-only, the latest version provides the
write capability. The user can now issue an insert, update, and delete
statements for the foreign tables using the mongo_fdw.

### Connection Pooling
The latest version comes with a connection pooler that utilizes the
same mango database connection for all the queries in the same session.
The previous version would open a new [MongoDB][1] connection for every
query. This is a performance enhancement.

### New MongoDB C Driver Support
The third enhancement is to add a new [MongoDB][1]' C driver. The
current implementation is based on the legacy driver of MongoDB. But
[MongoDB][1] is provided completely new library for driver called
MongoDB's meta driver. Added support for the same. Now compile time
option is available to use legacy and meta driver.

In order to use MongoDB driver 1.17.0+, take the following steps:

  * clone `libmongoc` version 1.17.0+
    (https://github.com/mongodb/mongo-c-driver) and follow the install
    directions given there.  `libbson` is now maintained in a subdirectory
    of the `libmongoc`.
    (https://github.com/mongodb/mongo-c-driver/tree/master/src/libbson).
  * ensure pkg-config / pkgconf is installed on your system.
  * run `make -f Makefile.meta && make -f Makefile.meta install`
  * if you get an error when trying to `CREATE EXTENSION mongo_fdw;`,
    then try running `ldconfig`

### Pushdown features
  * Aggregate functions:
      * `avg`: is converted to `$avg` aggregate function.
      * `count(*)`: is converted to `{ $sum : 1 }` BSON document.
      * `max`: is converted to `$max` aggregate function.
      * `min`: is converted to `$min` aggregate function.
      * `sum`: is converted to `$sum` aggregate function.
      * `stddev`: is converted to `$stdDevSamp` aggregate function.
      * `stddev_pop`: is converted to `$stdDevPop` aggregate function.
      * `stddev_samp`: is converted to `$stdDevSamp` aggregate function.
  * LEFT JOIN
      * The result for NULL comparison of MongoDB is different from PostgreSQL because MongoDB compares NULL value in equal comparison expression, but PostgreSQL does not.
      * The order for comparing NULL value in MongoDB and PostgreSQL is different ([MongoDB's order][5], [PostgreSQL's order][6])
  * LIMIT/OFFSET clause
  * JSON arrow operator (json -> text → json): Extracts JSON object field with the given key

Compilation script
-----------------
Number of manual steps needs to be performed to compile and install
different type of MongoDB drivers and supported libraries. If you want
to avoid the manual steps, there is a shell script available which will
download and install the appropriate drivers and libraries for you.

Here is how it works:

To install mongo-c and json-c libraries at custom locations, you need to
export environment variables `MONGOC_INSTALL_DIR` and `JSONC_INSTALL_DIR`
respectively. If these variables are not set then these libraries will be
installed in the default location. Please note that you need to have the
required permission to install the directory whether it is custom or default.

The `PKG_CONFIG_PATH` environment variable must be set to mongo-c-driver source
directory for successful compilation as shown below,

```sh
export PKG_CONFIG_PATH=$YOUR_MONGO_FDW_SOURCE_DIR/mongo-c-driver/src/libmongoc/src:$YOUR_MONGO_FDW_SOURCE_DIR/mongo-c-driver/src/libbson/src
```

The `LD_LIBRARY_PATH` environment variable must include the path to the mongo-c
installation directory containing the libmongoc-1.0.so and libbson-1.0.so
files. For example, assuming the installation directory is /home/mongo-c and
the libraries were created under it in lib64 sub-directory, then we can define
the `LD_LIBRARY_PATH` as:

```sh
export LD_LIBRARY_PATH=/home/mongo-c/lib64:$LD_LIBRARY_PATH
```

Note: This `LD_LIBRARY_PATH` environment variable setting must be in effect
when the `pg_ctl` utility is executed to start or restart PostgreSQL or
EDB Postgres Advanced Server.

Build with [MongoDB][1]'s legacy branch driver
   * autogen.sh --with-legacy

Build [MongoDB][1]'s master branch driver
   * autogen.sh --with-master

The script will do all the necessary steps to build with legacy and
meta driver accordingly.

Usage
-----
The following parameters can be set on a MongoDB foreign server object:

  * `address`: Address or hostname of the MongoDB server. Defaults to
    `127.0.0.1`
  * `port`: Port number of the MongoDB server. Defaults to `27017`.

The following options are only supported with meta driver:

  * `authentication_database`: Database against which user will be
    authenticated against. Only valid with password based authentication.
  * `replica_set`: Replica set the server is member of. If set,
    driver will auto-connect to correct primary in the replica set when
    writing.
  * `read_preference`: primary [default], secondary, primaryPreferred,
    secondaryPreferred, or nearest.
  * `ssl`: false [default], true to enable ssl. See
    http://mongoc.org/libmongoc/current/mongoc_ssl_opt_t.html to
    understand the options.
  * `pem_file`: SSL option.
  * `pem_pwd`: SSL option.
  * `ca_file`: SSL option.
  * `ca_dir`: SSL option.
  * `crl_file`: SSL option.
  * `weak_cert_validation`: SSL option, false [default].

The following parameters can be set on a MongoDB foreign table object:

  * `database`: Name of the MongoDB database to query. Defaults to
    `test`.
  * `collection`: Name of the MongoDB collection to query. Defaults to
    the foreign table name used in the relevant `CREATE` command.

The following parameters can be supplied while creating user mapping:

  * `username`: Username to use when connecting to MongoDB.
  * `password`: Password to authenticate to the MongoDB server.

As an example, the following commands demonstrate loading the
`mongo_fdw` wrapper, creating a server, and then creating a foreign
table associated with a MongoDB collection. The commands also show
specifying option values in the `OPTIONS` clause. If an option value
isn't provided, the wrapper uses the default value mentioned above.

`mongo_fdw` can collect data distribution statistics will incorporate
them when estimating costs for the query execution plan. To see selected
execution plans for a query, just run `EXPLAIN`.

Examples
--------

Examples with [MongoDB][1]'s equivalent statements.

```sql
-- load extension first time after install
CREATE EXTENSION mongo_fdw;

-- create server object
CREATE SERVER mongo_server
	FOREIGN DATA WRAPPER mongo_fdw
	OPTIONS (address '127.0.0.1', port '27017');

-- create user mapping
CREATE USER MAPPING FOR postgres
	SERVER mongo_server
	OPTIONS (username 'mongo_user', password 'mongo_pass');

-- create foreign table
CREATE FOREIGN TABLE warehouse
	(
		_id name,
		warehouse_id int,
		warehouse_name text,
		warehouse_created timestamptz
	)
	SERVER mongo_server
	OPTIONS (database 'db', collection 'warehouse');

-- Note: first column of the table must be "_id" of type "name".

-- select from table
SELECT * FROM warehouse WHERE warehouse_id = 1;
           _id            | warehouse_id | warehouse_name |     warehouse_created
--------------------------+--------------+----------------+---------------------------
 53720b1904864dc1f5a571a0 |            1 | UPS            | 2014-12-12 12:42:10+05:30
(1 row)

db.warehouse.find
(
	{
		"warehouse_id" : 1
	}
).pretty()
{
	"_id" : ObjectId("53720b1904864dc1f5a571a0"),
	"warehouse_id" : 1,
	"warehouse_name" : "UPS",
	"warehouse_created" : ISODate("2014-12-12T07:12:10Z")
}

-- insert row in table
INSERT INTO warehouse VALUES (0, 2, 'Laptop', '2015-11-11T08:13:10Z');

db.warehouse.insert
(
	{
		"warehouse_id" : NumberInt(2),
		"warehouse_name" : "Laptop",
		"warehouse_created" : ISODate("2015-11-11T08:13:10Z")
	}
)

-- delete row from table
DELETE FROM warehouse WHERE warehouse_id = 2;

db.warehouse.remove
(
	{
		"warehouse_id" : 2
	}
)

-- update a row of table
UPDATE warehouse SET warehouse_name = 'UPS_NEW' WHERE warehouse_id = 1;

db.warehouse.update
(
	{
		"warehouse_id" : 1
	},
	{
		"warehouse_id" : 1,
		"warehouse_name" : "UPS_NEW",
		"warehouse_created" : ISODate("2014-12-12T07:12:10Z")
	}
)

-- explain a table
EXPLAIN SELECT * FROM warehouse WHERE warehouse_id = 1;
                           QUERY PLAN
-----------------------------------------------------------------
 Foreign Scan on warehouse  (cost=0.00..0.00 rows=1000 width=84)
   Filter: (warehouse_id = 1)
   Foreign Namespace: db.warehouse
(3 rows)

-- collect data distribution statistics
ANALYZE warehouse;

```

Example for LEFT JOIN with NULL values behavior between PostgreSQL and MongoDB.

PostgreSQL treats a NULL value is larger than a non-NULL value<br>
but a NULL value is smaller than a non-NULL value in MongoDB.
```sql
-- The prepared data on PostgreSQL
SELECT * FROM postgres_t1;
 i | j |   t   
---+---+-------
 1 | 4 | one
 2 | 3 | two
 3 | 2 | three
 4 | 1 | for
 5 | 0 | five
 6 | 7 | six
 7 | 7 | seven
 8 | 8 | eight
 0 |   | zero
   |   | null
   | 0 | zero
(11 rows)

SELECT * FROM postgres_t2;
 i | k  
---+----
 1 | -1
 2 |  2
 3 | -3
 2 |  4
 5 | -5
 5 | -5
 0 |   
   |   
   |  0
(9 rows)

-- The JOIN result with NULL compare in PostgreSQL:
SELECT *
  FROM postgres_t1 LEFT JOIN postgres_t2 USING (i);
 i | j |   t   | k  
---+---+-------+----
 1 | 4 | one   | -1
 2 | 3 | two   |  2
 3 | 2 | three | -3
 2 | 3 | two   |  4
 5 | 0 | five  | -5
 5 | 0 | five  | -5
 0 |   | zero  |   
   | 0 | zero  |   
   |   | null  |   
 8 | 8 | eight |   
 6 | 7 | six   |   
 7 | 7 | seven |   
 4 | 1 | for   |   
(13 rows)
```
```batch
# The prepared data on MongoDB:
> db.mongo_t1.find();
{ "i" : 1, "j" : 4, "t" : "one" }
{ "i" : 2, "j" : 3, "t" : "two" }
{ "i" : 3, "j" : 2, "t" : "three" }
{ "i" : 4, "j" : 1, "t" : "for" }
{ "i" : 5, "j" : 0, "t" : "five" }
{ "i" : 6, "j" : 7, "t" : "six" }
{ "i" : 7, "j" : 7, "t" : "seven" }
{ "i" : 8, "j" : 8, "t" : "eight" }
{ "i" : 0, "j" : null, "t" : "zero" }
{ "i" : null, "j" : null, "t" : "null" }
{ "i" : null, "j" : 0, "t" : "zero" }

> db.mongo_t2.find();
{ "i" : 1, "k" : -1 }
{ "i" : 2, "k" : 2 }
{ "i" : 3, "k" : -3 }
{ "i" : 2, "k" : 4 }
{ "i" : 5, "k" : -5 }
{ "i" : 5, "k" : -5 }
{ "i" : 0, "k" : null }
{ "i" : null, "k" : null }
{ "i" : null, "k" : 0 }

# The JOIN result with NULL compare in MongoDB:
db.mongo_t1.aggregate([
    {
        $lookup:
        {
            from: "mongo_t2",
            let: {ref1: "$i"},
            pipeline:
            [
                {
                    $match:
                    {
                        $expr:
                        {
                            $eq:["$i", "$$ref1"]
                        }
                    }
                },
                {
                    $project: {_id: 0}
                }
            ],
            as: "mongo_t2" 
        }
    },
    {
        $unwind: "$mongo_t2" 
    },
    {
        $project:
        {
            _id: 0
        }
    }
])

{ "i" : 1, "j" : 4, "t" : "one", "mongo_t2" : { "i" : 1, "k" : -1 } }
{ "i" : 2, "j" : 3, "t" : "two", "mongo_t2" : { "i" : 2, "k" : 2 } }
{ "i" : 2, "j" : 3, "t" : "two", "mongo_t2" : { "i" : 2, "k" : 4 } }
{ "i" : 3, "j" : 2, "t" : "three", "mongo_t2" : { "i" : 3, "k" : -3 } }
{ "i" : 5, "j" : 0, "t" : "five", "mongo_t2" : { "i" : 5, "k" : -5 } }
{ "i" : 5, "j" : 0, "t" : "five", "mongo_t2" : { "i" : 5, "k" : -5 } }
{ "i" : 0, "j" : null, "t" : "zero", "mongo_t2" : { "i" : 0, "k" : null } }
{ "i" : null, "j" : null, "t" : "null", "mongo_t2" : { "i" : null, "k" : null } }
{ "i" : null, "j" : null, "t" : "null", "mongo_t2" : { "i" : null, "k" : 0 } }
{ "i" : null, "j" : 0, "t" : "zero", "mongo_t2" : { "i" : null, "k" : null } }
{ "i" : null, "j" : 0, "t" : "zero", "mongo_t2" : { "i" : null, "k" : 0 } }
```
Based on returned results from PostgreSQL and MongoDB, the returned result in MongoDB contains the value has i = NULL (4 records) but PostgreSQL has 2 records. Two records in PostgreSQL are from LEFT JOIN outter relation.

Limitations
-----------

  * If the BSON document key contains uppercase letters or occurs within
    a nested document, `mongo_fdw` requires the corresponding column names
    to be declared in double quotes.

  * Note that PostgreSQL limits column names to 63 characters by
    default. If you need column names that are longer, you can increase the
    `NAMEDATALEN` constant in `src/include/pg_config_manual.h`, compile,
    and re-install.

  * Filter condition (WHERE/HAVING): Not support for the following cases:
      * WHERE clause is true/false or any column/expression with boolean type
      * WHERE clause containing arthmetic operator expression
      * WHERE clause containing comparing operator inside another comparison expression

  * If PostgreSQL query contains whole-row reference under an outer JOIN, mongo_fdw
    cannot support it, because there is no way to expose whole-row reference in BSON document.
    For example SQL:
    ```sql
    EXPLAIN VERBOSE select t1.c2, count(t2.*)
    from ft1 t1 left join ft1 t2 on (t1.c2 = t2.c1)
    group by t1.c2 order by 1;
    ```


Contributing
------------
Have a fix for a bug or an idea for a great new feature? Great! Check
out the contribution guidelines [here][3].


Support
-------
This project will be modified to maintain compatibility with new
PostgreSQL and EDB Postgres Advanced Server releases.

If you need commercial support, please contact the EnterpriseDB sales
team, or check whether your existing PostgreSQL support provider can
also support mongo_fdw.


License
-------
Portions Copyright (c) 2004-2021, EnterpriseDB Corporation.
Portions Copyright © 2012–2014 Citus Data, Inc.

This program is free software: you can redistribute it and/or modify it
under the terms of the GNU Lesser General Public License as published by
the Free Software Foundation, either version 3 of the License, or (at
your option) any later version.

See the [`LICENSE`][4] file for full details.

[1]: http://www.mongodb.com
[2]: https://github.com/enterprisedb/mongo_fdw/issues/new
[3]: CONTRIBUTING.md
[4]: LICENSE
[5]: https://docs.mongodb.com/v4.4/reference/bson-type-comparison-order/
[6]: https://www.postgresql.org/docs/current/functions-comparisons.html