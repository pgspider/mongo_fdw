/*-------------------------------------------------------------------------
 *
 * mongo_fdw.h
 * 		Foreign-data wrapper for remote MongoDB servers
 *
 * Portions Copyright (c) 2012-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 2004-2022, EnterpriseDB Corporation.
 * Portions Copyright (c) 2012–2014 Citus Data, Inc.
 * Portions Copyright (c) 2021, TOSHIBA CORPORATION
 *
 * IDENTIFICATION
 * 		mongo_fdw.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MONGO_FDW_H
#define MONGO_FDW_H

#include "config.h"
#include "mongo_wrapper.h"

#ifdef META_DRIVER
#include "mongoc.h"
#else
#include "mongo.h"
#endif
#include "access/reloptions.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_user_mapping.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "fmgr.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "nodes/makefuncs.h"
#include "nodes/pg_list.h"
#if PG_VERSION_NUM < 120000
#include "nodes/relation.h"
#endif
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/plancat.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

#ifdef META_DRIVER
#define BSON bson_t
#define BSON_TYPE 							bson_type_t
#define BSON_ITERATOR 						bson_iter_t
#define MONGO_CONN 							mongoc_client_t
#define MONGO_CURSOR 						mongoc_cursor_t
#define BSON_TYPE_DOCUMENT 					BSON_TYPE_DOCUMENT
#define BSON_TYPE_NULL 						BSON_TYPE_NULL
#define BSON_TYPE_ARRAY 					BSON_TYPE_ARRAY
#define BSON_TYPE_INT32 					BSON_TYPE_INT32
#define BSON_TYPE_INT64						BSON_TYPE_INT64
#define BSON_TYPE_DOUBLE 					BSON_TYPE_DOUBLE
#define BSON_TYPE_BINDATA 					BSON_TYPE_BINARY
#define BSON_TYPE_BOOL 						BSON_TYPE_BOOL
#define BSON_TYPE_UTF8 						BSON_TYPE_UTF8
#define BSON_TYPE_OID 						BSON_TYPE_OID
#define BSON_TYPE_DATE_TIME 				BSON_TYPE_DATE_TIME
#define BSON_TYPE_SYMBOL 					BSON_TYPE_SYMBOL
#define BSON_TYPE_UNDEFINED 				BSON_TYPE_UNDEFINED
#define BSON_TYPE_REGEX 					BSON_TYPE_REGEX
#define BSON_TYPE_CODE 						BSON_TYPE_CODE
#define BSON_TYPE_CODEWSCOPE 				BSON_TYPE_CODEWSCOPE
#define BSON_TYPE_TIMESTAMP 				BSON_TYPE_TIMESTAMP

#define PREF_READ_PRIMARY_NAME 				"readPrimary"
#define PREF_READ_SECONDARY_NAME 			"readSecondary"
#define PREF_READ_PRIMARY_PREFERRED_NAME 	"readPrimaryPreferred"
#define PREF_READ_SECONDARY_PREFERRED_NAME  "readSecondaryPreferred"
#define PREF_READ_NEAREST_NAME 				"readNearest"

#define BSON_ITER_BOOL 						bson_iter_bool
#define BSON_ITER_DOUBLE 					bson_iter_double
#define BSON_ITER_INT32 					bson_iter_int32
#define BSON_ITER_INT64 					bson_iter_int64
#define BSON_ITER_OID 						bson_iter_oid
#define BSON_ITER_UTF8 						bson_iter_utf8
#define BSON_ITER_REGEX 					bson_iter_regex
#define BSON_ITER_DATE_TIME 				bson_iter_date_time
#define BSON_ITER_CODE 						bson_iter_code
#define BSON_ITER_VALUE 					bson_iter_value
#define BSON_ITER_KEY 						bson_iter_key
#define BSON_ITER_NEXT 						bson_iter_next
#define BSON_ITER_TYPE 						bson_iter_type
#define BSON_ITER_BINARY 					bson_iter_binary
#else
#define BSON 								bson
#define BSON_TYPE 							bson_type
#define BSON_ITERATOR 						bson_iterator
#define MONGO_CONN 							mongo
#define MONGO_CURSOR 						mongo_cursor
#define BSON_TYPE_DOCUMENT 					BSON_OBJECT
#define BSON_TYPE_NULL 						BSON_NULL
#define BSON_TYPE_ARRAY						BSON_ARRAY
#define BSON_TYPE_INT32 					BSON_INT
#define BSON_TYPE_INT64 					BSON_LONG
#define BSON_TYPE_DOUBLE 					BSON_DOUBLE
#define BSON_TYPE_BINDATA 					BSON_BINDATA
#define BSON_TYPE_BOOL 						BSON_BOOL
#define BSON_TYPE_UTF8 						BSON_STRING
#define BSON_TYPE_OID 						BSON_OID
#define BSON_TYPE_DATE_TIME 				BSON_DATE
#define BSON_TYPE_SYMBOL 					BSON_SYMBOL
#define BSON_TYPE_UNDEFINED 				BSON_UNDEFINED
#define BSON_TYPE_REGEX 					BSON_REGEX
#define BSON_TYPE_CODE 						BSON_CODE
#define BSON_TYPE_CODEWSCOPE 				BSON_CODEWSCOPE
#define BSON_TYPE_TIMESTAMP 				BSON_TIMESTAMP

#define BSON_ITER_BOOL 						bson_iterator_bool
#define BSON_ITER_DOUBLE 					bson_iterator_double
#define BSON_ITER_INT32 					bson_iterator_int
#define BSON_ITER_INT64 					bson_iterator_long
#define BSON_ITER_OID 						bson_iterator_oid
#define BSON_ITER_UTF8 						bson_iterator_string
#define BSON_ITER_REGEX 					bson_iterator_regex
#define BSON_ITER_DATE_TIME 				bson_iterator_date
#define BSON_ITER_CODE 						bson_iterator_code
#define BSON_ITER_VALUE 					bson_iterator_value
#define BSON_ITER_KEY 						bson_iterator_key
#define BSON_ITER_NEXT 						bson_iterator_next
#define BSON_ITER_TYPE 						bson_iterator_type
#define BSON_ITER_BINARY 					bson_iterator_bin_data
#endif

/* Defines for valid option names */
#define OPTION_NAME_ADDRESS					"address"
#define OPTION_NAME_PORT 					"port"
#define OPTION_NAME_DATABASE 				"database"
#define OPTION_NAME_COLLECTION 				"collection"
#define OPTION_NAME_USERNAME 				"username"
#define OPTION_NAME_PASSWORD 				"password"
#define OPTION_NAME_USE_REMOTE_ESTIMATE	    "use_remote_estimate"
#define OPTION_NAME_COLUMN_NAME				"column_name"
#ifdef META_DRIVER
#define OPTION_NAME_READ_PREFERENCE 		"read_preference"
#define OPTION_NAME_AUTHENTICATION_DATABASE "authentication_database"
#define OPTION_NAME_REPLICA_SET 			"replica_set"
#define OPTION_NAME_SSL 					"ssl"
#define OPTION_NAME_PEM_FILE 				"pem_file"
#define OPTION_NAME_PEM_PWD 				"pem_pwd"
#define OPTION_NAME_CA_FILE 				"ca_file"
#define OPTION_NAME_CA_DIR 					"ca_dir"
#define OPTION_NAME_CRL_FILE 				"crl_file"
#define OPTION_NAME_WEAK_CERT 				"weak_cert_validation"
#endif
#define OPTION_NAME_ENABLE_JOIN_PUSHDOWN	"enable_join_pushdown"

/* Default values for option parameters */
#define DEFAULT_IP_ADDRESS 					"127.0.0.1"
#define DEFAULT_PORT_NUMBER 				27017
#define DEFAULT_DATABASE_NAME 				"test"

/* Defines for sending queries and converting types */
#define EQUALITY_OPERATOR_NAME 				"="
#define INITIAL_ARRAY_CAPACITY 				8
#define MONGO_TUPLE_COST_MULTIPLIER 		5
#define MONGO_CONNECTION_COST_MULTIPLIER 	5
#define POSTGRES_TO_UNIX_EPOCH_DAYS 		(POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE)
#define POSTGRES_TO_UNIX_EPOCH_USECS 		(POSTGRES_TO_UNIX_EPOCH_DAYS * USECS_PER_DAY)

/* Macro for list API backporting. */
#if PG_VERSION_NUM < 130000
	#define mongo_list_concat(l1, l2) list_concat(l1, list_copy(l2))
#else
	#define mongo_list_concat(l1, l2) list_concat((l1), (l2))
#endif

/*
 * MongoValidOption keeps an option name and a context.  When an option is
 * passed into mongo_fdw objects (server and foreign table), we compare this
 * option's name and context against those of valid options.
 */
typedef struct MongoValidOption
{
	const char *optionName;
	Oid			optionContextId;
} MongoValidOption;

/* Array of options that are valid for mongo_fdw */
#ifdef META_DRIVER
static const uint32 ValidOptionCount = 20;
#else
static const uint32 ValidOptionCount = 8;
#endif
static const MongoValidOption ValidOptionArray[] =
{
	/* Foreign server options */
	{OPTION_NAME_ADDRESS, ForeignServerRelationId},
	{OPTION_NAME_PORT, ForeignServerRelationId},
	{OPTION_NAME_USE_REMOTE_ESTIMATE, ForeignServerRelationId},

#ifdef META_DRIVER
	{OPTION_NAME_READ_PREFERENCE, ForeignServerRelationId},
	{OPTION_NAME_AUTHENTICATION_DATABASE, ForeignServerRelationId},
	{OPTION_NAME_REPLICA_SET, ForeignServerRelationId},
	{OPTION_NAME_SSL, ForeignServerRelationId},
	{OPTION_NAME_PEM_FILE, ForeignServerRelationId},
	{OPTION_NAME_PEM_PWD, ForeignServerRelationId},
	{OPTION_NAME_CA_FILE, ForeignServerRelationId},
	{OPTION_NAME_CA_DIR, ForeignServerRelationId},
	{OPTION_NAME_CRL_FILE, ForeignServerRelationId},
	{OPTION_NAME_WEAK_CERT, ForeignServerRelationId},
#endif
	{OPTION_NAME_ENABLE_JOIN_PUSHDOWN, ForeignServerRelationId},

	/* Foreign table options */
	{OPTION_NAME_DATABASE, ForeignTableRelationId},
	{OPTION_NAME_COLLECTION, ForeignTableRelationId},
	{OPTION_NAME_ENABLE_JOIN_PUSHDOWN, ForeignTableRelationId},

	/* Column option */
	{OPTION_NAME_COLUMN_NAME, AttributeRelationId},

	/* User mapping options */
	{OPTION_NAME_USERNAME, UserMappingRelationId},
	{OPTION_NAME_PASSWORD, UserMappingRelationId}
};

/*
 * MongoFdwOptions holds the option values to be used when connecting to Mongo.
 * To resolve these values, we first check foreign table's options, and if not
 * present, we then fall back to the default values specified above.
 */
typedef struct MongoFdwOptions
{
	char	   *svr_address;
	uint16		svr_port;
	char	   *svr_database;
	char	   *collectionName;
	char	   *column_name;
	char	   *svr_username;
	char	   *svr_password;
	bool		use_remote_estimate;	/* use remote estimate for rows */
	bool        enable_join_pushdown;
#ifdef META_DRIVER
	char	   *readPreference;
	char	   *authenticationDatabase;
	char	   *replicaSet;
	bool		ssl;
	char	   *pem_file;
	char	   *pem_pwd;
	char	   *ca_file;
	char	   *ca_dir;
	char	   *crl_file;
	bool		weak_cert_validation;
#endif
} MongoFdwOptions;

typedef struct MongoPlanerJoinInfo
{
	Index		outerrel_relid;	/* Index of outer relation in range table entry */
	Index		innerrel_relid; /* Index of inner relation in range table entry */
	/* joinclauses contains only JOIN/ON conditions for an outer join */
	List	   *joinclauses;	/* List of RestrictInfo */
	char	   *innerel_name;	/* Name of inner relation */
	char	   *outerrel_name;	/* Name of outer relation */
	Oid			outerrel_oid;	/* Outer relation oid */
	Oid			innerrel_oid;	/* Inner relation oid */
	bool		join_is_sub_query;	/* If join relation is in sub query */
	RTEKind		outerrel_rtekind;	/* Outer relation of RangeTblEntry node */
	RTEKind		innerrel_rtekind;	/* Inner relation of RangeTblEntry node */
	char	   *outerrel_aliasname;	/* Alias name of outer relation */
	char	   *innerrel_aliasname;	/* Alias name of inner relation */
} MongoPlanerJoinInfo;

/*
 * The planner information is passed to execution stage
 * to build query document.
 */
typedef struct MongoPlanerInfo
{
	List	   *tlist;	/* Target list */
	List	   *retrieved_attrs;

	bool	   tlist_has_jsonb_arrow_op; /* True if tlist has jsonb arrow operator pushdown */

	Index	   rtindex;			/* Index of relation in range table entry */
	Oid		   rel_oid;			/* OID of the relation */
	RelOptKind reloptkind;		/* Relation kind of the foreign relation we are planning for */

	List	   *remote_exprs;	/* Remote conditions are applied for WHERE */
	List	   *local_exprs;	/* Local conditions are applied for WHERE */
	List	   *having_quals;	/* qualifications applied for HAVING to groups */

	bool	   has_limit;		/* Has LIMIT query */
	Node	   *limitOffset;	/* # of result tuples to skip (int8 expr) */
	Node	   *limitCount;		/* # of result tuples to return (int8 expr) */

	/* Bitmap of attr numbers we need to fetch from the remote server. */
	Bitmapset *attrs_used;

	/* List expression to be computed by Pathtarget */
	List	   *ptarget_exprs;

	bool	   has_groupClause;	/* True if having GROUP clause */
	bool	   has_grouping_agg;	/* True if query has having GROUP clause, aggregation */

	RelOptKind scan_reloptkind;	/* Relation kind of the underlying scan relation. Same as
								 * reloptkind, when that represents a join or
								 * a base relation. */
	/* JOIN information */
	JoinType   jointype;
	int		   joininfo_num;	/* Length of joininfo_list */
	List	   *joininfo_list;	/* This is list of join information that contains MongoPlanerJoinInfo */
} MongoPlanerInfo;

/*
 * MongoFdwExecState keeps foreign data wrapper specific execution state that
 * we create and hold onto when executing the query.
 *
 * Execution state of a foreign insert/update/delete operation.
 */
typedef struct MongoFdwModifyState
{
	Relation	rel;			/* relcache entry for the foreign table */
	List	   *target_attrs;	/* list of target attribute numbers */

	/* Info about parameters for prepared statement */
	int			p_nums;			/* number of parameters to transmit */
	FmgrInfo   *p_flinfo;		/* output conversion functions for them */

	MONGO_CONN *mongoConnection;	/* MongoDB connection */
	MONGO_CURSOR *mongoCursor;	/* MongoDB cursor */
	BSON	   *queryDocument;	/* Bson Document */

	MongoFdwOptions *options;
	AttrNumber	rowidAttno; 	/* attnum of resjunk rowid column */
} MongoFdwModifyState;

/*
 * Execution state of a foreign scan using mongo_fdw.
 */
typedef struct MongoFdwScanState
{
	Relation	rel;			/* relcache entry for the foreign table */

	MONGO_CONN *mongoConnection;	/* MongoDB connection */
	MONGO_CURSOR *mongoCursor;	/* MongoDB cursor */
	BSON	   *queryDocument;	/* Bson Document */

	MongoFdwOptions *options;

	/* All necessary planner information to build query document */
	MongoPlanerInfo *plannerInfo;
} MongoFdwScanState;

/*
 * ColumnMapping represents a hash table entry that maps a column name to
 * column-related information.  We construct these hash table entries to speed
 * up the conversion from BSON documents to PostgreSQL tuples, and each hash
 * entry maps the column name to the column's tuple index and its type-related
 * information.
 */
typedef struct ColumnMapping
{
	char		columnName[NAMEDATALEN];
	uint32		columnIndex;
	Oid			columnTypeId;
	int32		columnTypeMod;
	Oid			columnArrayTypeId;
} ColumnMapping;

/*
 * FDW-specific planner information kept in RelOptInfo.fdw_private for a
 * mongo_fdw foreign table.  For a baserel, this struct is created by
 * MongoGetForeignRelSize.
 */
typedef struct MongoFdwRelationInfo
{
	/*
	 * True means that the relation can be pushed down. Always true for simple
	 * foreign scan.
	 */
	bool	   pushdown_safe;

	/* baserestrictinfo clauses, broken down into safe and unsafe subsets. */
	List	   *local_conds;
	List	   *remote_conds;

	/* Join information */
	RelOptInfo *outerrel;
	RelOptInfo *innerrel;
	JoinType	jointype;
	/* joinclauses contains only JOIN/ON conditions for an outer join */
	List	   *joinclauses;	/* List of RestrictInfo */
	Oid			baserel_oid;	/* Base relation Oid, only set for base relation */
	Oid			outerrel_oid;	/* Outer relation Oid */
	Oid			innerrel_oid;	/* Inner relation Oid */
	bool		join_is_sub_query;	/* Mark if the join clause is planned in sub query */

	/* Upper relation information */
	UpperRelationKind stage;

	/* Grouping information */
	List	   *grouped_tlist;

	/*
	 * Index of the relation.  It is used to create an alias to a subquery
	 * representing the relation.
	 */
	int			relation_index;
	MongoFdwOptions *options;  /* Options applicable for this relation */
} MongoFdwRelationInfo;

/* options.c */
extern MongoFdwOptions *mongo_get_options(Oid foreignTableId, Oid userid);
extern void mongo_free_options(MongoFdwOptions *options);
extern StringInfo mongo_option_names_string(Oid currentContextId);

/* connection.c */
MONGO_CONN *mongo_get_connection(ForeignServer *server,
								 UserMapping *user,
								 MongoFdwOptions *opt);

extern void mongo_cleanup_connection(void);
extern void mongo_release_connection(MONGO_CONN *conn);

/* Function declarations related to creating the mongo query */
extern List *mongo_get_column_list(PlannerInfo *root, RelOptInfo *foreignrel,
								   List *scan_var_list);
extern bool mongo_is_foreign_expr(PlannerInfo *root, RelOptInfo *baserel,
								  Expr *expression);

/* Function declarations for foreign data wrapper */
extern Datum mongo_fdw_handler(PG_FUNCTION_ARGS);
extern Datum mongo_fdw_validator(PG_FUNCTION_ARGS);

extern int mongo_set_transmission_modes(void);
extern void mongo_reset_transmission_modes(int nestlevel);

/* deparse.c headers */
extern const char *mongo_get_jointype_name(JoinType jointype);
extern void mongo_add_null_check_ref(char *ref_name, BSON *expr);
extern void mongo_add_null_check_var(Var *column, BSON *qdoc, Oid rel_oid);
#endif							/* MONGO_FDW_H */
