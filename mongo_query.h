/*-------------------------------------------------------------------------
 *
 * mongo_query.h
 * 		FDW query handling for mongo_fdw
 *
 * Portions Copyright (c) 2012-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 2004-2021, EnterpriseDB Corporation.
 * Portions Copyright (c) 2012–2014 Citus Data, Inc.
 * Portions Copyright (c) 2021, TOSHIBA CORPORATION
 *
 * IDENTIFICATION
 * 		mongo_query.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MONGO_QUERY_H
#define MONGO_QUERY_H

#define NUMERICARRAY_OID 1231

bool AppendMongoValue(BSON *queryDocument,
					  const char *keyName,
					  Datum value,
					  bool isnull,
					  Oid id);

char *MongoOperatorName(const char *operatorName);
extern BSON* mongo_build_bson_query_document(EState *estate, TupleDesc tupdesc, MongoPlanerInfo *plannerInfo);
extern List *mongo_serialize_plannerInfoList (MongoPlanerInfo *plannerInfo);
extern MongoPlanerInfo *mongo_deserialize_plannerInfoList(List *plannerInfoList);
extern bool mongo_is_foreign_param(PlannerInfo *root,
							 RelOptInfo *baserel,
							 Expr *expr);
extern List *mongo_build_tlist_to_deparse(RelOptInfo *foreignrel);
extern List *mongo_pull_func_clause(Node *node);
extern bool mongo_tlist_has_jsonb_arrow_op(PlannerInfo *root, RelOptInfo *baserel, List *tlist);
#endif							/* MONGO_QUERY_H */
