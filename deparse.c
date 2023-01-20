/*-------------------------------------------------------------------------
 *
 * deparse.c
 * 		Query deparser for mongo_fdw
 *
 * Portions Copyright (c) 2012-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 2004-2022, EnterpriseDB Corporation.
 * Portions Copyright (c) 2012–2014 Citus Data, Inc.
 *
 * IDENTIFICATION
 * 		deparse.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "mongo_wrapper.h"

/*
 * mongo_get_jointype_name
 * 		Output join name for given join type
 */
const char *
mongo_get_jointype_name(JoinType jointype)
{
	switch (jointype)
	{
		case JOIN_INNER:
			return "INNER";

		case JOIN_LEFT:
			return "LEFT";

		case JOIN_RIGHT:
			return "RIGHT";

		default:
			/* Shouldn't come here, but protect from buggy code. */
			elog(ERROR, "unsupported join type %d", jointype);
	}

	/* Keep compiler happy */
	return NULL;
}

/*
 * mongo_add_null_check_ref
 *		Add null check for reference
 */
void mongo_add_null_check_ref(char *ref_name, BSON *qdoc)
{
	BSON        ne_expr,ref_doc;

	bsonAppendStartObject (qdoc, "$epxr", &ref_doc);
	bsonAppendStartArray(&ref_doc, "$ne", &ne_expr);

	bsonAppendUTF8(&ne_expr, "0", ref_name);
	bsonAppendNull(&ne_expr, "1");

	bsonAppendFinishArray(&ref_doc, &ne_expr);
	bsonAppendFinishObject (qdoc, &ref_doc);
}

/*
 * mongo_add_null_check_var
 *		Add null check for column
 */
void mongo_add_null_check_var(Var *node, BSON *qdoc, Oid rel_oid)
{
	BSON        ne_expr,ref_doc;
	char	   	*colname = NULL;

	bsonAppendStartObject(qdoc, "$epxr", &ref_doc);
	bsonAppendStartArray(&ref_doc, "$ne", &ne_expr);

	colname = get_attname(rel_oid, node->varattno, false);
	/* Build colname object like "$column" */
	colname = psprintf("$%s", colname);

	bsonAppendUTF8(&ne_expr, "0", colname);
	bsonAppendNull(&ne_expr, "1");

	bsonAppendFinishArray(&ref_doc, &ne_expr);
	bsonAppendFinishObject (qdoc, &ref_doc);
}
