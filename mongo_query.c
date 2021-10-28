/*-------------------------------------------------------------------------
 *
 * mongo_query.c
 * 		FDW query handling for mongo_fdw
 *
 * Portions Copyright (c) 2012-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 2004-2021, EnterpriseDB Corporation.
 * Portions Copyright (c) 2012–2014 Citus Data, Inc.
 * Portions Copyright (c) 2021, TOSHIBA CORPORATION
 *
 * IDENTIFICATION
 * 		mongo_query.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "mongo_wrapper.h"

#include <bson.h>
#include <json.h>

#if PG_VERSION_NUM < 120000
#include "access/sysattr.h"
#endif
#if PG_VERSION_NUM >= 120000
#include "access/table.h"
#endif
#include "catalog/heap.h"
#include "catalog/pg_collation.h"
#ifdef META_DRIVER
#include "mongoc.h"
#else
#include "mongo.h"
#endif
#include "mongo_query.h"
#if PG_VERSION_NUM < 120000
#include "nodes/relation.h"
#include "optimizer/var.h"
#endif
#if PG_VERSION_NUM >= 120000
#include "optimizer/optimizer.h"
#endif
#include "parser/parsetree.h"
#include "access/table.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_aggregate.h"
#include "optimizer/tlist.h"
#include "utils/builtins.h"
#include "nodes/nodeFuncs.h"
#include "utils/syscache.h"
#include "utils/rel.h"

/*
 * Global context for foreign_expr_walker's search of an expression tree.
 */
typedef struct foreign_glob_cxt
{
	PlannerInfo *root;			/* global planner state */
	RelOptInfo *foreignrel;		/* the foreign relation we are planning for */
	unsigned short varcount;	/* Var count */
	unsigned short opexprcount;
	Relids		relids;			/* relids of base relations in the underlying
								 * scan */
	bool		has_compare_op;	/* True if having comparing opertator */
} foreign_glob_cxt;

/*
 * Local (per-tree-level) context for foreign_expr_walker's search.
 * This is concerned with identifying collations used in the expression.
 */
typedef enum
{
	FDW_COLLATE_NONE,			/* expression is of a noncollatable type */
	FDW_COLLATE_SAFE,			/* collation derives from a foreign Var */
	FDW_COLLATE_UNSAFE			/* collation derives from something else */
} FDWCollateState;

typedef struct foreign_loc_cxt
{
	Oid			collation;		/* OID of current collation, if any */
	FDWCollateState state;		/* state of current collation choice */
	bool		has_scalar_array_op_expr; /* True if expression is ScalarArrayOpExpr */
} foreign_loc_cxt;

typedef enum MongoOperatorsSupport
{
	OP_CONDITIONAL = 1,
	OP_MATH = 2,
	OP_JSON = 3,
	OP_UNSUPPORT = 4,
} MongoOperatorsSupport;

typedef struct mongo_target_ref
{
	Expr		*expr;				/* Target that being requested */
	bool		is_group_target;	/* True if target is in group clause */
	int			target_idx;			/* Refer to target index */
} mongo_target_ref;

typedef struct mongo_aggref_ref
{
	Expr		*expr;			/* Aggregate functions in HAVING */
	char		*ref_target;	/* Refer to target index */
} mongo_aggref_ref;

/*
 * Context for build expression query document.
 */
typedef struct qdoc_expr_cxt
{
	EState		*estate;			/* Executor state */

	Oid			rel_oid;			/* OID of the relation */
	Index		rtindex;
	int			conds_num;			/* Number of remote conditions */
	RelOptKind	reloptkind;			/* Relation kind of the foreign relation we are planning for */
	RelOptKind	scan_reloptkind;	/* Relation kind of the underlying scan relation */
	List		*target_ref_list;	/* Refer to target list */
	bool		has_groupClause;	/* True if having GROUP clause */
	bool		has_grouping_agg;	/* True if query has having GROUP clause, aggregation */
	List		*agg_ref_list;		/* Refer to aggregate functions in HAVING clause */

	char		*bs_key;			/* BSON key for BSON object */
	bool		need_aggexpr_syntax;	/* Expression need to be built in aggregate expression syntax */
	int			count_boolexpr;		/* Counting level of booling expressions in a certain context */

	char		*innerel_name;		/* Name of inner relation */
	char		*outerrel_name;		/* Name of outer relation */
	List		*innerel_name_list; /* List name of inner relation */
} qdoc_expr_cxt;

typedef struct deparse_expr_cxt
{
	StringInfo	buf;			/* output buffer to append to */
	qdoc_expr_cxt *qdoc_ctx;	/* Query document context */
} deparse_expr_cxt;

/*
 * Struct to pull out aggregate function
 */
typedef struct pull_aggref_list_context
{
	List	   *aggref_list;
} pull_aggref_list_context;

/* Local functions forward declarations */
static Expr *FindArgumentOfType(List *argumentList, NodeTag argumentType);
static List *EqualityOperatorList(List *operatorList);
static List *UniqueColumnList(List *operatorList);
static List *ColumnOperatorList(Var *column, List *operatorList);
static void AppendConstantValue(BSON *queryDocument, const char *keyName,
								Const *constant);
static void AppendParamValue(BSON *queryDocument, const char *keyName,
							 Param *paramNode,
							 ForeignScanState *scanStateNode);
static bool foreign_expr_walker(Node *node,
								foreign_glob_cxt *glob_cxt,
								foreign_loc_cxt *outer_cxt);
static List *prepare_var_list_for_baserel(Oid relid, Index varno,
										  Bitmapset *attrs_used);
static const char * mongo_getSwitchedCmpOperatorName(const char *opname, bool need_switch_operator);
static MongoOperatorsSupport mongo_validateOperatorName(Oid opno,
												  const char **deparseName,
												  bool *is_switch_operator);
static void mongo_aggregate_pipeline_query(EState *estate, TupleDesc tupdesc,
											MongoPlanerInfo *plannerInfo,
											qdoc_expr_cxt *context,
											BSON *queryDocument);
static void mongo_append_lookup_doc(TupleDesc tupdesc,
									BSON *pipeline,
									List *tlist,
									RelOptKind scan_reloptkind,
									MongoPlanerJoinInfo *join_info,
									qdoc_expr_cxt *context);
static void mongo_append_grouping_doc(TupleDesc tupdesc, BSON *pipeline, MongoPlanerInfo *plannerInfo, qdoc_expr_cxt *context);
static void mongo_append_target_list_doc(TupleDesc tupdesc, BSON *pipeline, MongoPlanerInfo *plannerInfo, qdoc_expr_cxt *context);
static void mongo_append_filter_doc(BSON *pipeline, MongoPlanerInfo *plannerInfo, qdoc_expr_cxt *context);
static void mongo_build_expr_doc(BSON *qdoc, Expr *node, qdoc_expr_cxt *context);
static void mongo_deparseExpr(Expr *node, deparse_expr_cxt *deparse_context);
static void mongo_deparseRelation(StringInfo buf, Relation rel);
static void mongo_get_func_info_scalar_array (Oid const_array_type, Oid *consttype, PGFunction *func_addr);

/*
 * FindArgumentOfType
 *		Walks over the given argument list, looks for an argument with the
 *		given type, and returns the argument if it is found.
 */
static Expr *
FindArgumentOfType(List *argumentList, NodeTag argumentType)
{
	Expr	   *foundArgument = NULL;
	ListCell   *argumentCell;

	foreach(argumentCell, argumentList)
	{
		Expr	   *argument = (Expr *) lfirst(argumentCell);

		/* For RelabelType type, examine the inner node */
		if (IsA(argument, RelabelType))
			argument = ((RelabelType *) argument)->arg;

		if (nodeTag(argument) == argumentType)
		{
			foundArgument = argument;
			break;
		}
	}

	return foundArgument;
}

/*
 * QueryDocument
 *		Takes in the applicable operator expressions for a relation and
 *		converts these expressions into equivalent queries in MongoDB.
 *
 * For now, this function can only transform simple comparison expressions, and
 * returns these transformed expressions in a BSON document.  For example,
 * simple expressions:
 * "l_shipdate >= date '1994-01-01' AND l_shipdate < date '1995-01-01'" become
 * "l_shipdate: { $gte: new Date(757382400000), $lt: new Date(788918400000) }".
 */
BSON *
QueryDocument(Oid relationId, List *opExpressionList,
			  ForeignScanState *scanStateNode)
{
	List	   *equalityOperatorList;
	List	   *comparisonOperatorList;
	List	   *columnList;
	ListCell   *equalityOperatorCell;
	ListCell   *columnCell;
	BSON	   *queryDocument = BsonCreate();

	/*
	 * We distinguish between equality expressions and others since we need to
	 * insert the latter (<, >, <=, >=, <>) as separate sub-documents into the
	 * BSON query object.
	 */
	equalityOperatorList = EqualityOperatorList(opExpressionList);
	comparisonOperatorList = list_difference(opExpressionList,
											 equalityOperatorList);

	/* Append equality expressions to the query */
	foreach(equalityOperatorCell, equalityOperatorList)
	{
		OpExpr	   *equalityOperator = (OpExpr *) lfirst(equalityOperatorCell);
		Oid			columnId = InvalidOid;
		char	   *columnName;
		Const	   *constant;
		Param	   *paramNode;
		List	   *argumentList = equalityOperator->args;
		Var		   *column = (Var *) FindArgumentOfType(argumentList, T_Var);

		constant = (Const *) FindArgumentOfType(argumentList, T_Const);
		paramNode = (Param *) FindArgumentOfType(argumentList, T_Param);

		columnId = column->varattno;
#if PG_VERSION_NUM < 110000
		columnName = get_relid_attribute_name(relationId, columnId);
#else
		columnName = get_attname(relationId, columnId, false);
#endif

		if (constant != NULL)
			AppendConstantValue(queryDocument, columnName, constant);
		else
			AppendParamValue(queryDocument, columnName, paramNode,
							 scanStateNode);
	}

	/*
	 * For comparison expressions, we need to group them by their columns and
	 * append all expressions that correspond to a column as one sub-document.
	 *
	 * Otherwise, even when we have two expressions to define the upper- and
	 * lower-bound of a range, Mongo uses only one of these expressions during
	 * an index search.
	 */
	columnList = UniqueColumnList(comparisonOperatorList);

	/* Append comparison expressions, grouped by columns, to the query */
	foreach(columnCell, columnList)
	{
		Var		   *column = (Var *) lfirst(columnCell);
		Oid			columnId = InvalidOid;
		char	   *columnName;
		List	   *columnOperatorList;
		ListCell   *columnOperatorCell;
		BSON		childDocument;

		columnId = column->varattno;
#if PG_VERSION_NUM < 110000
		columnName = get_relid_attribute_name(relationId, columnId);
#else
		columnName = get_attname(relationId, columnId, false);
#endif

		/* Find all expressions that correspond to the column */
		columnOperatorList = ColumnOperatorList(column,
												comparisonOperatorList);

		/* For comparison expressions, start a sub-document */
		BsonAppendStartObject(queryDocument, columnName, &childDocument);

		foreach(columnOperatorCell, columnOperatorList)
		{
			OpExpr	   *columnOperator = (OpExpr *) lfirst(columnOperatorCell);
			char	   *operatorName;
			char	   *mongoOperatorName;
			List	   *argumentList = columnOperator->args;
			Const	   *constant = (Const *) FindArgumentOfType(argumentList,
																T_Const);

			operatorName = get_opname(columnOperator->opno);
			mongoOperatorName = MongoOperatorName(operatorName);
#ifdef META_DRIVER
			AppendConstantValue(&childDocument, mongoOperatorName, constant);
#else
			AppendConstantValue(queryDocument, mongoOperatorName, constant);
#endif
		}
		BsonAppendFinishObject(queryDocument, &childDocument);
	}

	if (!BsonFinish(queryDocument))
	{
#ifdef META_DRIVER
		ereport(ERROR,
				(errmsg("could not create document for query"),
				 errhint("BSON flags: %d", queryDocument->flags)));
#else
		ereport(ERROR,
				(errmsg("could not create document for query"),
				 errhint("BSON error: %d", queryDocument->err)));
#endif
	}

	return queryDocument;
}

/*
 * MongoOperatorName
 * 		Takes in the given PostgreSQL comparison operator name, and returns its
 * 		equivalent in MongoDB.
 */
char *
MongoOperatorName(const char *operatorName)
{
	const char *mongoOperatorName = NULL;
	const int32 nameCount = 5;
	static const char *nameMappings[][2] = {{"<", "$lt"},
	{">", "$gt"},
	{"<=", "$lte"},
	{">=", "$gte"},
	{"<>", "$ne"}};
	int32		nameIndex;

	for (nameIndex = 0; nameIndex < nameCount; nameIndex++)
	{
		const char *pgOperatorName = nameMappings[nameIndex][0];

		if (strncmp(pgOperatorName, operatorName, NAMEDATALEN) == 0)
		{
			mongoOperatorName = nameMappings[nameIndex][1];
			break;
		}
	}

	return (char *) mongoOperatorName;
}

/*
 * EqualityOperatorList
 *		Finds the equality (=) operators in the given list, and returns these
 *		operators in a new list.
 */
static List *
EqualityOperatorList(List *operatorList)
{
	List	   *equalityOperatorList = NIL;
	ListCell   *operatorCell;

	foreach(operatorCell, operatorList)
	{
		OpExpr	   *operator = (OpExpr *) lfirst(operatorCell);

		if (strncmp(get_opname(operator->opno), EQUALITY_OPERATOR_NAME,
					NAMEDATALEN) == 0)
			equalityOperatorList = lappend(equalityOperatorList, operator);
	}

	return equalityOperatorList;
}

/*
 * UniqueColumnList
 *		Walks over the given operator list, and extracts the column argument in
 *		each operator.
 *
 * The function then de-duplicates extracted columns, and returns them in a new
 * list.
 */
static List *
UniqueColumnList(List *operatorList)
{
	List	   *uniqueColumnList = NIL;
	ListCell   *operatorCell;

	foreach(operatorCell, operatorList)
	{
		OpExpr	   *operator = (OpExpr *) lfirst(operatorCell);
		List	   *argumentList = operator->args;
		Var		   *column = (Var *) FindArgumentOfType(argumentList, T_Var);

		/* List membership is determined via column's equal() function */
		uniqueColumnList = list_append_unique(uniqueColumnList, column);
	}

	return uniqueColumnList;
}

/*
 * ColumnOperatorList
 *		Finds all expressions that correspond to the given column, and returns
 *		them in a new list.
 */
static List *
ColumnOperatorList(Var *column, List *operatorList)
{
	List	   *columnOperatorList = NIL;
	ListCell   *operatorCell;

	foreach(operatorCell, operatorList)
	{
		OpExpr	   *operator = (OpExpr *) lfirst(operatorCell);
		List	   *argumentList = operator->args;
		Var		   *foundColumn = (Var *) FindArgumentOfType(argumentList,
															 T_Var);

		if (equal(column, foundColumn))
			columnOperatorList = lappend(columnOperatorList, operator);
	}

	return columnOperatorList;
}

static void
AppendParamValue(BSON *queryDocument, const char *keyName, Param *paramNode,
				 ForeignScanState *scanStateNode)
{
	ExprState  *param_expr;
	Datum		param_value;
	bool		isNull;
	ExprContext *econtext;

	if (scanStateNode == NULL)
		return;

	econtext = scanStateNode->ss.ps.ps_ExprContext;

	/* Prepare for parameter expression evaluation */
	param_expr = ExecInitExpr((Expr *) paramNode, (PlanState *) scanStateNode);

	/* Evaluate the parameter expression */
#if PG_VERSION_NUM >= 100000
	param_value = ExecEvalExpr(param_expr, econtext, &isNull);
#else
	param_value = ExecEvalExpr(param_expr, econtext, &isNull, NULL);
#endif

	AppendMongoValue(queryDocument, keyName, param_value, isNull,
					 paramNode->paramtype);
}

/*
 * AppendConstantValue
 *		Appends to the query document the key name and constant value.
 *
 * The function translates the constant value from its PostgreSQL type
 * to its MongoDB equivalent.
 */
static void
AppendConstantValue(BSON *queryDocument, const char *keyName, Const *constant)
{
	if (constant->constisnull)
	{
		BsonAppendNull(queryDocument, keyName);
		return;
	}

	AppendMongoValue(queryDocument, keyName, constant->constvalue, false,
					 constant->consttype);
}

bool
AppendMongoValue(BSON *queryDocument, const char *keyName, Datum value,
				 bool isnull, Oid id)
{
	bool		status = false;

	if (isnull)
	{
		status = BsonAppendNull(queryDocument, keyName);
		return status;
	}

	switch (id)
	{
		case INT2OID:
			{
				int16		valueInt = DatumGetInt16(value);

				status = BsonAppendInt32(queryDocument, keyName,
										 (int) valueInt);
			}
			break;
		case INT4OID:
			{
				int32		valueInt = DatumGetInt32(value);

				status = BsonAppendInt32(queryDocument, keyName, valueInt);
			}
			break;
		case INT8OID:
			{
				int64		valueLong = DatumGetInt64(value);

				status = BsonAppendInt64(queryDocument, keyName, valueLong);
			}
			break;
		case FLOAT4OID:
			{
				float4		valueFloat = DatumGetFloat4(value);

				status = BsonAppendDouble(queryDocument, keyName,
										  (double) valueFloat);
			}
			break;
		case FLOAT8OID:
			{
				float8		valueFloat = DatumGetFloat8(value);

				status = BsonAppendDouble(queryDocument, keyName, valueFloat);
			}
			break;
		case NUMERICOID:
			{
				Datum		valueDatum = DirectFunctionCall1(numeric_float8,
															 value);
				float8		valueFloat = DatumGetFloat8(valueDatum);

				status = BsonAppendDouble(queryDocument, keyName, valueFloat);
			}
			break;
		case BOOLOID:
			{
				bool		valueBool = DatumGetBool(value);

				status = BsonAppendBool(queryDocument, keyName,
										(int) valueBool);
			}
			break;
		case BPCHAROID:
		case VARCHAROID:
		case TEXTOID:
			{
				char	   *outputString;
				Oid			outputFunctionId;
				bool		typeVarLength;

				getTypeOutputInfo(id, &outputFunctionId, &typeVarLength);
				outputString = OidOutputFunctionCall(outputFunctionId, value);
				status = BsonAppendUTF8(queryDocument, keyName, outputString);
			}
			break;
		case BYTEAOID:
			{
				int			len;
				char	   *data;
				char	   *result = DatumGetPointer(value);

				if (VARATT_IS_1B(result))
				{
					len = VARSIZE_1B(result) - VARHDRSZ_SHORT;
					data = VARDATA_1B(result);
				}
				else
				{
					len = VARSIZE_4B(result) - VARHDRSZ;
					data = VARDATA_4B(result);
				}
#ifdef META_DRIVER
				if (strcmp(keyName, "_id") == 0)
				{
					bson_oid_t	oid;

					bson_oid_init_from_data(&oid, (const uint8_t *) data);
					status = BsonAppendOid(queryDocument, keyName, &oid);
				}
				else
					status = BsonAppendBinary(queryDocument, keyName, data,
											  len);
#else
				status = BsonAppendBinary(queryDocument, keyName, data, len);
#endif
			}
			break;
		case NAMEOID:
			{
				char	   *outputString;
				Oid			outputFunctionId;
				bool		typeVarLength;
				bson_oid_t	bsonObjectId;

				memset(bsonObjectId.bytes, 0, sizeof(bsonObjectId.bytes));
				getTypeOutputInfo(id, &outputFunctionId, &typeVarLength);
				outputString = OidOutputFunctionCall(outputFunctionId, value);
				BsonOidFromString(&bsonObjectId, outputString);
				status = BsonAppendOid(queryDocument, keyName, &bsonObjectId);
			}
			break;
		case DATEOID:
			{
				Datum		valueDatum = DirectFunctionCall1(date_timestamp,
															 value);
				Timestamp	valueTimestamp = DatumGetTimestamp(valueDatum);
				int64		valueMicroSecs = valueTimestamp + POSTGRES_TO_UNIX_EPOCH_USECS;
				int64		valueMilliSecs = valueMicroSecs / 1000;

				status = BsonAppendDate(queryDocument, keyName,
										valueMilliSecs);
			}
			break;
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			{
				Timestamp	valueTimestamp = DatumGetTimestamp(value);
				int64		valueMicroSecs = valueTimestamp + POSTGRES_TO_UNIX_EPOCH_USECS;
				int64		valueMilliSecs = valueMicroSecs / 1000;

				status = BsonAppendDate(queryDocument, keyName,
										valueMilliSecs);
			}
			break;
		case NUMERICARRAY_OID:
			{
				ArrayType  *array;
				Oid			elmtype;
				int16		elmlen;
				bool		elmbyval;
				char		elmalign;
				int			num_elems;
				Datum	   *elem_values;
				bool	   *elem_nulls;
				int			i;
				BSON		childDocument;

				array = DatumGetArrayTypeP(value);
				elmtype = ARR_ELEMTYPE(array);
				get_typlenbyvalalign(elmtype, &elmlen, &elmbyval, &elmalign);

				deconstruct_array(array, elmtype, elmlen, elmbyval, elmalign,
								  &elem_values, &elem_nulls, &num_elems);

				BsonAppendStartArray(queryDocument, keyName, &childDocument);
				for (i = 0; i < num_elems; i++)
				{
					Datum		valueDatum;
					float8		valueFloat;

					if (elem_nulls[i])
						continue;

					valueDatum = DirectFunctionCall1(numeric_float8,
													 elem_values[i]);
					valueFloat = DatumGetFloat8(valueDatum);
#ifdef META_DRIVER
					status = BsonAppendDouble(&childDocument, keyName,
											  valueFloat);
#else
					status = BsonAppendDouble(queryDocument, keyName,
											  valueFloat);
#endif
				}
				BsonAppendFinishArray(queryDocument, &childDocument);
				pfree(elem_values);
				pfree(elem_nulls);
			}
			break;
		case TEXTARRAYOID:
			{
				ArrayType  *array;
				Oid			elmtype;
				int16		elmlen;
				bool		elmbyval;
				char		elmalign;
				int			num_elems;
				Datum	   *elem_values;
				bool	   *elem_nulls;
				int			i;
				BSON		childDocument;

				array = DatumGetArrayTypeP(value);
				elmtype = ARR_ELEMTYPE(array);
				get_typlenbyvalalign(elmtype, &elmlen, &elmbyval, &elmalign);

				deconstruct_array(array, elmtype, elmlen, elmbyval, elmalign,
								  &elem_values, &elem_nulls, &num_elems);

				BsonAppendStartArray(queryDocument, keyName, &childDocument);
				for (i = 0; i < num_elems; i++)
				{
					char	   *valueString;
					Oid			outputFunctionId;
					bool		typeVarLength;

					if (elem_nulls[i])
						continue;

					getTypeOutputInfo(TEXTOID, &outputFunctionId,
									  &typeVarLength);
					valueString = OidOutputFunctionCall(outputFunctionId,
														elem_values[i]);
					status = BsonAppendUTF8(queryDocument, keyName,
											valueString);
				}
				BsonAppendFinishArray(queryDocument, &childDocument);
				pfree(elem_values);
				pfree(elem_nulls);
			}
			break;
		case JSONBOID:
		case JSONOID:
			{
				char	   *outputString;
				Oid			outputFunctionId;
				struct json_object *o;
				bool		typeVarLength;

				getTypeOutputInfo(id, &outputFunctionId, &typeVarLength);
				outputString = OidOutputFunctionCall(outputFunctionId, value);
				o = JsonTokenerPrase(outputString);

				if (o == NULL)
				{
					elog(WARNING, "cannot parse the document");
					status = 0;
					break;
				}

				status = JsonToBsonAppendElement(queryDocument, keyName, o);
			}
			break;
		default:
			/*
			 * We currently error out on other data types. Some types such as
			 * byte arrays are easy to add, but they need testing.
			 *
			 * Other types such as money or inet, do not have equivalents in
			 * MongoDB.
			 */
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
					 errmsg("cannot convert constant value to BSON value"),
					 errhint("Constant value data type: %u", id)));
			break;
	}

	return status;
}

/*
 * mongo_get_column_list
 *		Process scan_var_list to find all columns needed for query execution
 *		and return them.
 */
List *
mongo_get_column_list(PlannerInfo *root, RelOptInfo *foreignrel,
					  List *scan_var_list)
{
	List	   *columnList = NIL;
	ListCell   *lc;

	foreach(lc, scan_var_list)
	{
		Var		   *var = (Var *) lfirst(lc);

		Assert(IsA(var, Var));

		/* Var belongs to foreign table? */
		if (!bms_is_member(var->varno, foreignrel->relids))
			continue;

		/* Is whole-row reference requested? */
		if (var->varattno == 0)
		{
			List	   *wr_var_list;
			RangeTblEntry *rte = rt_fetch(var->varno, root->parse->rtable);
			Bitmapset  *attrs_used;

			Assert(OidIsValid(rte->relid));

			/*
			 * Get list of Var nodes for all undropped attributes of the base
			 * relation.
			 */
			attrs_used = bms_make_singleton(0 -
										 FirstLowInvalidHeapAttributeNumber);

			wr_var_list = prepare_var_list_for_baserel(rte->relid, var->varno,
													   attrs_used);
			columnList = list_concat_unique(columnList, wr_var_list);
			bms_free(attrs_used);
		}
		else
			columnList = list_append_unique(columnList, var);
	}

	return columnList;
}

/*
 * Check if expression is safe to execute remotely, and return true if so.
 *
 * In addition, *outer_cxt is updated with collation information.
 *
 * We must check that the expression contains only node types we can deparse,
 * that all types/operators are safe to send (which we approximate
 * as being built-in), and that all collations used in the expression derive
 * from Vars of the foreign table.
 *
 * We only support simple binary operators that compare a column against a
 * constant.  If the expression is a tree, we don't recurse into it.
 */
static bool
foreign_expr_walker(Node *node, foreign_glob_cxt *glob_cxt,
					foreign_loc_cxt *outer_cxt)
{
	foreign_loc_cxt inner_cxt;
	Oid			collation;
	FDWCollateState state;

	/* Need do nothing for empty subexpressions */
	if (node == NULL)
		return true;

	/* Set up inner_cxt for possible recursion to child nodes */
	inner_cxt.collation = InvalidOid;
	inner_cxt.state = FDW_COLLATE_NONE;
	inner_cxt.has_scalar_array_op_expr = false;

	switch (nodeTag(node))
	{
		case T_Var:
			{
				Var		   *var = (Var *) node;

				/* Increment the Var count */
				glob_cxt->varcount++;

				/*
				 * If the Var is from the foreign table, we consider its
				 * collation (if any) safe to use.  If it is from another
				 * table, we treat its collation the same way as we would a
				 * Param's collation, i.e. it's not safe for it to have a
				 * non-default collation.
				 */
				if (bms_is_member(var->varno, glob_cxt->relids) &&
					var->varlevelsup == 0)
				{
					/* Var belongs to foreign table */
					collation = var->varcollid;
					state = OidIsValid(collation) ? FDW_COLLATE_SAFE : FDW_COLLATE_NONE;
				}
				else
				{
					/* Var belongs to some other table */
					collation = var->varcollid;
					if (var->varcollid != InvalidOid &&
						var->varcollid != DEFAULT_COLLATION_OID)
						return false;

					if (collation == InvalidOid ||
						collation == DEFAULT_COLLATION_OID)
					{
						/*
						 * It's noncollatable, or it's safe to combine with a
						 * collatable foreign Var, so set state to NONE.
						 */
						state = FDW_COLLATE_NONE;
					}
					else
					{
						/*
						 * Do not fail right away, since the Var might appear
						 * in a collation-insensitive context.
						 */
						state = FDW_COLLATE_UNSAFE;
					}
				}
			}
			break;
		case T_Const:
			{
				Const	   *c = (Const *) node;

				/*
				 * We don't push down operators where the constant is an array,
				 * since conditional operators for arrays in MongoDB aren't
				 * properly defined.
				 */
				if ((!outer_cxt->has_scalar_array_op_expr) &&
					OidIsValid(get_element_type(c->consttype)))
					return false;

				/*
				 * If the constant has nondefault collation, either it's of a
				 * non-builtin type, or it reflects folding of a CollateExpr.
				 * It's unsafe to send to the remote unless it's used in a
				 * non-collation-sensitive context.
				 */
				collation = c->constcollid;
				if (collation == InvalidOid ||
					collation == DEFAULT_COLLATION_OID)
					state = FDW_COLLATE_NONE;
				else
					state = FDW_COLLATE_UNSAFE;
			}
			break;
		case T_Param:
			{
				Param	   *p = (Param *) node;

				/*
				 * Bail out on planner internal params. We could perhaps pass
				 * them to the remote server as regular params, but we don't
				 * have the machinery to do that at the moment.
				 */
				if (p->paramkind != PARAM_EXTERN)
					return false;

				/*
				 * Collation rule is same as for Consts and non-foreign Vars.
				 */
				collation = p->paramcollid;
				if (collation == InvalidOid ||
					collation == DEFAULT_COLLATION_OID)
					state = FDW_COLLATE_NONE;
				else
					state = FDW_COLLATE_UNSAFE;
			}
			break;
		case T_OpExpr:
			{
				OpExpr	   *oe = (OpExpr *) node;
				enum MongoOperatorsSupport opKind;
				ListCell *lc;
				bool has_compare_cols = false;

				/* Increment the operator expression count */
				glob_cxt->opexprcount++;

				/* Determine operators support */
				opKind = mongo_validateOperatorName(oe->opno, NULL, NULL);

				if (opKind == OP_UNSUPPORT)
					return false;
				else if (opKind == OP_CONDITIONAL)
				{
					/* Does not support comparing operation within comparing operation */
					if (glob_cxt->has_compare_op)
						return false;

					glob_cxt->has_compare_op = true;
				}
				else if (opKind == OP_MATH)
				{
					/* Does not support arithmetic operation within comparing operation */
					if (glob_cxt->has_compare_op)
					{
						glob_cxt->has_compare_op = false;
						return false;
					}
				}
				else if (opKind == OP_JSON)
				{
					/*
					 * MongoDB only support to extract an json object field,
					 * so the right operand of json arrow operator must be
					 * a contant with non-integer type.
					 */
					Expr *expr = (Expr *)lfirst(list_tail(oe->args));

					if (!IsA(expr, Const) ||
						(IsA(expr, Const) &&
						((Const *)expr)->consttype != TEXTOID))
						return false;
				}

				/*
				 * The operand must be either a column name or constant or aggregate function.
				 */
				foreach(lc, oe->args)
				{
					Node *n = (Node *) lfirst(lc);
					if (IsA(n, RelabelType) ||
						IsA(n, List) ||
						IsA(n, OpExpr) ||
						(IsA(n, Aggref) && opKind == OP_CONDITIONAL))
						continue;

					/* Does not support compare between columns if it is not in JOIN relation */
					if (IsA(n, Var))
					{
						if (has_compare_cols && !IS_JOIN_REL(glob_cxt->foreignrel))
							return false;
						else
						{
							if (opKind == OP_CONDITIONAL)
								has_compare_cols = true;
							continue;
						}
					}
					else if (IsA(n, Const))
					{
						Const *c = (Const *)n;

						/*
						 * Postgresql and MongoDB have different rules for
						 * comparing with JSON type, so we do not pushdown
						 * if there is any constant json value is compared.
						 */
						if (opKind == OP_CONDITIONAL &&
							(c->consttype == JSONBOID ||
							 c->consttype == JSONOID))
							return false;
						else
							continue;
					}

					return false;
				}

				/*
				 * Recurse to input subexpressions.
				 */
				if (!foreign_expr_walker((Node *) oe->args,
										 glob_cxt, &inner_cxt))
					return false;

				if (opKind != OP_JSON)
				{
					/*
					* If operator's input collation is not derived from a foreign
					* Var, it can't be sent to remote.
					*/
					if (oe->inputcollid == InvalidOid)
						/* OK, inputs are all noncollatable */ ;
					else if (inner_cxt.state != FDW_COLLATE_SAFE ||
							oe->inputcollid != inner_cxt.collation)
						return false;
				}

				/* Result-collation handling */
				collation = oe->opcollid;
				if (collation == InvalidOid)
					state = FDW_COLLATE_NONE;
				else if (inner_cxt.state == FDW_COLLATE_SAFE &&
						 collation == inner_cxt.collation)
					state = FDW_COLLATE_SAFE;
				else if (collation == DEFAULT_COLLATION_OID)
					state = FDW_COLLATE_NONE;
				else
					state = FDW_COLLATE_UNSAFE;
			}
			break;
		case T_RelabelType:
			{
				RelabelType *r = (RelabelType *) node;

				if (outer_cxt->has_scalar_array_op_expr)
					inner_cxt.has_scalar_array_op_expr = true;

				/*
				 * Recurse to input subexpression.
				 */
				if (!foreign_expr_walker((Node *) r->arg,
										 glob_cxt, &inner_cxt))
					return false;

				/*
				 * RelabelType must not introduce a collation not derived from
				 * an input foreign Var (same logic as for a real function).
				 */
				collation = r->resultcollid;
				if (collation == InvalidOid)
					state = FDW_COLLATE_NONE;
				else if (inner_cxt.state == FDW_COLLATE_SAFE &&
						 collation == inner_cxt.collation)
					state = FDW_COLLATE_SAFE;
				else if (collation == DEFAULT_COLLATION_OID)
					state = FDW_COLLATE_NONE;
				else
					state = FDW_COLLATE_UNSAFE;
			}
			break;
		case T_List:
			{
				List	   *l = (List *) node;
				ListCell   *lc;

				if (outer_cxt->has_scalar_array_op_expr)
					inner_cxt.has_scalar_array_op_expr = true;

				/*
				 * Recurse to component subexpressions.
				 *
				 * If comparison is between two columns of same table then we
				 * don't push down because currently building corresponding
				 * MongoDB query not possible with the help of MongoC driver.
				 */
				foreach(lc, l)
				{
					if ((!foreign_expr_walker((Node *) lfirst(lc),
											  glob_cxt, &inner_cxt)))
						return false;
				}

				/*
				 * When processing a list, collation state just bubbles up
				 * from the list elements.
				 */
				collation = inner_cxt.collation;
				state = inner_cxt.state;
			}
			break;
		case T_Aggref:
			{
				Aggref	   *agg = (Aggref *) node;
				ListCell   *lc;
				char	   *aggname = NULL;

				/* Not safe to pushdown when not in grouping context */
				if (!IS_UPPER_REL(glob_cxt->foreignrel))
					return false;

				/* Only non-split aggregates are pushable. */
				if (agg->aggsplit != AGGSPLIT_SIMPLE)
					return false;

				/* Does not support DISTINCT, ORDER BY, FILTER, VARIADIC
				 * inside aggregate function.
				 */
				if (agg->aggdistinct || agg->aggorder || agg->aggfilter || agg->aggvariadic)
					return false;

				if (AGGKIND_IS_ORDERED_SET(agg->aggkind))
					return false;

				/* Get function name */
				aggname = get_func_name(agg->aggfnoid);
				if (aggname == NULL)
					elog(ERROR, "No such function name for function OID %u", agg->aggfnoid);

				/* These functions can be passed to MongoDB */
				if (!(strcmp(aggname, "avg") == 0
					  || (strcmp(aggname, "count") == 0 && agg->aggstar)
					  || strcmp(aggname, "min") == 0
					  || strcmp(aggname, "max") == 0
					  || strcmp(aggname, "sum") == 0
					  || strcmp(aggname, "stddev") == 0
					  || strcmp(aggname, "stddev_pop") == 0
					  || strcmp(aggname, "stddev_samp") == 0))
				{
					return false;
				}

				foreach(lc, agg->args)
				{
					Node	   *n = (Node *) lfirst(lc);

					/* If TargetEntry, extract the expression from it */
					if (IsA(n, TargetEntry))
					{
						TargetEntry *tle = (TargetEntry *) n;

						n = (Node *) tle->expr;
					}

					if (!foreign_expr_walker(n, glob_cxt, &inner_cxt))
						return false;
				}

				/*
				 * If aggregate's input collation is not derived from a
				 * foreign Var, it can't be sent to remote.
				 */
				if (agg->inputcollid == InvalidOid)
					 /* OK, inputs are all noncollatable */ ;
				else if (inner_cxt.state != FDW_COLLATE_SAFE ||
						 agg->inputcollid != inner_cxt.collation)
					return false;

				/*
				 * Detect whether node is introducing a collation not derived
				 * from a foreign Var.  (If so, we just mark it unsafe for now
				 * rather than immediately returning false, since the parent
				 * node might not care.)
				 */
				collation = agg->aggcollid;
				if (collation == InvalidOid)
					state = FDW_COLLATE_NONE;
				else if (inner_cxt.state == FDW_COLLATE_SAFE &&
						 collation == inner_cxt.collation)
					state = FDW_COLLATE_SAFE;
				else if (collation == DEFAULT_COLLATION_OID)
					state = FDW_COLLATE_NONE;
				else
					state = FDW_COLLATE_UNSAFE;
			}
			break;
		case T_BoolExpr:
			{
				BoolExpr   *b = (BoolExpr *) node;
				ListCell   *lc;

				foreach(lc, b->args)
				{
					Node *n = (Node *) lfirst(lc);

					/*
					 * Does not support Var has boolean type
					 * e.g. WHERE c1::bool [ AND | OR ] { expression }.
					 */
					if (IsA(n, Var))
						return false;
				}

				/*
				 * Recurse to input subexpressions.
				 */
				if (!foreign_expr_walker((Node *) b->args,
										 glob_cxt, &inner_cxt))
					return false;

				/* Output is always boolean and so noncollatable. */
				collation = InvalidOid;
				state = FDW_COLLATE_NONE;
			}
			break;
		case T_NullTest:
			{
				NullTest   *nt = (NullTest *) node;

				/* Input expression only support column */
				if (!IsA(nt->arg, Var))
					return false;

				/*
				 * Recurse to input subexpressions.
				 */
				if (!foreign_expr_walker((Node *) nt->arg,
										 glob_cxt, &inner_cxt))
					return false;

				/* Output is always boolean and so noncollatable. */
				collation = InvalidOid;
				state = FDW_COLLATE_NONE;
			}
			break;
		case T_ScalarArrayOpExpr:
			{
				ScalarArrayOpExpr *oe = (ScalarArrayOpExpr *) node;
				char	   *opname = NULL;

				/* Sanity check. */
				Assert(list_length(oe->args) == 2);

				/* Get operator name */
				opname = get_opname(oe->opno);
				if (opname == NULL)
					elog(ERROR, "No such operator name for operator OID %u", oe->opno);

				/* Operator must be equal or not-equal operator. */
				if (!(strcmp(opname, "=") == 0 ||
					  strcmp(opname, "<>") == 0 ||
					  strcmp(opname, "!=") == 0))
					return false;

				inner_cxt.has_scalar_array_op_expr = true;

				/*
				 * Recurse to input subexpressions.
				 */
				if (!foreign_expr_walker((Node *) oe->args,
												glob_cxt, &inner_cxt))
					return false;

				/*
				 * If operator's input collation is not derived from a foreign
				 * Var, it can't be sent to remote.
				 */
				if (oe->inputcollid == InvalidOid)
					 /* OK, inputs are all noncollatable */ ;
				else if (inner_cxt.state != FDW_COLLATE_SAFE ||
						 oe->inputcollid != inner_cxt.collation)
					return false;

				/* Output is always boolean and so noncollatable. */
				collation = InvalidOid;
				state = FDW_COLLATE_NONE;
			}
			break;
		default:

			/*
			 * If it's anything else, assume it's unsafe.  This list can be
			 * expanded later, but don't forget to add deparse support.
			 */
			return false;
	}

	/*
	 * Now, merge my collation information into my parent's state.
	 */
	if (state > outer_cxt->state)
	{
		/* Override previous parent state */
		outer_cxt->collation = collation;
		outer_cxt->state = state;
	}
	else if (state == outer_cxt->state)
	{
		/* Merge, or detect error if there's a collation conflict */
		switch (state)
		{
			case FDW_COLLATE_NONE:
				/* Nothing + nothing is still nothing */
				break;
			case FDW_COLLATE_SAFE:
				if (collation != outer_cxt->collation)
				{
					/*
					 * Non-default collation always beats default.
					 */
					if (outer_cxt->collation == DEFAULT_COLLATION_OID)
					{
						/* Override previous parent state */
						outer_cxt->collation = collation;
					}
					else if (collation != DEFAULT_COLLATION_OID)
					{
						/*
						 * Conflict; show state as indeterminate.  We don't
						 * want to "return false" right away, since parent
						 * node might not care about collation.
						 */
						outer_cxt->state = FDW_COLLATE_UNSAFE;
					}
				}
				break;
			case FDW_COLLATE_UNSAFE:
				/* We're still conflicted ... */
				break;
		}
	}

	/* It looks OK */
	return true;
}

/*
 * mongo_is_foreign_expr
 *		Returns true if given expr is safe to evaluate on the foreign server.
 */
bool
mongo_is_foreign_expr(PlannerInfo *root, RelOptInfo *baserel, Expr *expression)
{
	foreign_glob_cxt glob_cxt;
	foreign_loc_cxt loc_cxt;
	MongoFdwRelationInfo *fpinfo = (MongoFdwRelationInfo *) (baserel->fdw_private);

	/*
	 * Check that the expression consists of nodes that are safe to execute
	 * remotely.
	 */
	glob_cxt.root = root;
	glob_cxt.foreignrel = baserel;
	glob_cxt.varcount = 0;
	glob_cxt.opexprcount = 0;
	/*
	 * For an upper relation, use relids from its underneath scan relation,
	 * because the upperrel's own relids currently aren't set to anything
	 * meaningful by the core code.  For other relation, use their own relids.
	 */
	if (IS_UPPER_REL(baserel))
		glob_cxt.relids = fpinfo->outerrel->relids;
	else
		glob_cxt.relids = baserel->relids;
	glob_cxt.has_compare_op = false;

	loc_cxt.collation = InvalidOid;
	loc_cxt.state = FDW_COLLATE_NONE;

	if (!foreign_expr_walker((Node *) expression, &glob_cxt, &loc_cxt))
		return false;

	/*
	 * If the expression has a valid collation that does not arise from a
	 * foreign var, the expression can not be sent over.
	 */
	if (loc_cxt.state == FDW_COLLATE_UNSAFE)
		return false;

	/* OK to evaluate on the remote server */
	return true;
}

/*
 * prepare_var_list_for_baserel
 *		Build list of nodes corresponding to the attributes requested for given
 *		base relation.
 *
 * The list contains Var nodes corresponding to the attributes specified in
 * attrs_used. If whole-row reference is required, add Var nodes corresponding
 * to all the attributes in the relation.
 */
static List *
prepare_var_list_for_baserel(Oid relid, Index varno, Bitmapset *attrs_used)
{
	int			attno;
	List	   *tlist = NIL;
	Node	   *node;
	bool		wholerow_requested = false;
	Relation	relation;
	TupleDesc	tupdesc;

	Assert(OidIsValid(relid));

	/* Planner must have taken a lock, so request no lock here */
#if PG_VERSION_NUM < 130000
	relation = heap_open(relid, NoLock);
#else
	relation = table_open(relid, NoLock);
#endif

	tupdesc = RelationGetDescr(relation);

	/* Is whole-row reference requested? */
	wholerow_requested = bms_is_member(0 - FirstLowInvalidHeapAttributeNumber,
									   attrs_used);

	/* Handle user defined attributes first. */
	for (attno = 1; attno <= tupdesc->natts; attno++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, attno - 1);

		/* Ignore dropped attributes. */
		if (attr->attisdropped)
			continue;

		/* For a required attribute create a Var node */
		if (wholerow_requested ||
			bms_is_member(attno - FirstLowInvalidHeapAttributeNumber,
						  attrs_used))
		{
			node = (Node *) makeVar(varno, attno, attr->atttypid,
									attr->atttypmod, attr->attcollation, 0);
			tlist = lappend(tlist, node);

		}
	}

#if PG_VERSION_NUM < 130000
	heap_close(relation, NoLock);
#else
	table_close(relation, NoLock);
#endif

	return tlist;
}

/*
 * Build a pipeline query document based on aggregate API.
 */
static void mongo_aggregate_pipeline_query(EState *estate, TupleDesc tupdesc,
											MongoPlanerInfo *plannerInfo,
											qdoc_expr_cxt *context,
											BSON *queryDocument)
{
	BSON	pipeline;

	if (queryDocument == NULL)
		return;

	/* Build pipeline */
	BsonAppendStartArray (queryDocument, "pipeline", &pipeline);

	/* Scan rel must be join relation */
	if (context->scan_reloptkind == RELOPT_JOINREL ||
		context->scan_reloptkind == RELOPT_OTHER_JOINREL)
	{
		ListCell *lc;
		/*
		 * Build "$lookup" object.
		 */
		foreach(lc, plannerInfo->joininfo_list)
		{
			MongoPlanerJoinInfo *join_info = (MongoPlanerJoinInfo *)lfirst(lc);

			mongo_append_lookup_doc(tupdesc, &pipeline, plannerInfo->tlist,
									plannerInfo->scan_reloptkind, join_info, context);

			/* Build $unwind stage */
			if (context->innerel_name)
			{
				BSON unwind, unwind_doc;
				char *buf;

				BsonAppendStartObject (&pipeline, "0", &unwind);
				BsonAppendStartObject (&unwind, "$unwind", &unwind_doc);
				buf = psprintf("$%s", context->innerel_name);

				BsonAppendUTF8 (&unwind_doc, "path", buf);
				BsonAppendBool (&unwind_doc, "preserveNullAndEmptyArrays", true);

				BsonAppendFinishObject (&unwind, &unwind_doc);
				BsonAppendFinishObject (&pipeline, &unwind);

				context->innerel_name_list = lappend(context->innerel_name_list, makeString(context->innerel_name));
			}
		}
	}

	/* Build filter document for WHERE clause */
	if (plannerInfo->remote_exprs)
		mongo_append_filter_doc(&pipeline, plannerInfo, context);

	if (context->reloptkind == RELOPT_UPPER_REL ||
		context->reloptkind == RELOPT_OTHER_UPPER_REL)
	{
		/*
		 * Build "$group" object.
		 *
		 * For a join or upper relation the input tlist gives the list of
		 * columns required to be fetched from the foreign server.
		 */
		if (context->has_grouping_agg)
			mongo_append_grouping_doc(tupdesc, &pipeline, plannerInfo, context);
	}

	/* Build filter document for HAVING clause */
	if (plannerInfo->having_quals)
	{
		context->need_aggexpr_syntax = true;
		mongo_append_filter_doc(&pipeline, plannerInfo, context);
	}

	/* Build target list document */
	mongo_append_target_list_doc(tupdesc, &pipeline, plannerInfo, context);

	/*
	 * Build LIMIT/OFFSET:
	 *		{$skip:1}, {$limit: 1}
	 */
	if (plannerInfo->has_limit)
	{
		if (plannerInfo->limitOffset)
		{
			BSON offset_doc;

			BsonAppendStartObject (&pipeline, "0", &offset_doc);
			AppendConstantValue (&offset_doc, "$skip", (Const *)plannerInfo->limitOffset);
			BsonAppendFinishObject (&pipeline, &offset_doc);
		}
		if (plannerInfo->limitCount)
		{
			BSON limit_doc;

			BsonAppendStartObject (&pipeline, "0", &limit_doc);
			AppendConstantValue (&limit_doc, "$limit", (Const *)plannerInfo->limitCount);
			BsonAppendFinishObject (&pipeline, &limit_doc);
		}
	}

	BsonAppendFinishArray (queryDocument, &pipeline);

	if (!BsonFinish(queryDocument))
	{
#ifdef META_DRIVER
		ereport(ERROR,
				(errmsg("could not create document for query"),
				 errhint("BSON flags: %d", queryDocument->flags)));
#else
		ereport(ERROR,
				(errmsg("could not create document for query"),
				 errhint("BSON error: %d", queryDocument->err)));
#endif
	}
}

/*
 * Building "$lookup" stage".
 */
static void mongo_append_lookup_doc(TupleDesc tupdesc,
									  BSON *pipeline,
									  List *tlist,
									  RelOptKind scan_reloptkind,
									  MongoPlanerJoinInfo *join_info,
									  qdoc_expr_cxt *context)
{
	BSON lookup_stage, join_doc, let_doc;
	RangeTblEntry *rte_o = NULL;
	RangeTblEntry *rte_i = NULL;
	Relation	rel;
	StringInfoData buf;
	ListCell *lc;
	qdoc_expr_cxt local_context;
	MongoPlanerInfo *plannerInfo_inner;
	int i = 0;

	Assert (scan_reloptkind == RELOPT_JOINREL ||
			scan_reloptkind == RELOPT_OTHER_JOINREL);

	if (join_info->outerrel_relid > 0)
	{
		rte_o = exec_rt_fetch(join_info->outerrel_relid, context->estate);
		context->rel_oid = rte_o->relid;
		context->rtindex = join_info->outerrel_relid;

		initStringInfo(&buf);
		rel = table_open(rte_o->relid, NoLock);
		mongo_deparseRelation(&buf, rel);
		table_close(rel, NoLock);

		context->outerrel_name = pstrdup(buf.data);
		join_info->outerrel_name = context->outerrel_name;
	}

	if (join_info->innerrel_relid > 0)
	{
		rte_i = exec_rt_fetch(join_info->innerrel_relid, context->estate);
		local_context.rel_oid = rte_i->relid;
		local_context.rtindex = join_info->innerrel_relid;

		if (rte_o)
			resetStringInfo(&buf);
		else
			initStringInfo(&buf);
		rel = table_open(rte_i->relid, NoLock);
		mongo_deparseRelation(&buf, rel);
		table_close(rel, NoLock);

		context->innerel_name = pstrdup(buf.data);
		join_info->innerel_name = context->innerel_name;
	}

	local_context.estate = context->estate;
	local_context.conds_num = 0;
	local_context.reloptkind = RELOPT_BASEREL;
	local_context.scan_reloptkind = RELOPT_BASEREL;
	local_context.target_ref_list = NIL;
	local_context.has_groupClause = false;
	local_context.has_grouping_agg = false;
	local_context.bs_key = NULL;
	local_context.need_aggexpr_syntax = true;
	local_context.agg_ref_list = NIL;
	local_context.count_boolexpr = 0;
	local_context.innerel_name = NULL;
	local_context.outerrel_name = NULL;
	local_context.innerel_name_list = NIL;

	BsonAppendStartObject (pipeline, "0", &lookup_stage);
	BsonAppendStartObject (&lookup_stage, "$lookup", &join_doc);

	/* From inner relation name: { from: "inner collection name" } */
	BsonAppendUTF8(&join_doc, "from", buf.data);

	/* Build "let" object document */
	BsonAppendStartObject (&join_doc, "let", &let_doc);
	/*
	 * Pull out Columns in joinclauses which are column that belongs to
	 * outer relation. These columns are passed to pipeline stage.
	 */
	foreach(lc, join_info->joinclauses)
	{
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);
		List *var_list;
		ListCell *var_cell;
		mongo_aggref_ref *agg_ref = NULL;

		var_list = pull_var_clause((Node *) rinfo->clause, PVC_RECURSE_PLACEHOLDERS);
		foreach(var_cell, var_list)
		{
			RangeTblEntry *rte = NULL;
			Var *var = (Var *) lfirst(var_cell);
			char *ref_var_outer;
			char *col_name;

			/* Using Var node that belong to outer to create "let" object */
			if (join_info->innerrel_relid != 0 &&
				var->varno == join_info->innerrel_relid)
				continue;

			/*
			 * Build reference column name for "let".
			 * Using varattno as reference index.
			 */
			rte = exec_rt_fetch(var->varno, context->estate);
			col_name = get_attname(rte->relid, var->varattno, false);
			col_name = psprintf("$%s", col_name);
			ref_var_outer = psprintf("ref%d", var->varattno);
			BsonAppendUTF8(&let_doc, ref_var_outer, col_name);

			agg_ref = palloc0(sizeof(mongo_aggref_ref));
			agg_ref->expr = (Expr *)var;
			agg_ref->ref_target = psprintf("$ref%d", var->varattno);

			local_context.agg_ref_list = lappend(local_context.agg_ref_list, agg_ref);
		}
	}
	BsonAppendFinishObject (&join_doc, &let_doc);

	/* Rebuild plannerInfo for inner relation */
	plannerInfo_inner = (MongoPlanerInfo *) palloc0(sizeof(MongoPlanerInfo));
	foreach(lc, tlist)
	{
		Node *node = (Node *)lfirst(lc);
		Expr *expr = (Expr *) node;

		if (IsA(node, TargetEntry))
			expr = ((TargetEntry *)node)->expr;

		if (IsA(expr, Var))
		{
			Var *var = (Var *)expr;

			if (var->varno == join_info->innerrel_relid)
			{
				mongo_target_ref *target_ref = palloc0(sizeof(mongo_target_ref));
				plannerInfo_inner->tlist = add_to_flat_tlist(plannerInfo_inner->tlist, list_make1(expr));

				target_ref->expr = expr;
				target_ref->is_group_target = false;
				target_ref->target_idx = i;
				local_context.target_ref_list = lappend(local_context.target_ref_list, target_ref);
			}
		}
		i++;
	}
	plannerInfo_inner->reloptkind = RELOPT_BASEREL;
	plannerInfo_inner->scan_reloptkind = RELOPT_BASEREL;
	plannerInfo_inner->rtindex = join_info->innerrel_relid;
	plannerInfo_inner->remote_exprs = join_info->joinclauses;

	/* Build sub-pipeline */
	mongo_aggregate_pipeline_query(context->estate, tupdesc, plannerInfo_inner, &local_context, &join_doc);

	/* Build "as" */
	BsonAppendUTF8(&join_doc, "as", buf.data);

	BsonAppendFinishObject (&lookup_stage, &join_doc);
	BsonAppendFinishObject (pipeline, &lookup_stage);
}

/*
 * Building "$group" stage".
 */
static void mongo_append_grouping_doc(TupleDesc tupdesc,
									  BSON *pipeline,
									  MongoPlanerInfo *plannerInfo,
									  qdoc_expr_cxt *context)
{
	BSON	group_stage, group_tlist, col_group;
	List	*aggref_tlist = NIL;
	ListCell   *lc;
	int		i = 0;

	Assert (plannerInfo->reloptkind == RELOPT_UPPER_REL ||
			plannerInfo->reloptkind == RELOPT_OTHER_UPPER_REL);

	BsonAppendStartObject (pipeline, "0", &group_stage);
	BsonAppendStartObject (&group_stage, "$group", &group_tlist);

	BsonAppendStartObject (&group_tlist, "_id", &col_group);
	foreach(lc, plannerInfo->tlist)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);
		Expr		*expr = (Expr *) tle->expr;
		char		*ref_target;

		/* Build sort group target in "_id" object */
		if (context->has_groupClause && tle->ressortgroupref != 0)
		{
			mongo_target_ref *target_ref = palloc0(sizeof(mongo_target_ref));
			mongo_aggref_ref *agg_ref = palloc0(sizeof(mongo_aggref_ref));

			/* Build reference target in aggregation group likes: {"ref0: "$(expr)" } */
			ref_target = psprintf("ref%d", i);

			context->bs_key = ref_target;
			mongo_build_expr_doc(&col_group, expr, context);
			context->bs_key = NULL;

			target_ref->is_group_target = true;
			target_ref->target_idx = i;
			context->target_ref_list = lappend(context->target_ref_list, target_ref);

			agg_ref->expr = expr;
			agg_ref->ref_target = ref_target;
			context->agg_ref_list = lappend(context->agg_ref_list, agg_ref);
		}

		i++;
	}
	BsonAppendFinishObject (&group_tlist, &col_group);

	i = 0;
	foreach(lc, plannerInfo->tlist)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);
		Expr		*expr = (Expr *) tle->expr;
		char		*ref_target;

		/* Build non - sort group target */
		if (!context->has_groupClause || tle->ressortgroupref == 0)
		{
			mongo_target_ref *target_ref = palloc0(sizeof(mongo_target_ref));

			/* Build reference target in aggregation group likes: {"ref0: "$(expr)" } */
			ref_target = psprintf("ref%d", i);

			context->bs_key = ref_target;
			mongo_build_expr_doc(&group_tlist, expr, context);
			context->bs_key = NULL;

			target_ref->is_group_target = false;
			target_ref->target_idx = i;
			context->target_ref_list = lappend(context->target_ref_list, target_ref);
		}
		i++;
	}

	/*
	 * Extract and append aggregate function in remote
	 * conditions of HAVING to target list
	 */
	foreach(lc, plannerInfo->having_quals)
	{
		List		*agglist;
		ListCell	*aggcell;
		Expr	   *expr = (Expr *) lfirst(lc);

		/* Pull out aggregate functions */
		agglist = mongo_pull_func_clause((Node *) expr);

		foreach(aggcell, agglist)
		{
			expr = (Expr *) lfirst(aggcell);

			if (!list_member(aggref_tlist, expr))
				aggref_tlist = lappend(aggref_tlist, expr);
		}
	}

	i = list_length(context->target_ref_list);
	/* Build reference group expression for HAVING clause */
	foreach(lc, aggref_tlist)
	{
		Expr		*expr = (Expr *) lfirst(lc);
		mongo_aggref_ref *agg_ref = palloc0(sizeof(mongo_aggref_ref));

		/* Build reference target in aggregation group likes: {"ref0: {"$avg": "$c1" } */
		agg_ref->expr = expr;
		agg_ref->ref_target = psprintf("ref%d", i);

		context->bs_key = agg_ref->ref_target;
		mongo_build_expr_doc(&group_tlist, expr, context);
		context->bs_key = NULL;

		context->agg_ref_list = lappend(context->agg_ref_list, agg_ref);

		i++;
	}

	BsonAppendFinishObject (&group_stage, &group_tlist);
	BsonAppendFinishObject (pipeline, &group_stage);
}

/*
 * Building "$project" stage for targets list.
 */
static void mongo_append_target_list_doc(TupleDesc tupdesc,
										 BSON *pipeline,
										 MongoPlanerInfo *plannerInfo,
										 qdoc_expr_cxt *context)
{
	BSON	project_stage, tlist_doc;
	ListCell	*lc;
	bool	is_first = true;

	plannerInfo->retrieved_attrs = NIL;

	if (plannerInfo->reloptkind != RELOPT_UPPER_REL &&
		plannerInfo->reloptkind != RELOPT_OTHER_UPPER_REL)
	{
		if (plannerInfo->tlist != NIL)
		{
			char	   *ref_target = NULL;
			int			i = 0, ref_index = 0;

			/* Build target list contains jsonb arrow operator */
			foreach(lc, plannerInfo->tlist)
			{
				Expr *expr = ((TargetEntry *)lfirst(lc))->expr;
				ListCell *tc;

				ref_index = i;

				foreach(tc, context->target_ref_list)
				{
					mongo_target_ref *target_ref = (mongo_target_ref *)lfirst(tc);

					if (equal(expr, target_ref->expr))
					{
						ref_index = target_ref->target_idx;
						break;
					}
				}

				if (is_first)
				{
					BsonAppendStartObject (pipeline, "0", &project_stage);
					BsonAppendStartObject (&project_stage, "$project", &tlist_doc);
					is_first = false;
				}

				/* Build reference target in aggregation group */
				ref_target = psprintf("ref%d", ref_index);

				context->bs_key = ref_target;
				mongo_build_expr_doc(&tlist_doc, expr, context);
				context->bs_key = NULL;
				pfree(ref_target);

				plannerInfo->retrieved_attrs = lappend_int(plannerInfo->retrieved_attrs, ref_index + 1);
				i++;
			}
		}
		else
		{
			Relation	rel = table_open(context->rel_oid, NoLock);
			bool		have_wholerow;
			int			i = 0;

			/*
			* Identify which attributes will need to be retrieved from the remote
			* server.  These include all attrs needed for joins or final output, plus
			* all attrs used in the local_conds.  (Note: if we end up using a
			* parameterized scan, it's possible that some of the join clauses will be
			* sent to the remote and thus we wouldn't really need to retrieve the
			* columns used in them.  Doesn't seem worth detecting that case though.)
			*/
			plannerInfo->attrs_used = NULL;
			pull_varattnos((Node *)plannerInfo->ptarget_exprs, plannerInfo->rtindex,
							&plannerInfo->attrs_used);
			foreach(lc, plannerInfo->local_exprs)
			{
				Expr	   *expr = (Expr *) lfirst(lc);

				/* Extract clause from RestrictInfo, if required */
				if (IsA(expr, RestrictInfo))
					expr = ((RestrictInfo *) expr)->clause;

				pull_varattnos((Node *) expr, plannerInfo->rtindex,
							&plannerInfo->attrs_used);
			}

			/* If there's a whole-row reference, we'll need all the columns. */
			have_wholerow = bms_is_member(0 - FirstLowInvalidHeapAttributeNumber,
										plannerInfo->attrs_used);

			for (i = 1; i <= tupdesc->natts; i++)
			{
				Form_pg_attribute attr = TupleDescAttr(tupdesc, i - 1);

				/* Ignore dropped attributes. */
				if (attr->attisdropped)
					continue;

				if (have_wholerow ||
					bms_is_member(i - FirstLowInvalidHeapAttributeNumber,
								plannerInfo->attrs_used))
				{
					char *colname = get_attname(context->rel_oid, i, false);

					if (strcmp(colname, "__doc") == 0)
					{
						/* Does not include __doc to target */
						plannerInfo->retrieved_attrs = lappend_int(plannerInfo->retrieved_attrs, i);
						continue;
					}

					if (is_first)
					{
						BsonAppendStartObject (pipeline, "0", &project_stage);
						BsonAppendStartObject (&project_stage, "$project", &tlist_doc);
						is_first = false;
					}

					/* Append column name */
					BsonAppendInt32 (&tlist_doc, colname, 1);

					plannerInfo->retrieved_attrs = lappend_int(plannerInfo->retrieved_attrs, i);
				}
			}

			table_close(rel, NoLock);
		}
	}
	else
	{
		char	   *ref_target = NULL;
		int			i = 0;

		foreach(lc, context->target_ref_list)
		{
			mongo_target_ref *target_ref = (mongo_target_ref *)lfirst(lc);

			if (is_first)
			{
				BsonAppendStartObject (pipeline, "0", &project_stage);
				BsonAppendStartObject (&project_stage, "$project", &tlist_doc);
				is_first = false;
			}

			/* Append target reference */
			/* Build reference target in aggregation group */
			ref_target = psprintf("ref%d", target_ref->target_idx);
			if (target_ref->is_group_target)
			{
				char *ref_target_path = psprintf("$_id.%s", ref_target);

				BsonAppendUTF8 (&tlist_doc, ref_target, ref_target_path);
				pfree(ref_target_path);
			}
			else
			{
				BsonAppendInt32 (&tlist_doc, ref_target, 1);
			}

			pfree(ref_target);

			plannerInfo->retrieved_attrs = lappend_int(plannerInfo->retrieved_attrs, i + 1);
			i++;
		}
	}

	if (is_first == false)
	{
		foreach(lc, context->innerel_name_list)
		{
			char *innerel_name = strVal(lfirst(lc));

			BsonAppendInt32 (&tlist_doc, innerel_name, 1);
		}

		BsonAppendFinishObject (&project_stage, &tlist_doc);
		BsonAppendFinishObject (pipeline, &project_stage);
	}
}

/*
 * Building "$match" stage for filter BSON query document.
 */
static void mongo_append_filter_doc(BSON *pipeline, MongoPlanerInfo *plannerInfo, qdoc_expr_cxt *context)
{
	BSON	match_stage, filter_conds, multi_cond_exprs, expr_stage;
	List	*remote_exprs = NIL;
	ListCell   *lc;
	int			nestlevel;
	bool		is_first = true;
	int 		conds_num;

	remote_exprs = plannerInfo->remote_exprs;

	if (context->need_aggexpr_syntax)
	{
		context->bs_key = "$expr";

		if (context->reloptkind == RELOPT_UPPER_REL ||
			context->reloptkind == RELOPT_OTHER_UPPER_REL)
			remote_exprs = plannerInfo->having_quals;
	}

	conds_num = list_length(remote_exprs);
	context->conds_num = conds_num;

	BsonAppendStartObject (pipeline, "0", &match_stage);
	BsonAppendStartObject (&match_stage, "$match", &filter_conds);

	/* Make sure any constants in the exprs are printed portably */
	nestlevel = mongo_set_transmission_modes();

	foreach(lc, remote_exprs)
	{
		Expr	   *expr = (Expr *) lfirst(lc);

		/* Extract clause from RestrictInfo, if required */
		if (IsA(expr, RestrictInfo))
			expr = ((RestrictInfo *) expr)->clause;

		/*
		 * If more than 1 condition use and for combine as:
		 *		{ $and: [ { <expr1> }, { <expr2> } , ... , { <exprN> } ] }
		 */
		if (conds_num > 1 && is_first)
		{
			if (context->need_aggexpr_syntax)
			{
				BsonAppendStartObject (&filter_conds, "$expr", &expr_stage);
				BsonAppendStartArray (&expr_stage, "$and", &multi_cond_exprs);
			}
			else
				BsonAppendStartArray (&filter_conds, "$and", &multi_cond_exprs);
			mongo_build_expr_doc(&multi_cond_exprs, expr, context);
		}
		else if (conds_num > 1)
			mongo_build_expr_doc(&multi_cond_exprs, expr, context);
		else
			mongo_build_expr_doc(&filter_conds, expr, context);

		is_first = false;
	}

	if (conds_num > 1)
	{
		if (context->need_aggexpr_syntax)
		{
			BsonAppendFinishArray (&expr_stage, &multi_cond_exprs);
			BsonAppendFinishObject (&filter_conds, &expr_stage);
		}
		else
			BsonAppendFinishArray (&filter_conds, &multi_cond_exprs);
	}

	mongo_reset_transmission_modes(nestlevel);

	if (context->need_aggexpr_syntax)
		context->bs_key = NULL;

	BsonAppendFinishObject (&match_stage, &filter_conds);
	BsonAppendFinishObject (pipeline, &match_stage);
}

/*
 *	Build Boolean expression in BSON query document.
 */
static void mongo_build_boolexpr_doc(BSON *qdoc, BoolExpr *node, qdoc_expr_cxt *context)
{
	BSON boolexpr_doc, op_doc, expr_stage, *ptr_qdoc;
	char *op = NULL;		/* keep compiler quiet */
	ListCell   *lc;
	int		conds_num = context->conds_num;

	switch (node->boolop)
	{
		case AND_EXPR:
			op = "$and";
			break;
		case OR_EXPR:
			op = "$or";
			break;
		default :
			return;
	}

	ptr_qdoc = qdoc;

	if (context->need_aggexpr_syntax && context->count_boolexpr == 0)
	{
		if (context->bs_key == NULL)
			elog(ERROR, "Could not add a boolean expression");
		BsonAppendStartObject(qdoc, context->bs_key, &expr_stage);
		ptr_qdoc = &expr_stage;
	}

	context->count_boolexpr++;

	if (context->conds_num > 1)
	{
		BsonAppendStartObject(ptr_qdoc, op, &op_doc);
		BsonAppendStartArray (&op_doc, op, &boolexpr_doc);
	}
	else
		BsonAppendStartArray (ptr_qdoc, op, &boolexpr_doc);

	foreach(lc, node->args)
	{
		context->conds_num++;
		mongo_build_expr_doc(&boolexpr_doc, (Expr *) lfirst(lc), context);
	}
	context->conds_num = conds_num;

	if (context->conds_num > 1)
	{
		BsonAppendFinishArray (&op_doc, &boolexpr_doc);
		BsonAppendFinishObject(ptr_qdoc, &op_doc);
	}
	else
		BsonAppendFinishArray (ptr_qdoc, &boolexpr_doc);

	context->count_boolexpr--;

	if (context->need_aggexpr_syntax && context->count_boolexpr == 0)
		BsonAppendFinishObject(qdoc, &expr_stage);

	context->conds_num++;
}

/*
 *	Build operator expression in BSON query document.
 */
static void mongo_build_opexpr_doc(BSON *qdoc, OpExpr *node, qdoc_expr_cxt *context)
{
	MongoOperatorsSupport opkind;
	Expr		*right_opr = NULL;
	Expr		*left_opr = NULL;
	Node		*n;
	bool		is_switch_operator = false;
	bool		need_switch_operator = false;
	const char	*opName = NULL;

	/* Retrieve information about the operator. */
	opkind = mongo_validateOperatorName(node->opno, &opName, &is_switch_operator);

	/* Sanity check. */
	Assert(list_length(node->args) == 2);

	if (opkind == OP_JSON)
	{
		/* Deparse expression for nested json object */
		deparse_expr_cxt deparse_context;
		StringInfoData buf;

		initStringInfo(&buf);

		deparse_context.buf = &buf;
		deparse_context.qdoc_ctx = context;

		appendStringInfoChar(&buf, '$');

		mongo_deparseExpr((Expr *) node, &deparse_context);

		if (context->bs_key)
			BsonAppendUTF8(qdoc, context->bs_key, buf.data);
		else
			elog(ERROR, "Could not add json nested object");

		return;
	}

	/*
	 * If right operand of a logic operator expression
	 * is a column name, we need to switch left and right
	 * operands with converted logic operator.
	 */
	n = (Node *) lfirst(list_tail(node->args));
	if (is_switch_operator && IsA(n, Var))
	{
		need_switch_operator = true;
		/* Get node for left operand */
		left_opr = (Expr *)lfirst(list_tail(node->args));

		/* Get node for right operand */
		right_opr = (Expr *)lfirst(list_head(node->args));
	}
	else
	{
		/* Get node for left operand */
		left_opr = (Expr *)lfirst(list_head(node->args));

		/* Get node for right operand */
		right_opr = (Expr *)lfirst(list_tail(node->args));
	}

	if (opkind == OP_CONDITIONAL && !context->need_aggexpr_syntax)
	{
		BSON opexpr_doc, left_opr_doc;
		char *leftopr_str = NULL;

		/* Left operand only can be field name or a nested json object */
		if (IsA(left_opr, Var))
			leftopr_str = get_attname(context->rel_oid, ((Var *) left_opr)->varattno, false);
		else
		{
			/* Deparse expression for nested json object */
			deparse_expr_cxt deparse_context;
			StringInfoData buf;

			initStringInfo(&buf);

			deparse_context.buf = &buf;
			deparse_context.qdoc_ctx = context;

			mongo_deparseExpr(left_opr, &deparse_context);
			leftopr_str = pstrdup(deparse_context.buf->data);
		}

		/* Build BSON for comparing operator: {field: { opname: value}} */
		if (context->conds_num > 1)
		{
			BsonAppendStartObject (qdoc, leftopr_str, &opexpr_doc);
			BsonAppendStartObject(&opexpr_doc, leftopr_str, &left_opr_doc);
		}
		else
			BsonAppendStartObject(qdoc, leftopr_str, &left_opr_doc);

		opName = mongo_getSwitchedCmpOperatorName(opName, need_switch_operator);
		AppendConstantValue(&left_opr_doc, opName, (Const *) right_opr);

		if (context->conds_num > 1)
		{
			BsonAppendFinishObject(&opexpr_doc, &left_opr_doc);
			BsonAppendFinishObject (qdoc, &opexpr_doc);
		}
		else
			BsonAppendFinishObject(qdoc, &left_opr_doc);
	}
	else
	{
		BSON opexpr_doc, ref_doc;
		ListCell *lc;

		/* Build reference target in aggregation group likes: {"bson_key": { "$add":["$c1", 1] } } */
		if (context->bs_key == NULL)
			elog(ERROR, "Could not add a operator expression");

		BsonAppendStartObject (qdoc, context->bs_key, &ref_doc);
		BsonAppendStartArray (&ref_doc, opName, &opexpr_doc);

		foreach(lc, node->args)
		{
			Expr *expr = (Expr *) lfirst(lc);
			ListCell *aggcell;
			char *ref_key = NULL;

			if (context->need_aggexpr_syntax && (IsA(expr, Aggref) || IsA(expr, Var) || IsA(expr, RelabelType)))
			{
				foreach(aggcell, context->agg_ref_list)
				{
					mongo_aggref_ref *agg_ref = (mongo_aggref_ref *) lfirst(aggcell);

					if (IsA(expr, RelabelType))
						expr = ((RelabelType *)expr)->arg;

					/*
					 * Check whether expression in HAVING is referenced by
					 * aggref in grouping.
					 */
					if (equal(expr, agg_ref->expr))
					{
						/* Build reference key like "$refx" */
						if (IsA(expr, Aggref))
							ref_key = psprintf("$%s", agg_ref->ref_target);
						else if (IsA(expr, Var))
						{
							if (context->has_groupClause)
								ref_key = psprintf("$_id.%s", agg_ref->ref_target); /* reference from $group stage */
							else
								ref_key = psprintf("$%s", agg_ref->ref_target); /* reference from "let" in $lookup stage */
						}

						break;
					}
				}

				if (ref_key != NULL)
					BsonAppendUTF8(&opexpr_doc, context->bs_key, ref_key);
				else
					mongo_build_expr_doc(&opexpr_doc, expr, context);
			}
			else
				mongo_build_expr_doc(&opexpr_doc, expr, context);
		}

		BsonAppendFinishArray (&ref_doc, &opexpr_doc);
		BsonAppendFinishObject (qdoc, &ref_doc);
	}
}

/*
 *	Build column name in BSON query document.
 */
static void
mongo_build_column_doc(BSON *qdoc, Var *node, qdoc_expr_cxt *context)
{
	char	   *colname = NULL;

	if (node->varattno < 0)
		elog(ERROR, "Could not build BSON query document for system attribute");
	else if (node->varattno == 0)
		elog(ERROR, "Could not build BSON query document for whole-row reference");

	if (context->rtindex != 0 && node->varno != context->rtindex)
		return;

	colname = get_attname(context->rel_oid, node->varattno, false);

	/* Build colname object like "$column" */
	colname = psprintf("$%s", colname);

	if (context->bs_key)
		BsonAppendUTF8(qdoc, context->bs_key, colname);
	else
		elog(ERROR, "Could not add column object");
}

/*
 *	Build constant value in BSON query document.
 */
static void
mongo_build_const_doc(BSON *qdoc, Const *node, qdoc_expr_cxt *context)
{
	if (context->bs_key)
		AppendConstantValue(qdoc, context->bs_key, node);
	else
		elog(ERROR, "Could not add constant value object");
}

/*
 *	Build aggregate function in BSON query document.
 */
static void
mongo_build_aggref_doc(BSON *qdoc, Aggref *node, qdoc_expr_cxt *context)
{
	BSON aggref_doc;
	char *proname;

	/* Only basic, non-split aggregation accepted. */
	Assert(node->aggsplit == AGGSPLIT_SIMPLE);

	/* Get function name */
	proname = get_func_name(node->aggfnoid);
	if (proname == NULL)
		elog(ERROR, "No such function name for function OID %u", node->aggfnoid);

	/* Convert to MongoDB aggregate function */
	if (strcmp(proname, "stddev_pop") == 0)
		proname = "stdDevPop";
	else if (strcmp(proname, "stddev") == 0 ||
			 strcmp(proname, "stddev_samp") == 0)
		proname = "stdDevSamp";

	/* Build reference target in aggregation group likes: {"ref0: "$avg" } */
	if (context->bs_key == NULL)
		elog(ERROR, "Could not add a aggregate function");

	BsonAppendStartObject (qdoc, context->bs_key, &aggref_doc);

	/* aggstar can be set only in zero-argument aggregates */
	if (node->aggstar)
	{
		/*
		 * Currently, we only support count(*).
		 * This function is built by using "sum" function like:
		 * 	{ "$sum" : 1 }
		 */
		if (strcmp(proname, "count") == 0)
		{
			BsonAppendInt32(&aggref_doc, "$sum", 1);
		}
	}
	else
	{
		TargetEntry *tle = NULL;
		Node	   *n = NULL;
		ListCell   *arg = NULL;
		int			args_num = list_length(node->args);

		/* Build function name in bson format like "$avg", "$sum" */
		proname = psprintf("$%s", proname);

		context->bs_key = proname;
		if (args_num == 1)
		{
			tle = (TargetEntry *) lfirst(list_head(node->args));
			n = (Node *) tle->expr;

			/* Syntax: e.g. { $max: <expression> } */
			mongo_build_expr_doc(&aggref_doc, (Expr *)n, context);
		}
		else
		{
			BSON args_doc;

			/* Syntax: e.g. { $max: [ <expression1>, <expression2> ... ]  } */
			BsonAppendStartArray (&aggref_doc, context->bs_key, &args_doc);
			foreach(arg, node->args)
			{
				tle = (TargetEntry *) lfirst(arg);
				n = (Node *) tle->expr;

				if (tle->resjunk)
					continue;

				mongo_build_expr_doc(&args_doc, (Expr *)n, context);
			}
			BsonAppendFinishArray (&aggref_doc, &args_doc);
		}
		context->bs_key = NULL;
	}

	BsonAppendFinishObject (qdoc, &aggref_doc);
}

/*
 *	Build RelabelType expression in BSON query document.
 *		Discard explicit cast type.
 */
static void
mongo_build_relabeltype_doc(BSON *qdoc, RelabelType *node, qdoc_expr_cxt *context)
{
	mongo_build_expr_doc(qdoc, node->arg, context);
}

/*
 * Build IS [NOT] NULL expression query document, e.g:
 * 	c1 IS NULL is transformed by { eq: ["$c1", null] }
 * 	c1 IS NOT NULL is transformed by { ne: ["$c1", null] }
 */
static void
mongo_build_NullTest_doc(BSON *qdoc, NullTest *node, qdoc_expr_cxt *context)
{
	BSON nulltest_doc;
	BSON op_doc;
	const char* opname = NULL;
	char *input_expr_str = NULL;

	if (node->nulltesttype == IS_NULL)
		opname = "$eq";
	else
		opname = "$ne";

	/* Input expression only is field name. */
	if (IsA(node->arg, Var))
		input_expr_str = get_attname(context->rel_oid, ((Var *) node->arg)->varattno, false);
	else
		elog (ERROR, "Could not build BSON document for input expression of NullTest");

	if (context->need_aggexpr_syntax)
	{
		ListCell *aggcell;
		char *ref_key = NULL;

		foreach(aggcell, context->agg_ref_list)
		{
			mongo_aggref_ref *agg_ref = (mongo_aggref_ref *) lfirst(aggcell);

			/*
			 * Check whether expression in HAVING is referenced by
			 * aggref in grouping.
			 */
			if (equal(node->arg, agg_ref->expr))
			{
				/* Build reference key like "$refx" */
				ref_key = psprintf("%s", agg_ref->ref_target); /* reference from "let" in $lookup stage */

				break;
			}
		}

		if (ref_key != NULL)
			input_expr_str = ref_key;
		else
			input_expr_str = psprintf("$%s", input_expr_str);

		BsonAppendStartObject(qdoc, "0", &nulltest_doc);

		BsonAppendStartArray(&nulltest_doc, opname, &op_doc);
		BsonAppendUTF8(&op_doc, "0", input_expr_str);
		BsonAppendNull(&op_doc, "1");

		BsonAppendFinishArray(&nulltest_doc, &op_doc);

		BsonAppendFinishObject(qdoc, &nulltest_doc);
	}
	else
	{
		if (context->conds_num > 1)
		{
			BsonAppendStartObject(qdoc, "0", &nulltest_doc);

			BsonAppendStartObject(&nulltest_doc, input_expr_str, &op_doc);
			BsonAppendNull(&op_doc, opname);
			BsonAppendFinishObject(&nulltest_doc, &op_doc);

			BsonAppendFinishObject(qdoc, &nulltest_doc);
		}
		else
		{
			BsonAppendStartObject(qdoc, input_expr_str, &nulltest_doc);
			BsonAppendNull(&nulltest_doc, opname);
			BsonAppendFinishObject(qdoc, &nulltest_doc);
		}
	}
}

/*
 * Deparse given ScalarArrayOpExpr expression.  To avoid problems
 * around priority of operations, we always parenthesize the arguments.
 */
static void
mongo_build_scalar_array_op_expr(BSON *qdoc, ScalarArrayOpExpr *node, qdoc_expr_cxt *context)
{
	Expr	   *arg1;
	Expr	   *arg2;
	List	   *const_list = NIL;
	ListCell   *lc;
	char	   *opname = NULL;
	char	   *extval;
	const char *mongo_opname = NULL;
	char	   *input_expr_str = NULL;
	char	   *ref_key = NULL;
	BSON	   scalarArrayOpExpr_doc;
	BSON	   op_doc;
	int		   i = -1;

	/* Sanity check. */
	Assert(list_length(node->args) == 2);

	/* Get operator name */
	opname = get_opname(node->opno);
	if (opname == NULL)
		elog(ERROR, "No such operator name for operator OID %u", node->opno);

	/*
	 * Using $in for '= ANY'$ (similar with IN )
	 * Using $nin for '<> ALL' (similar with NOT IN )
	 * Using $all for '= ALL'
	 */
	if (node->useOr == true)
		mongo_opname = "$in";
	else if (strcmp(opname, "<>") == 0)
		mongo_opname = "$nin";
	else if (strcmp(opname, "=") == 0)
		mongo_opname = "$all";

	/* Get left and right argument for deparsing */
	arg1 = linitial(node->args);
	arg2 = lsecond(node->args);

	/* Parse scalar constant into array */
	switch (nodeTag((Node *) arg2))
	{
		case T_Const:
			{
				StringInfoData	buf;
				Const	   *c = (Const *) arg2;
				const char *valptr;
				Const	   *new_expr;
				Oid			consttype = InvalidOid;
				Datum		value;
				bool		constisnull = c->constisnull;
				bool		constbyval = c->constbyval;
				PGFunction  func_addr = NULL;
				Oid		    typoutput;
				bool	    typIsVarlena;

				initStringInfo(&buf);
				i = -1;

				if (!c->constisnull)
				{
					getTypeOutputInfo(c->consttype,
									  &typoutput, &typIsVarlena);
					extval = OidOutputFunctionCall(typoutput, c->constvalue);

					/* Get contant type and function address */
					mongo_get_func_info_scalar_array(c->consttype, &consttype, &func_addr);

					for (valptr = extval; *valptr; valptr++)
					{
						char		ch = *valptr;

						i++;

						/*
						 * Remove '{', '}' and \" character from the string.
						 * Because this syntax is not recognize by the remote
						 * MongoDB server.
						 */
						if ((ch == '{' && i == 0) || ch == '\"')
							continue;
						if ((ch == ',') || (ch == '}' && (i == (strlen(extval) - 1))))
						{
							value = DirectFunctionCall1(func_addr, CStringGetDatum(buf.data));

							/* Append constant */
							new_expr = makeConst(consttype,
											 -1,
											 InvalidOid,
											 buf.len,
											 value,
											 constisnull,	/* isnull */
											 constbyval /* byval */ );
							const_list = lappend(const_list, (Expr *) new_expr);
							resetStringInfo(&buf);
							continue;
						}
						appendStringInfoChar(&buf, ch);
					}
				}
				else
				{
					/* Append NULL */
					new_expr = makeConst(INT4OID,
										 -1,
										 InvalidOid,
										 sizeof(int32),
										 (Datum) 0,
										 true, /* isnull */
										 true /* byval */ );
					const_list = lappend(const_list, (Expr *) new_expr);
					return;
				}
			}
			break;
		default:
			elog(ERROR, "unsupported expression type for deparse: %d", (int) nodeTag(node));
			break;
	}

	/* Building BSON document for ScalarArrayOpExpr. */
	if (IsA(arg1, Var))
		input_expr_str = get_attname(context->rel_oid, ((Var *) arg1)->varattno, false);
	if (IsA(arg1, RelabelType))
	{
		Var *var = (Var *) ((RelabelType *) arg1)->arg;
		input_expr_str = get_attname(context->rel_oid, var->varattno, false);
	}

	i = 0;
	if (context->need_aggexpr_syntax)
	{
		ListCell *aggcell;
		char *ref_key = NULL;
		BSON array_doc, element_doc;

		foreach(aggcell, context->agg_ref_list)
		{
			mongo_aggref_ref *agg_ref = (mongo_aggref_ref *) lfirst(aggcell);

			/*
			 * Check whether expression in HAVING is referenced by
			 * aggref in grouping.
			 */
			if (equal(arg1, agg_ref->expr))
			{
				/* Build reference key like "$refx" */
				ref_key = psprintf("%s", agg_ref->ref_target); /* reference from "let" in $lookup stage */

				break;
			}
		}

		if (ref_key != NULL)
			input_expr_str = ref_key;
		else
			input_expr_str = psprintf("$%s", input_expr_str);

		BsonAppendStartObject(qdoc, "0", &element_doc);

		BsonAppendStartArray(&element_doc, mongo_opname, &op_doc);

		BsonAppendUTF8(&op_doc, "0", input_expr_str);

		BsonAppendStartArray(&op_doc, "1", &array_doc);
		foreach(lc, const_list)
		{
			Const *c = (Const *) lfirst(lc);

			ref_key = psprintf("%d", i);
			AppendConstantValue(&array_doc, ref_key, c);
			i++;
		}
		BsonAppendFinishArray(&op_doc, &array_doc);

		BsonAppendFinishArray(&element_doc, &op_doc);

		BsonAppendFinishObject(qdoc, &element_doc);
	}
	else
	{
		if (context->conds_num > 1)
		{
			BSON element_doc;
			BsonAppendStartObject(qdoc, "0", &element_doc);

			BsonAppendStartObject(&element_doc, input_expr_str, &scalarArrayOpExpr_doc);

			BsonAppendStartArray(&scalarArrayOpExpr_doc, mongo_opname, &op_doc);
			foreach(lc, const_list)
			{
				Const *c = (Const *) lfirst(lc);

				ref_key = psprintf("%d", i);
				AppendConstantValue(&op_doc, ref_key, c);
				i++;
			}
			BsonAppendFinishArray(&scalarArrayOpExpr_doc, &op_doc);

			BsonAppendFinishObject(&element_doc, &scalarArrayOpExpr_doc);

			BsonAppendFinishObject(qdoc, &element_doc);
		}
		else
		{
			BsonAppendStartObject(qdoc, input_expr_str, &scalarArrayOpExpr_doc);

			BsonAppendStartArray(&scalarArrayOpExpr_doc, mongo_opname, &op_doc);
			foreach(lc, const_list)
			{
				Const *c = (Const *) lfirst(lc);

				ref_key = psprintf("%d", i);
				AppendConstantValue(&op_doc, ref_key, c);
				i++;
			}
			BsonAppendFinishArray(&scalarArrayOpExpr_doc, &op_doc);

			BsonAppendFinishObject(qdoc, &scalarArrayOpExpr_doc);
		}
	}
}

/*
 * Build query document for expression.
 */
static void mongo_build_expr_doc(BSON *qdoc, Expr *node, qdoc_expr_cxt *context)
{
	if (node == NULL)
		return;

	switch (nodeTag(node))
	{
		case T_Var:
			mongo_build_column_doc(qdoc, (Var *) node, context);
			break;
		case T_Const:
			mongo_build_const_doc(qdoc, (Const *) node, context);
			break;
		case T_OpExpr:
			mongo_build_opexpr_doc(qdoc, (OpExpr *) node, context);
			break;
		case T_RelabelType:
			mongo_build_relabeltype_doc(qdoc, (RelabelType *)node, context);
			break;
		case T_Aggref:
			mongo_build_aggref_doc(qdoc, (Aggref *) node, context);
			break;
		case T_BoolExpr:
			mongo_build_boolexpr_doc(qdoc, (BoolExpr *) node, context);
			break;
		case T_NullTest:
			mongo_build_NullTest_doc(qdoc, (NullTest *) node, context);
			break;
		case T_ScalarArrayOpExpr:
			mongo_build_scalar_array_op_expr(qdoc, (ScalarArrayOpExpr *) node, context);
			break;
		default:
			elog(ERROR, "unsupported expression type for deparse: %d",
				 (int) nodeTag(node));
			break;
	}
}

/*
 * mongo_serialize_plannerInfoList
 */
List *mongo_serialize_plannerInfoList (MongoPlanerInfo *plannerInfo)
{
	List *plannerInfoList = NIL;
	ListCell *lc;

	plannerInfoList = lappend(plannerInfoList, plannerInfo->tlist);
	plannerInfoList = lappend(plannerInfoList, makeInteger((plannerInfo->tlist_has_jsonb_arrow_op) ? 1 : 0));

	plannerInfoList = lappend(plannerInfoList, makeInteger(plannerInfo->reloptkind));
	plannerInfoList = lappend(plannerInfoList, makeInteger(plannerInfo->scan_reloptkind));
	plannerInfoList = lappend(plannerInfoList, makeInteger(plannerInfo->rtindex));
	plannerInfoList = lappend(plannerInfoList, plannerInfo->remote_exprs);
	plannerInfoList = lappend(plannerInfoList, plannerInfo->local_exprs);
	plannerInfoList = lappend(plannerInfoList, plannerInfo->having_quals);

	plannerInfoList = lappend(plannerInfoList, makeInteger((plannerInfo->has_limit) ? 1 : 0));
	if (plannerInfo->has_limit)
	{
		plannerInfoList = lappend(plannerInfoList, plannerInfo->limitOffset);
		plannerInfoList = lappend(plannerInfoList, plannerInfo->limitCount);
	}

	plannerInfoList = lappend(plannerInfoList, plannerInfo->ptarget_exprs);

	plannerInfoList = lappend(plannerInfoList, makeInteger((plannerInfo->has_groupClause) ? 1 : 0));
	plannerInfoList = lappend(plannerInfoList, makeInteger((plannerInfo->has_grouping_agg) ? 1 : 0));

	plannerInfoList = lappend(plannerInfoList, makeInteger(plannerInfo->jointype));

	plannerInfo->joininfo_num = list_length(plannerInfo->joininfo_list);
	plannerInfoList = lappend(plannerInfoList, makeInteger(plannerInfo->joininfo_num));

	foreach(lc, plannerInfo->joininfo_list)
	{
		MongoPlanerJoinInfo *join_info = (MongoPlanerJoinInfo *)lfirst(lc);

		plannerInfoList = lappend(plannerInfoList, makeInteger(join_info->outerrel_relid));
		plannerInfoList = lappend(plannerInfoList, makeInteger(join_info->innerrel_relid));
		plannerInfoList = lappend(plannerInfoList, join_info->joinclauses);
	}

	return plannerInfoList;
}

/*
 * mongo_deserialize_plannerInfoList
 */
MongoPlanerInfo *mongo_deserialize_plannerInfoList(List *plannerInfoList)
{
	MongoPlanerInfo *plannerInfo = NULL;
	ListCell		*lc = list_head(plannerInfoList);
	int i;

	plannerInfo = (MongoPlanerInfo *) palloc0(sizeof(MongoPlanerInfo));

	plannerInfo->tlist = (List *) lfirst(lc);
	lc = lnext(plannerInfoList, lc);

	plannerInfo->tlist_has_jsonb_arrow_op = (intVal(lfirst(lc)) == 1) ? true : false;
	lc = lnext(plannerInfoList, lc);

	plannerInfo->reloptkind = intVal(lfirst(lc));
	lc = lnext(plannerInfoList, lc);

	plannerInfo->scan_reloptkind = intVal(lfirst(lc));
	lc = lnext(plannerInfoList, lc);

	plannerInfo->rtindex = intVal(lfirst(lc));
	lc = lnext(plannerInfoList, lc);

	plannerInfo->remote_exprs = (List *) lfirst(lc);
	lc = lnext(plannerInfoList, lc);

	plannerInfo->local_exprs = (List *) lfirst(lc);
	lc = lnext(plannerInfoList, lc);

	plannerInfo->having_quals = (List *) lfirst(lc);
	lc = lnext(plannerInfoList, lc);

	plannerInfo->has_limit = (intVal(lfirst(lc)) == 1) ? true : false;
	lc = lnext(plannerInfoList, lc);

	if (plannerInfo->has_limit)
	{
		plannerInfo->limitOffset = (Node *) lfirst(lc);
		lc = lnext(plannerInfoList, lc);

		plannerInfo->limitCount = (Node *) lfirst(lc);
		lc = lnext(plannerInfoList, lc);
	}

	plannerInfo->ptarget_exprs = (List *) lfirst(lc);
	lc = lnext(plannerInfoList, lc);

	plannerInfo->has_groupClause = (intVal(lfirst(lc)) == 1) ? true : false;
	lc = lnext(plannerInfoList, lc);

	plannerInfo->has_grouping_agg = (intVal(lfirst(lc)) == 1) ? true : false;
	lc = lnext(plannerInfoList, lc);

	plannerInfo->jointype = intVal(lfirst(lc));
	lc = lnext(plannerInfoList, lc);

	plannerInfo->joininfo_num = intVal(lfirst(lc));
	lc = lnext(plannerInfoList, lc);

	for(i = 0; i < plannerInfo->joininfo_num; i++)
	{
		MongoPlanerJoinInfo *join_info = (MongoPlanerJoinInfo *)palloc0(sizeof(MongoPlanerJoinInfo));

		join_info->outerrel_relid = intVal(lfirst(lc));
		lc = lnext(plannerInfoList, lc);

		join_info->innerrel_relid = intVal(lfirst(lc));
		lc = lnext(plannerInfoList, lc);

		join_info->joinclauses = (List *) lfirst(lc);
		lc = lnext(plannerInfoList, lc);

		plannerInfo->joininfo_list = lappend(plannerInfo->joininfo_list, join_info);
	}

	return plannerInfo;
}

/*
 * Print the name of an operator.
 */
static const char *
mongo_getSwitchedCmpOperatorName(const char *opname, bool need_switch_operator)
{
	if (need_switch_operator)
	{
		if (strcmp(opname, "$gt") == 0)
			opname = "$lte";
		else if (strcmp(opname, "$gte") == 0)
			opname = "$lt";
		else if (strcmp(opname, "$lt") == 0)
			opname = "$gte";
		else if (strcmp(opname, "$lte") == 0)
			opname = "$gt";
	}

	return opname;
}

static MongoOperatorsSupport mongo_validateOperatorName(Oid opno,
												  const char **deparseName,
												  bool *is_switch_operator)
{
	char	   *opname;
	int			i=0;
	typedef struct
	{
		const char *op_name; 		/* Operator's name supported by PostgreSQL */
		const char *op_name_abbr; 		/* Abbreviation of operator formed in URL request */
	} deparse_op_abbr;
	static deparse_op_abbr compOpNameMappings[] =
	{
		/* Operator name		Abbreviation */
		{"<",					"$lt"	}, 	/* Less than */
		{">",					"$gt"	},		/* Greater than */
		{"<=",					"$lte"	},	/* Less than or equal */
		{">=",					"$gte"	},	/* Greater than or equal */
		{"=",					"$eq"	},	/* Equal */
		{"!=",					"$ne"	},	/* Not equal */
		{"<>",					"$ne"	},	/* Not equal */
		{NULL,					NULL	},	/* NULL */
	};
	static deparse_op_abbr mathOpNameMappings[] =
	{
		/* Operator name		Abbreviation */
		{"+",					"$add"		},	/* Addition */
		{"-",					"$subtract"	},	/* Subtraction */
		{"*",					"$multiply"	},	/* Multiplication */
		{"/",					"$divide"	},	/* Division */
		{"%",					"$mod"		},	/* Mudulo */
		{NULL,					NULL		},	/* NULL */
	};
	static deparse_op_abbr jsonOpNameMappings[] =
	{
		/* Operator name		Abbreviation */
		{"->",					"."		},	/* json arrow operator */
		{NULL,					NULL	},	/* NULL */
	};



	if (is_switch_operator)
		*is_switch_operator = false;

	/* Get operator name */
	opname = get_opname(opno);
	if (opname == NULL)
		elog(ERROR, "No such operator name for operator OID %u", opno);

	/* Looking for check if operator is filter operator */
	for (i = 0; compOpNameMappings[i].op_name != NULL; i++)
	{
		if (strcmp(opname, compOpNameMappings[i].op_name) == 0)
		{
			/* Current operator is safe to pushdown */
			if (deparseName)
				*deparseName = compOpNameMappings[i].op_name_abbr;

			/* Operator can be switched */
			if (is_switch_operator)
				*is_switch_operator = true;

			return OP_CONDITIONAL;
		}
	}

	for (i = 0; mathOpNameMappings[i].op_name != NULL; i++)
	{
		if (strcmp(opname, mathOpNameMappings[i].op_name) == 0)
		{
				/* Current operator is safe to pushdown */
			if (deparseName)
				*deparseName = mathOpNameMappings[i].op_name_abbr;
			return OP_MATH;
		}
	}

	for (i = 0; jsonOpNameMappings[i].op_name != NULL; i++)
	{
		if (strcmp(opname, jsonOpNameMappings[i].op_name) == 0)
		{
				/* Current operator is safe to pushdown */
			if (deparseName)
				*deparseName = jsonOpNameMappings[i].op_name_abbr;
			return OP_JSON;
		}
	}

	/* Funtion does not in conditional list and math list */
	return OP_UNSUPPORT;
}

/*
 * Returns true if given expr is something we'd have to send the value of
 * to the foreign server.
 *
 * This should return true when the expression is a shippable node that
 * deparseExpr would add to context->params_list.  Note that we don't care
 * if the expression *contains* such a node, only whether one appears at top
 * level.  We need this to detect cases where setrefs.c would recognize a
 * false match between an fdw_exprs item (which came from the params_list)
 * and an entry in fdw_scan_tlist (which we're considering putting the given
 * expression into).
 */
bool
mongo_is_foreign_param(PlannerInfo *root,
				 RelOptInfo *baserel,
				 Expr *expr)
{
	if (expr == NULL)
		return false;

	switch (nodeTag(expr))
	{
		case T_Var:
			{
				/* It would have to be sent unless it's a foreign Var */
				Var		   *var = (Var *) expr;
				MongoFdwRelationInfo *fpinfo = (MongoFdwRelationInfo *) (baserel->fdw_private);
				Relids		relids;

				if (IS_UPPER_REL(baserel))
					relids = fpinfo->outerrel->relids;
				else
					relids = baserel->relids;

				if (bms_is_member(var->varno, relids) && var->varlevelsup == 0)
					return false;	/* foreign Var, so not a param */
				else
					return true;	/* it'd have to be a param */
				break;
			}
		case T_Param:
			/* Params always have to be sent to the foreign server */
			return true;
		default:
			break;
	}
	return false;
}

/*
 * Build the targetlist for given relation to be deparsed as SELECT clause.
 *
 * The output targetlist contains the columns that need to be fetched from the
 * foreign server for the given relation.  If foreignrel is an upper relation,
 * then the output targetlist can also contain expressions to be evaluated on
 * foreign server.
 */
List *
mongo_build_tlist_to_deparse(RelOptInfo *foreignrel)
{
	List	   *tlist = NIL;
	MongoFdwRelationInfo *fpinfo = (MongoFdwRelationInfo *) foreignrel->fdw_private;
	ListCell   *lc;

	/*
	 * For an upper relation, we have already built the target list while
	 * checking shippability, so just return that.
	 */
	if (IS_UPPER_REL(foreignrel))
		return fpinfo->grouped_tlist;

	/*
	 * We require columns specified in foreignrel->reltarget->exprs and those
	 * required for evaluating the local conditions.
	 */
	tlist = add_to_flat_tlist(tlist,
							  pull_var_clause((Node *) foreignrel->reltarget->exprs,
											  PVC_RECURSE_PLACEHOLDERS));
	foreach(lc, fpinfo->local_conds)
	{
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

		tlist = add_to_flat_tlist(tlist,
								  pull_var_clause((Node *) rinfo->clause,
												  PVC_RECURSE_PLACEHOLDERS));
	}

	return tlist;
}


/*
 * mongo_pull_func_clause_walker
 *
 * Recursively search for functions within a clause.
 */
static bool
mongo_pull_func_clause_walker(Node *node, pull_aggref_list_context *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, Aggref))
	{
		context->aggref_list = lappend(context->aggref_list, node);
		return false;
	}

	return expression_tree_walker(node, mongo_pull_func_clause_walker,
								  (void *) context);
}

/*
 * pull_func_clause
 *
 * Pull out function from a clause and then add to target list
 */
List *
mongo_pull_func_clause(Node *node)
{
	pull_aggref_list_context context;
	context.aggref_list = NIL;

	mongo_pull_func_clause_walker(node, &context);

	return context.aggref_list;
}

/*
 * Deparse given operator expression.   To avoid problems around
 * priority of operations, we always parenthesize the arguments.
 */
static void
mongo_deparseOpExpr(OpExpr *node, deparse_expr_cxt *deparse_context)
{
	StringInfo	buf = deparse_context->buf;
	char	   *opname;
	ListCell   *arg;

	/* Sanity check. */
	Assert(list_length(node->args) == 2);

	/* Deparse left operand. */
	arg = list_head(node->args);
	mongo_deparseExpr(lfirst(arg), deparse_context);

	/* Deparse operator name. */
	opname = get_opname(node->opno);
	if (opname == NULL)
		elog(ERROR, "No such operator name for operator OID %u", node->opno);

	if (strcmp(opname, "->") == 0)
		appendStringInfoChar(buf, '.');
	else
		ereport(ERROR, \
				(errcode(ERRCODE_FDW_ERROR), \
				errmsg("Can not deparse %s operator", opname), \
				errhint("Only support deparse JSON arrow operators: \"->\""))); \

	/* Deparse right operand. */
	arg = list_tail(node->args);
	mongo_deparseExpr(lfirst(arg), deparse_context);
}

/*
 * Deparse given Var node into deparse_context->buf.
 *
 * If the Var belongs to the foreign relation, just print its remote name.
 * Otherwise, it's effectively a Param (and will in fact be a Param at
 * run time).  Handle it the same way we handle plain Params --- see
 * deparseParam for comments.
 */
static void
mongo_deparseVar(Var *node, deparse_expr_cxt *deparse_context)
{
	char	   *colname = NULL;

	if (node->varattno <= 0)
		elog(ERROR, "Could not support system attribute and whole row reference");

	colname = get_attname(deparse_context->qdoc_ctx->rel_oid, node->varattno, false);

	appendStringInfoString(deparse_context->buf, colname);
}

/*
 * Deparse given constant value into deparse_context->buf.
 *
 * This function has to be kept in sync with ruleutils.c's get_const_expr.
 * As for that function, showtype can be -1 to never show "::typename" decoration,
 * or +1 to always show it, or 0 to show it only if the constant wouldn't be assumed
 * to be the right type by default.
 */
static void
mongo_deparseConst(Const *node, deparse_expr_cxt *deparse_context)
{
	Oid			typoutput;
	bool		typIsVarlena;
	char	   *extval;

	if (node->constisnull)
	{
		appendStringInfoString(deparse_context->buf, "null");
		return;
	}

	getTypeOutputInfo(node->consttype,
					  &typoutput, &typIsVarlena);
	extval = OidOutputFunctionCall(typoutput, node->constvalue);

	appendStringInfoString(deparse_context->buf, extval);

	pfree(extval);

	return;
}

/*
 * Deparse a RelabelType (binary-compatible cast) node.
 * 	Discard explicit cast type.
 */
static void
mongo_deparseRelabelType(RelabelType *node, deparse_expr_cxt *deparse_context)
{
	mongo_deparseExpr(node->arg, deparse_context);
}

/*
 * Deparse given expression into deparse_context->buf.
 *
 * This function must support all the same node types that foreign_expr_walker
 * accepts.
 *
 * Note: unlike ruleutils.c, we just use a simple hard-wired parenthesization
 * scheme: anything more complex than a Var, Const, function call or cast
 * should be self-parenthesized.
 */
static void
mongo_deparseExpr(Expr *node, deparse_expr_cxt *deparse_context)
{
	if (node == NULL)
		return;

	switch (nodeTag(node))
	{
		case T_Var:
			mongo_deparseVar((Var *) node, deparse_context);
			break;
		case T_Const:
			mongo_deparseConst((Const *) node, deparse_context);
			break;
		case T_OpExpr:
			mongo_deparseOpExpr((OpExpr *) node, deparse_context);
			break;
		case T_RelabelType:
			mongo_deparseRelabelType((RelabelType *)node, deparse_context);
			break;
		default:
			elog(ERROR, "unsupported expression type for deparse: %d",
				 (int) nodeTag(node));
			break;
	}
}

/*
 * mongo_tlist_has_jsonb_arrow_op.
 *
 * Determine whether target list has Jsonb arrow operator
 * that is safe to pushdown.
 */
bool
mongo_tlist_has_jsonb_arrow_op(PlannerInfo *root, RelOptInfo *baserel, List *tlist)
{
	MongoOperatorsSupport opkind;
	List	 *input_tlist;
	ListCell *lc;
	bool	 json_op_safe = false;

	if (!IS_SIMPLE_REL(baserel))
		return false;

	input_tlist = (tlist) ? tlist : baserel->reltarget->exprs;

	/* Check Jsonb arrow operator "->" */
	foreach(lc, input_tlist)
	{
		Node *node = lfirst(lc);

		if (IsA(node, TargetEntry))
			node = (Node *) ((TargetEntry *) node)->expr;

		/*
		 * If any target expression is not pushdown, then we cannot
		 * push down Json arrow operator to the foreign server.
		 */
		if (!mongo_is_foreign_expr(root, baserel, (Expr *)node))
			return false;

		if (IsA(node, OpExpr))
		{
			OpExpr *oe = (OpExpr *) node;

			opkind = mongo_validateOperatorName(oe->opno, NULL, NULL);

			if (opkind == OP_JSON)
				json_op_safe = true;
			else
				return false;
		}
	}

	return json_op_safe;
}

/*
 * Append remote collection of specified foreign table to buf.
 * Use value of collection FDW option (if any) instead of relation's name.
 */
static void
mongo_deparseRelation(StringInfo buf, Relation rel)
{
	ForeignTable *table;
	const char *relname = NULL;
	ListCell   *lc;

	/* obtain additional catalog information. */
	table = GetForeignTable(RelationGetRelid(rel));

	/*
	 * Use value of FDW options if any, instead of the name of object itself.
	 */
	foreach(lc, table->options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, OPTION_NAME_COLLECTION) == 0)
			relname = defGetString(def);
	}

	if (relname == NULL)
		relname = RelationGetRelationName(rel);

	appendStringInfoString(buf, relname);
}

/*
 * Build a BSON query document based on aggregate API.
 */
BSON* mongo_build_bson_query_document(EState *estate, TupleDesc tupdesc, MongoPlanerInfo *plannerInfo)
{
	BSON *queryDocument = BsonCreate();
	qdoc_expr_cxt context;

	/* Initialize context params */
	context.estate = estate;
	context.rel_oid = plannerInfo->rel_oid;
	context.rtindex = plannerInfo->rtindex;
	context.conds_num = 0;
	context.reloptkind = plannerInfo->reloptkind;
	context.scan_reloptkind = plannerInfo->scan_reloptkind;
	context.target_ref_list = NIL;
	context.has_groupClause = plannerInfo->has_groupClause;
	context.has_grouping_agg = plannerInfo->has_grouping_agg;
	context.bs_key = NULL;
	context.need_aggexpr_syntax = false;
	context.agg_ref_list = NIL;
	context.count_boolexpr = 0;
	context.innerel_name = NULL;
	context.outerrel_name = NULL;
	context.innerel_name_list = NIL;

	mongo_aggregate_pipeline_query(estate, tupdesc, plannerInfo, &context, queryDocument);

	return queryDocument;
}

/*
 * Get function infor of scalar array.
 */
static void mongo_get_func_info_scalar_array (Oid const_array_type, Oid *consttype, PGFunction *func_addr)
{
	switch (const_array_type)
	{
		case INT2ARRAYOID:
			*consttype = INT2OID;
			*func_addr = int2in;
			break;
		case INT4ARRAYOID:
			*consttype = INT4OID;
			*func_addr = int4in;
			break;
		case INT8ARRAYOID:
			*consttype = INT8OID;
			*func_addr = int8in;
			break;
		case FLOAT4ARRAYOID:
			*consttype = FLOAT4OID;
			*func_addr = float4in;
			break;
		case FLOAT8ARRAYOID:
		case NUMERICARRAYOID:
			*consttype = FLOAT8OID;
			*func_addr = float8in;
			break;
		case BOOLARRAYOID:
			*consttype = BOOLOID;
			*func_addr = boolin;
			break;
		case BPCHARARRAYOID:
		case VARCHARARRAYOID:
		case TEXTARRAYOID:
			*consttype = TEXTOID;
			*func_addr = textin;
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
					errmsg("cannot get function information for ScalarArrayOpExpr constant"),
					errhint("Constant value data type: %u", (uint32) const_array_type)));
			break;
	}
}
