/*-------------------------------------------------------------------------
 *
 * mongo_fdw.c
 * 		Foreign-data wrapper for remote MongoDB servers
 *
 * Portions Copyright (c) 2012-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 2004-2021, EnterpriseDB Corporation.
 * Portions Copyright (c) 2012–2014 Citus Data, Inc.
 * Portions Copyright (c) 2021, TOSHIBA CORPORATION
 *
 * IDENTIFICATION
 * 		mongo_fdw.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "mongo_wrapper.h"

#if PG_VERSION_NUM >= 90300
#include "access/htup_details.h"
#endif
#if PG_VERSION_NUM < 120000
#include "access/sysattr.h"
#endif
#if PG_VERSION_NUM >= 120000
#include "access/table.h"
#endif
#include "catalog/heap.h"
#include "catalog/pg_type.h"
#if PG_VERSION_NUM >= 130000
#include "common/hashfn.h"
#include "common/jsonapi.h"
#endif
#include "miscadmin.h"
#include "mongo_fdw.h"
#include "mongo_query.h"
#if PG_VERSION_NUM >= 140000
#include "optimizer/appendinfo.h"
#endif
#if PG_VERSION_NUM >= 120000
#include "optimizer/optimizer.h"
#endif
#if PG_VERSION_NUM < 120000
#include "optimizer/var.h"
#endif
#include "parser/parsetree.h"
#include "storage/ipc.h"
#include "utils/jsonb.h"
#if PG_VERSION_NUM < 130000
#include "utils/jsonapi.h"
#else
#include "utils/jsonfuncs.h"
#endif
#include "utils/rel.h"
#include "utils/guc.h"
#include "utils/float.h"
#include "optimizer/tlist.h"

/* Declarations for dynamic loading */
PG_MODULE_MAGIC;

/*
 * In PG 9.5.1 the number will be 90501,
 * our version is 5.2.10 so number will be 50210
 */
#define CODE_VERSION   50210

extern PGDLLEXPORT void _PG_init(void);
const char *EscapeJsonString(const char *string);
void BsonToJsonString(StringInfo output, BSON_ITERATOR iter, bool isArray);

PG_FUNCTION_INFO_V1(mongo_fdw_handler);
PG_FUNCTION_INFO_V1(mongo_fdw_version);

/* FDW callback routines */
static void MongoGetForeignRelSize(PlannerInfo *root,
								   RelOptInfo *baserel,
								   Oid foreigntableid);
static void MongoGetForeignPaths(PlannerInfo *root,
								 RelOptInfo *baserel,
								 Oid foreigntableid);
static ForeignScan *MongoGetForeignPlan(PlannerInfo *root,
										RelOptInfo *foreignrel,
										Oid foreigntableid,
										ForeignPath *best_path,
										List *targetlist,
										List *restrictionClauses,
										Plan *outer_plan);
static void MongoExplainForeignScan(ForeignScanState *node, ExplainState *es);
static void MongoBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *MongoIterateForeignScan(ForeignScanState *node);
static void MongoEndForeignScan(ForeignScanState *node);
static void MongoReScanForeignScan(ForeignScanState *node);
static TupleTableSlot *MongoExecForeignUpdate(EState *estate,
											  ResultRelInfo *resultRelInfo,
											  TupleTableSlot *slot,
											  TupleTableSlot *planSlot);
static TupleTableSlot *MongoExecForeignDelete(EState *estate,
											  ResultRelInfo *resultRelInfo,
											  TupleTableSlot *slot,
											  TupleTableSlot *planSlot);
static void MongoEndForeignModify(EState *estate,
								  ResultRelInfo *resultRelInfo);
#if PG_VERSION_NUM >= 140000
static void MongoAddForeignUpdateTargets(PlannerInfo *root,
										 Index rtindex,
										 RangeTblEntry *target_rte,
										 Relation target_relation);
#else
static void MongoAddForeignUpdateTargets(Query *parsetree,
										 RangeTblEntry *target_rte,
										 Relation target_relation);
#endif
static void MongoBeginForeignModify(ModifyTableState *mtstate,
									ResultRelInfo *resultRelInfo,
									List *fdw_private,
									int subplan_index,
									int eflags);
static TupleTableSlot *MongoExecForeignInsert(EState *estate,
											  ResultRelInfo *resultRelInfo,
											  TupleTableSlot *slot,
											  TupleTableSlot *planSlot);
static List *MongoPlanForeignModify(PlannerInfo *root,
									ModifyTable *plan,
									Index resultRelation,
									int subplan_index);
static void MongoExplainForeignModify(ModifyTableState *mtstate,
									  ResultRelInfo *rinfo,
									  List *fdw_private,
									  int subplan_index,
									  ExplainState *es);
static bool MongoAnalyzeForeignTable(Relation relation,
									 AcquireSampleRowsFunc *func,
									 BlockNumber *totalpages);
#if PG_VERSION_NUM >= 110000
static void MongoBeginForeignInsert(ModifyTableState *mtstate,
									ResultRelInfo *resultRelInfo);
static void MongoEndForeignInsert(EState *estate,
								  ResultRelInfo *resultRelInfo);
#endif

static void MongoGetForeignJoinPaths(PlannerInfo *root,
										RelOptInfo *joinrel,
										RelOptInfo *outerrel,
										RelOptInfo *innerrel,
										JoinType jointype,
										JoinPathExtraData *extra);

static void MongoGetForeignUpperPaths(PlannerInfo *root,
									  UpperRelationKind stage,
									  RelOptInfo *input_rel,
									  RelOptInfo *output_rel,
									  void *extra);

/*
 * Helper functions
 */
static double ForeignTableDocumentCount(Oid foreignTableId);
static void FillTupleSlot(const BSON *bsonDocument,
						  const char *bsonDocumentKey,
						  MongoPlanerInfo *plannerInfo,
						  TupleDesc tupleDescriptor,
						  Datum *columnValues,
						  bool *columnNulls,
						  bool is_agg);
static void FillTupleSlotAgg(const BSON *bsonDocument,
							 const char *bsonDocumentKey,
							 MongoPlanerInfo *plannerInfo,
							 TupleDesc tupleDescriptor,
							 Datum *columnValues,
							 bool *columnNulls);
static void FillTupleSlotAttr(const BSON *bsonDocument,
							  const char *bsonDocumentKey,
							  MongoPlanerInfo *plannerInfo,
							  TupleDesc tupleDescriptor,
							  Datum *columnValues,
							  bool *columnNulls);
static bool ColumnTypesCompatible(BSON_TYPE bsonType, Oid columnTypeId);
static Datum ColumnValueArray(BSON_ITERATOR *bsonIterator, Oid valueTypeId);
static Datum ColumnValue(BSON_ITERATOR *bsonIterator,
						 Oid columnTypeId,
						 int32 columnTypeMod);
static void MongoFreeScanState(MongoFdwScanState *fsstate);
static void MongoFreeModifyState(MongoFdwModifyState *fmstate);
static int MongoAcquireSampleRows(Relation relation,
								  int errorLevel,
								  HeapTuple *sampleRows,
								  int targetRowCount,
								  double *totalRowCount,
								  double *totalDeadRowCount);
static void mongo_fdw_exit(int code, Datum arg);
static void mongo_BsonToStringValue(StringInfo output, BSON_ITERATOR *bsIterator, BSON_TYPE bsonType);
static void mongo_get_join_planner_info(RelOptInfo *scanrel, MongoPlanerInfo *plannerInfo);
static void mongo_get_limit_info(PlannerInfo *root, MongoPlanerInfo *plannerInfo);

/* The null action object used for pure validation */
#if PG_VERSION_NUM < 130000
static JsonSemAction nullSemAction =
{
	NULL, NULL, NULL, NULL, NULL,
	NULL, NULL, NULL, NULL, NULL
};
#else
JsonSemAction nullSemAction =
{
	NULL, NULL, NULL, NULL, NULL,
	NULL, NULL, NULL, NULL, NULL
};
#endif

/*
 * This enum describes what's kept in the fdw_private list for a ForeignPath.
 * We store:
 *
 * 1) Boolean flag showing if the remote query has the final sort
 * 2) Boolean flag showing if the remote query has the LIMIT clause
 */
enum FdwPathPrivateIndex
{
	/* has-final-sort flag (as an integer Value node) */
	FdwPathPrivateHasFinalSort,
	/* has-limit flag (as an integer Value node) */
	FdwPathPrivateHasLimit
};

/*
 * Library load-time initalization, sets on_proc_exit() callback for
 * backend shutdown.
 */
void
_PG_init(void)
{
#ifdef META_DRIVER
	/* Initialize MongoDB C driver */
	mongoc_init();
#endif

	on_proc_exit(&mongo_fdw_exit, PointerGetDatum(NULL));
}

/*
 * mongo_fdw_handler
 *		Creates and returns a struct with pointers to foreign table callback
 *		functions.
 */
Datum
mongo_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdwRoutine = makeNode(FdwRoutine);

	/* Functions for scanning foreign tables */
	fdwRoutine->GetForeignRelSize = MongoGetForeignRelSize;
	fdwRoutine->GetForeignPaths = MongoGetForeignPaths;
	fdwRoutine->GetForeignPlan = MongoGetForeignPlan;
	fdwRoutine->BeginForeignScan = MongoBeginForeignScan;
	fdwRoutine->IterateForeignScan = MongoIterateForeignScan;
	fdwRoutine->ReScanForeignScan = MongoReScanForeignScan;
	fdwRoutine->EndForeignScan = MongoEndForeignScan;

	/* Support for insert/update/delete */
	fdwRoutine->AddForeignUpdateTargets = MongoAddForeignUpdateTargets;
	fdwRoutine->PlanForeignModify = MongoPlanForeignModify;
	fdwRoutine->BeginForeignModify = MongoBeginForeignModify;
	fdwRoutine->ExecForeignInsert = MongoExecForeignInsert;
	fdwRoutine->ExecForeignUpdate = MongoExecForeignUpdate;
	fdwRoutine->ExecForeignDelete = MongoExecForeignDelete;
	fdwRoutine->EndForeignModify = MongoEndForeignModify;

	/* Support for EXPLAIN */
	fdwRoutine->ExplainForeignScan = MongoExplainForeignScan;
	fdwRoutine->ExplainForeignModify = MongoExplainForeignModify;

	/* Support for ANALYZE */
	fdwRoutine->AnalyzeForeignTable = MongoAnalyzeForeignTable;

#if PG_VERSION_NUM >= 110000
	/* Partition routing and/or COPY from */
	fdwRoutine->BeginForeignInsert = MongoBeginForeignInsert;
	fdwRoutine->EndForeignInsert = MongoEndForeignInsert;
#endif

	/* Support functions for join push-down */
	fdwRoutine->GetForeignJoinPaths = MongoGetForeignJoinPaths;

	/* Support functions for upper relation push-down */
	fdwRoutine->GetForeignUpperPaths = MongoGetForeignUpperPaths;

	PG_RETURN_POINTER(fdwRoutine);
}

/*
 * mongo_fdw_exit
 *		Exit callback function.
 */
static void
mongo_fdw_exit(int code, Datum arg)
{
	mongo_cleanup_connection();
#ifdef META_DRIVER
	/* Release all memory and other resources allocated by the driver */
	mongoc_cleanup();
#endif
}

/*
 * MongoGetForeignRelSize
 * 		Obtains relation size estimates for mongo foreign table.
 */
static void
MongoGetForeignRelSize(PlannerInfo *root,
					   RelOptInfo *baserel,
					   Oid foreigntableid)
{
	double		documentCount = ForeignTableDocumentCount(foreigntableid);
	MongoFdwRelationInfo *fpinfo;
	ListCell   *lc;

	/*
	 * We use MongoFdwRelationInfo to pass various information to subsequent
	 * functions.
	 */
	fpinfo = (MongoFdwRelationInfo *) palloc0(sizeof(MongoFdwRelationInfo));
	baserel->fdw_private = (void *) fpinfo;

	/* Base foreign tables need to be pushed down always. */
	fpinfo->pushdown_safe = true;

	/* Set the relation index. */
	fpinfo->relation_index = baserel->relid;

	/*
	 * Identify which baserestrictinfo clauses can be sent to the remote
	 * server and which can't.  Only the OpExpr clauses are sent to the remote
	 * server.
	 */
	foreach(lc, baserel->baserestrictinfo)
	{
		RestrictInfo *ri = (RestrictInfo *) lfirst(lc);

		/* Does not support "WHERE column" where column has boolean type */
		if (IsA(ri->clause, Var))
			fpinfo->local_conds = lappend(fpinfo->local_conds, ri);
		else if (mongo_is_foreign_expr(root, baserel, ri->clause))
			fpinfo->remote_conds = lappend(fpinfo->remote_conds, ri);
		else
			fpinfo->local_conds = lappend(fpinfo->local_conds, ri);
	}

	if (documentCount > 0.0)
	{
		double		rowSelectivity;

		/*
		 * We estimate the number of rows returned after restriction
		 * qualifiers are applied.  This will be more accurate if analyze is
		 * run on this relation.
		 */
		rowSelectivity = clauselist_selectivity(root,
												baserel->baserestrictinfo,
												0, JOIN_INNER, NULL);
		baserel->rows = clamp_row_est(documentCount * rowSelectivity);
	}
	else
		ereport(DEBUG1,
				(errmsg("could not retrieve document count for collection"),
				 errhint("Falling back to default estimates in planning.")));
}

/*
 * MongoGetForeignPaths
 *		Creates the only scan path used to execute the query.
 *
 * Note that MongoDB may decide to use an underlying index for this scan, but
 * that decision isn't deterministic or visible to us.  We therefore create a
 * single table scan path.
 */
static void
MongoGetForeignPaths(PlannerInfo *root,
					 RelOptInfo *baserel,
					 Oid foreigntableid)
{
	double		tupleFilterCost = baserel->baserestrictcost.per_tuple;
	double		inputRowCount;
	double		documentSelectivity;
	double		foreignTableSize;
	int32		documentWidth;
	BlockNumber pageCount;
	double		totalDiskAccessCost;
	double		cpuCostPerDoc;
	double		cpuCostPerRow;
	double		totalCpuCost;
	double		connectionCost;
	double 		documentCount;
	List	   *opExpressionList;
	Cost		startupCost = 0.0;
	Cost		totalCost = 0.0;
	Path	   *foreignPath;
	MongoFdwRelationInfo *fpinfo = (MongoFdwRelationInfo *) baserel->fdw_private;

	documentCount = ForeignTableDocumentCount(foreigntableid);

	if (documentCount > 0.0)
	{
		/*
		 * We estimate the number of rows returned after restriction
		 * qualifiers are applied by MongoDB.
		 */
		opExpressionList = fpinfo->remote_conds;
		documentSelectivity = clauselist_selectivity(root, opExpressionList,
													 0, JOIN_INNER, NULL);
		inputRowCount = clamp_row_est(documentCount * documentSelectivity);

		/*
		 * We estimate disk costs assuming a sequential scan over the data.
		 * This is an inaccurate assumption as Mongo scatters the data over
		 * disk pages, and may rely on an index to retrieve the data.  Still,
		 * this should at least give us a relative cost.
		 */
		documentWidth = get_relation_data_width(foreigntableid,
												baserel->attr_widths);
		foreignTableSize = documentCount * documentWidth;

		pageCount = (BlockNumber) rint(foreignTableSize / BLCKSZ);
		totalDiskAccessCost = seq_page_cost * pageCount;

		/*
		 * The cost of processing a document returned by Mongo (input row) is
		 * 5x the cost of processing a regular row.
		 */
		cpuCostPerDoc = cpu_tuple_cost;
		cpuCostPerRow = (cpu_tuple_cost * MONGO_TUPLE_COST_MULTIPLIER) + tupleFilterCost;
		totalCpuCost = (cpuCostPerDoc * documentCount) +(cpuCostPerRow * inputRowCount);

		connectionCost = MONGO_CONNECTION_COST_MULTIPLIER * seq_page_cost;
		startupCost = baserel->baserestrictcost.startup + connectionCost;
		totalCost = startupCost + totalDiskAccessCost + totalCpuCost;
	}
	else
		ereport(DEBUG1,
				(errmsg("could not retrieve document count for collection"),
				 errhint("Falling back to default estimates in planning.")));

	/* Create a foreign path node */
	foreignPath = (Path *) create_foreignscan_path(root, baserel,
#if PG_VERSION_NUM >= 90600
												   NULL,	/* default pathtarget */
#endif
												   baserel->rows,
												   startupCost,
												   totalCost,
												   NIL, /* no pathkeys */
												   baserel->lateral_relids,
#if PG_VERSION_NUM >= 90500
												   NULL,	/* no extra plan */
#endif
												   NULL);	/* no fdw_private data */

	/* Add foreign path as the only possible path */
	add_path(baserel, foreignPath);
}

/*
 * MongoGetForeignPlan
 *		Creates a foreign scan plan node for scanning the MongoDB collection.
 *
 * Note that MongoDB may decide to use an underlying index for this
 * scan, but that decision isn't deterministic or visible to us.
 */
static ForeignScan *
MongoGetForeignPlan(PlannerInfo *root,
					RelOptInfo *foreignrel,
					Oid foreigntableid,
					ForeignPath *best_path,
					List *targetList,
					List *restrictionClauses,
					Plan *outer_plan)
{
	MongoFdwRelationInfo *fpinfo = (MongoFdwRelationInfo *) foreignrel->fdw_private;
	Index		scanRangeTableIndex;
	ForeignScan *foreignScan;
	List	   *scan_var_list;
	List	   *fdw_scan_tlist = NIL;
	List	   *fdw_recheck_quals = NIL;
	ListCell   *lc;
	List	   *local_exprs = NIL;
	List	   *remote_exprs = NIL;
	List	   *plannerInfoList = NIL;
	MongoPlanerInfo *plannerInfo = NULL;
	RelOptInfo *scanrel = NULL;
	bool		tlist_has_jsonb_arrow_op;
	bool		has_limit = false;

	/* Decide to execute JsonB arrow operator support in the target list. */
	tlist_has_jsonb_arrow_op = mongo_tlist_has_jsonb_arrow_op(root, foreignrel, targetList);

	/*
	 * Get FDW private data created by MongoGetForeignUpperPaths(), if any.
	 */
	if (best_path->fdw_private)
	{
		has_limit = intVal(list_nth(best_path->fdw_private,
									FdwPathPrivateHasLimit));
	}

#if PG_VERSION_NUM >= 90600
	scan_var_list = pull_var_clause((Node *) foreignrel->reltarget->exprs,
									PVC_RECURSE_PLACEHOLDERS | PVC_RECURSE_AGGREGATES);
#else
	scan_var_list = pull_var_clause((Node *) foreignrel->reltargetlist,
									PVC_RECURSE_AGGREGATES,
									PVC_RECURSE_PLACEHOLDERS);
#endif

	/* System attributes are not allowed. */
	foreach(lc, scan_var_list)
	{
		Var		   *var = lfirst(lc);
		const FormData_pg_attribute *attr;

		Assert(IsA(var, Var));

		if (var->varattno >= 0)
			continue;

#if PG_VERSION_NUM >= 120000
		attr = SystemAttributeDefinition(var->varattno);
#else
		attr = SystemAttributeDefinition(var->varattno, false);
#endif
		ereport(ERROR,
				(errcode(ERRCODE_FDW_COLUMN_NAME_NOT_FOUND),
				 errmsg("system attribute \"%s\" can't be fetched from remote relation",
						attr->attname.data)));
	}

	if (IS_SIMPLE_REL(foreignrel))
	{
		/*
		 * For base relations, set scanRangeTableIndex as the relid of the relation.
		 */
		scanRangeTableIndex = foreignrel->relid;

		/*
		* Separate the restrictionClauses into those that can be executed remotely
		* and those that can't.  baserestrictinfo clauses that were previously
		* determined to be safe or unsafe are shown in fpinfo->remote_conds and
		* fpinfo->local_conds.  Anything else in the restrictionClauses list will
		* be a join clause, which we have to check for remote-safety.  Only the
		* OpExpr clauses are sent to the remote server.
		*/
		foreach(lc, restrictionClauses)
		{
			RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

			Assert(IsA(rinfo, RestrictInfo));

			/* Ignore pseudoconstants, they are dealt with elsewhere */
			if (rinfo->pseudoconstant)
				continue;

			if (list_member_ptr(fpinfo->remote_conds, rinfo))
				remote_exprs = lappend(remote_exprs, rinfo->clause);
			else if (list_member_ptr(fpinfo->local_conds, rinfo))
				local_exprs = lappend(local_exprs, rinfo->clause);
			else if (IsA(rinfo->clause, OpExpr) &&
					mongo_is_foreign_expr(root, foreignrel, rinfo->clause))
				remote_exprs = lappend(remote_exprs, rinfo->clause);
			else
				local_exprs = lappend(local_exprs, rinfo->clause);
		}

		/*
		 * For a base-relation scan, we have to support EPQ recheck, which
		 * should recheck all the remote quals.
		 */
		fdw_recheck_quals = remote_exprs;

		/*
		 * Build the list of columns that contain Jsonb arrow operator
		 * to be fetched from the foreign server.
		 */
		if (tlist_has_jsonb_arrow_op == true)
		{
			if (targetList)
				fdw_scan_tlist = list_copy(targetList);
			else
				fdw_scan_tlist = add_to_flat_tlist(fdw_scan_tlist, foreignrel->reltarget->exprs);

			foreach(lc, remote_exprs)
			{
				Node *node = (Node *)lfirst(lc);

				fdw_scan_tlist = add_to_flat_tlist(fdw_scan_tlist,
													pull_var_clause((Node *) node,
																	PVC_RECURSE_PLACEHOLDERS));
			}
			foreach(lc, local_exprs)
			{
				Node *node = (Node *)lfirst(lc);

				fdw_scan_tlist = add_to_flat_tlist(fdw_scan_tlist,
													pull_var_clause((Node *) node,
																	PVC_RECURSE_PLACEHOLDERS));
			}
		}
	}
	else
	{
		/*
		 * Join relation or upper relation - set scanRangeTableIndex to 0.
		 */
		scanRangeTableIndex = 0;

		/*
		 * For a join rel, baserestrictinfo is NIL and we are not considering
		 * parameterization right now, so there should be no restrictionClauses
		 * for a joinrel or an upper rel either.
		 */
		Assert(!restrictionClauses);

		/*
		 * Instead we get the conditions to apply from the fdw_private
		 * structure.
		 */
		remote_exprs = extract_actual_clauses(fpinfo->remote_conds, false);
		local_exprs = extract_actual_clauses(fpinfo->local_conds, false);

		/*
		 * We leave fdw_recheck_quals empty in this case, since we never need
		 * to apply EPQ recheck clauses.  In the case of a joinrel, EPQ
		 * recheck is handled elsewhere --- see MongoGetForeignJoinPaths().
		 * If we're planning an upperrel (ie, remote grouping or aggregation)
		 * then there's no EPQ to do because SELECT FOR UPDATE wouldn't be
		 * allowed, and indeed we *can't* put the remote clauses into
		 * fdw_recheck_quals because the unaggregated Vars won't be available
		 * locally.
		 */

		/* Build the list of columns to be fetched from the foreign server. */
		fdw_scan_tlist = mongo_build_tlist_to_deparse(foreignrel);

		/*
		 * Ensure that the outer plan produces a tuple whose descriptor
		 * matches our scan tuple slot.  Also, remove the local conditions
		 * from outer plan's quals, lest they be evaluated twice, once by the
		 * local plan and once by the scan.
		 */
		if (outer_plan)
		{
			ListCell   *lc;

			/*
			 * Right now, we only consider grouping and aggregation beyond
			 * joins. Queries involving aggregates or grouping do not require
			 * EPQ mechanism, hence should not have an outer plan here.
			 */
			Assert(!IS_UPPER_REL(foreignrel));

			/*
			 * First, update the plan's qual list if possible.  In some cases
			 * the quals might be enforced below the topmost plan level, in
			 * which case we'll fail to remove them; it's not worth working
			 * harder than this.
			 */
			foreach(lc, local_exprs)
			{
				Node	   *qual = lfirst(lc);

				outer_plan->qual = list_delete(outer_plan->qual, qual);

				/*
				 * For an inner join the local conditions of foreign scan plan
				 * can be part of the joinquals as well.  (They might also be
				 * in the mergequals or hashquals, but we can't touch those
				 * without breaking the plan.)
				 */
				if (IsA(outer_plan, NestLoop) ||
					IsA(outer_plan, MergeJoin) ||
					IsA(outer_plan, HashJoin))
				{
					Join	   *join_plan = (Join *) outer_plan;

					if (join_plan->jointype == JOIN_INNER)
						join_plan->joinqual = list_delete(join_plan->joinqual,
														  qual);
				}
			}

			/*
			 * Now fix the subplan's tlist --- this might result in inserting
			 * a Result node atop the plan tree.
			 */
			outer_plan = change_plan_targetlist(outer_plan, fdw_scan_tlist,
												best_path->path.parallel_safe);
		}
	}

	/* Serialize plannerInfo */
	plannerInfo = (MongoPlanerInfo *) palloc0(sizeof(MongoPlanerInfo));
	plannerInfo->tlist = fdw_scan_tlist;
	plannerInfo->tlist_has_jsonb_arrow_op = tlist_has_jsonb_arrow_op;
	plannerInfo->reloptkind = foreignrel->reloptkind;
	plannerInfo->scan_reloptkind = IS_UPPER_REL(foreignrel) ?
									fpinfo->outerrel->reloptkind : foreignrel->reloptkind;
	plannerInfo->rtindex = foreignrel->relid;

	/*
	 * For upper relations, the WHERE clause is built from the remote
	 * conditions of the underlying scan relation; otherwise, we can use the
	 * supplied list of remote conditions directly.
	 */
	if (IS_UPPER_REL(foreignrel))
	{
		MongoFdwRelationInfo *ofpinfo;

		scanrel = fpinfo->outerrel;

		ofpinfo = (MongoFdwRelationInfo *) scanrel->fdw_private;
		plannerInfo->remote_exprs = ofpinfo->remote_conds;
		plannerInfo->local_exprs = ofpinfo->local_conds;
		plannerInfo->having_quals = remote_exprs;
	}
	else
	{
		scanrel = foreignrel;

		plannerInfo->remote_exprs = remote_exprs;
		plannerInfo->local_exprs = local_exprs;
		plannerInfo->ptarget_exprs = foreignrel->reltarget->exprs;
	}

	plannerInfo->has_limit = has_limit;
	if (has_limit)
		mongo_get_limit_info(root, plannerInfo);
	plannerInfo->has_groupClause = (root->parse->groupClause) ? true : false;
	plannerInfo->has_grouping_agg = (root->parse->groupClause || root->parse->groupingSets ||
									 root->parse->hasAggs || (root->hasHavingQual && root->parse->havingQual));

	/* Pickup information of JOIN relation */
	if (IS_JOIN_REL(scanrel))
	{
		MongoFdwRelationInfo *f_joininfo = (MongoFdwRelationInfo *) scanrel->fdw_private;

		plannerInfo->jointype = f_joininfo->jointype;
		mongo_get_join_planner_info(scanrel, plannerInfo);
	}

	plannerInfoList = mongo_serialize_plannerInfoList(plannerInfo);

	/* Create the foreign scan node */
	foreignScan = make_foreignscan(targetList, local_exprs,
								   scanRangeTableIndex,
								   NIL, /* No expressions to evaluate */
								   plannerInfoList
#if PG_VERSION_NUM >= 90500
								   ,fdw_scan_tlist
								   ,fdw_recheck_quals
								   ,outer_plan
#endif
		);

	return foreignScan;
}

/*
 * MongoExplainForeignScan
 *		Produces extra output for the Explain command.
 */
static void
MongoExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
	MongoFdwScanState *fsstate = (MongoFdwScanState *) node->fdw_state;
	ForeignScan *fsplan = (ForeignScan *) node->ss.ps.plan;
	EState	   *estate = node->ss.ps.state;
	MongoFdwOptions *options;
	StringInfo	namespaceName;
	RangeTblEntry *rte;
	int			rtindex;

	if (fsplan->scan.scanrelid > 0)
		rtindex = fsplan->scan.scanrelid;
	else
		rtindex = bms_next_member(fsplan->fs_relids, -1);
	rte = exec_rt_fetch(rtindex, estate);

	options = mongo_get_options(rte->relid);

	/* Construct fully qualified collection name */
	namespaceName = makeStringInfo();
	appendStringInfo(namespaceName, "%s.%s", options->svr_database,
					 options->collectionName);

	mongo_free_options(options);

	ExplainPropertyText("Foreign Namespace", namespaceName->data, es);

	if (es->verbose)
	{
		char *queryDocument_str = NULL;

		queryDocument_str = bson_as_canonical_extended_json(fsstate->queryDocument, NULL);
		ExplainPropertyText("Query document", queryDocument_str, es);
		bson_free(queryDocument_str);
	}
}

static void
MongoExplainForeignModify(ModifyTableState *mtstate,
						  ResultRelInfo *rinfo,
						  List *fdw_private,
						  int subplan_index,
						  ExplainState *es)
{
	MongoFdwOptions *options;
	StringInfo	namespaceName;
	Oid			foreignTableId;

	foreignTableId = RelationGetRelid(rinfo->ri_RelationDesc);
	options = mongo_get_options(foreignTableId);

	/* Construct fully qualified collection name */
	namespaceName = makeStringInfo();
	appendStringInfo(namespaceName, "%s.%s", options->svr_database,
					 options->collectionName);

	mongo_free_options(options);
	ExplainPropertyText("Foreign Namespace", namespaceName->data, es);
}

/*
 * MongoBeginForeignScan
 *		Connects to the MongoDB server, and opens a cursor that uses the
 *		database name, collection name, and the remote query to send to the
 *		server.
 *
 * The function also creates a hash table that maps referenced
 * column names to column index and type information.
 */
static void
MongoBeginForeignScan(ForeignScanState *node, int eflags)
{
	MongoFdwOptions *options;
	MongoFdwScanState *fsstate;
	RangeTblEntry *rte;
	EState	   *estate = node->ss.ps.state;
	ForeignScan *fsplan = (ForeignScan *) node->ss.ps.plan;
	Oid			userid;
	ForeignServer *server;
	UserMapping *user;
	ForeignTable *table;
	List		*plannerInfoList;
	TupleTableSlot *tupleSlot = node->ss.ss_ScanTupleSlot;
	int			rtindex;

	/*
	 * Identify which user to do the remote access as.  This should match what
	 * ExecCheckRTEPerms() does.  In case of a join or aggregate, use the
	 * lowest-numbered member RTE as a representative; we would get the same
	 * result from any.
	 */
	if (fsplan->scan.scanrelid > 0)
		rtindex = fsplan->scan.scanrelid;
	else
		rtindex = bms_next_member(fsplan->fs_relids, -1);
	rte = exec_rt_fetch(rtindex, estate);
	userid = rte->checkAsUser ? rte->checkAsUser : GetUserId();

	options = mongo_get_options(rte->relid);

	fsstate = (MongoFdwScanState *) palloc0(sizeof(MongoFdwScanState));
	node->fdw_state = (void *) fsstate;

	/* Get information from planner */
	plannerInfoList = fsplan->fdw_private;
	fsstate->plannerInfo = mongo_deserialize_plannerInfoList(plannerInfoList);
	fsstate->plannerInfo->rel_oid = (rte) ? rte->relid : 0;

	/* Construct the BSON query document. */
	fsstate->queryDocument = mongo_build_bson_query_document(estate,
															 tupleSlot->tts_tupleDescriptor,
															 fsstate->plannerInfo);

	/* If Explain with no Analyze, do nothing */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	/* Get info about foreign table. */
	fsstate->rel = node->ss.ss_currentRelation;
	table = GetForeignTable(rte->relid);
	server = GetForeignServer(table->serverid);
	user = GetUserMapping(userid, server->serverid);

	fsstate->options = options;

	/*
	 * Get connection to the foreign server.  Connection manager will establish
	 * new connection if necessary.
	 */
	fsstate->mongoConnection = mongo_get_connection(server, user, options);
}

/*
 * MongoIterateForeignScan
 *		Opens a Mongo cursor that uses the database name, collection name, and
 *		the remote query to send to the server.
 *
 *	Reads the next document from MongoDB, converts it to a PostgreSQL tuple,
 *	and stores the converted tuple into the ScanTupleSlot as a virtual tuple.
 */
static TupleTableSlot *
MongoIterateForeignScan(ForeignScanState *node)
{
	MongoFdwScanState *fsstate = (MongoFdwScanState *) node->fdw_state;
	TupleTableSlot *tupleSlot = node->ss.ss_ScanTupleSlot;
	MONGO_CURSOR *mongoCursor = fsstate->mongoCursor;
	ForeignScan *foreignScan = (ForeignScan *) node->ss.ps.plan;
	TupleDesc	tupleDescriptor = tupleSlot->tts_tupleDescriptor;
	Datum	   *columnValues = tupleSlot->tts_values;
	bool	   *columnNulls = tupleSlot->tts_isnull;
	int32		columnCount = tupleDescriptor->natts;
	bool		is_agg;

	if (foreignScan->scan.scanrelid > 0 &&
		fsstate->plannerInfo->tlist_has_jsonb_arrow_op == false)
		is_agg = false;
	else
		is_agg = true;

	/* Create cursor for collection name and set query */
	if (mongoCursor == NULL)
	{
		MongoPlanerJoinInfo *join_info;
		char *most_outerrel_name = NULL;
		char *collection_name;

		if (fsstate->plannerInfo->joininfo_list != NIL)
		{
			join_info = (MongoPlanerJoinInfo *)lfirst(list_head(fsstate->plannerInfo->joininfo_list));
			most_outerrel_name = join_info->outerrel_name;
		}

		collection_name = (most_outerrel_name) ?
							most_outerrel_name :
							fsstate->options->collectionName;
		mongoCursor = MongoCursorCreate(fsstate->mongoConnection,
										fsstate->options->svr_database,
										collection_name,
										fsstate->queryDocument, true);

		/* Save mongoCursor */
		fsstate->mongoCursor = mongoCursor;
	}

	/*
	 * We execute the protocol to load a virtual tuple into a slot. We first
	 * call ExecClearTuple, then fill in values / isnull arrays, and last call
	 * ExecStoreVirtualTuple.  If we are done fetching documents from Mongo,
	 * we just return an empty slot as required.
	 */
	ExecClearTuple(tupleSlot);

	/* Initialize all values for this row to null */
	memset(columnValues, 0, columnCount * sizeof(Datum));
	memset(columnNulls, true, columnCount * sizeof(bool));

	if (MongoCursorNext(mongoCursor, NULL))
	{
		const BSON *bsonDocument = MongoCursorBson(mongoCursor);
		const char *bsonDocumentKey = NULL; /* Top level document */

		FillTupleSlot(bsonDocument, bsonDocumentKey,
						fsstate->plannerInfo,
						tupleDescriptor,
						columnValues,
						columnNulls,
						is_agg);

		ExecStoreVirtualTuple(tupleSlot);
	}

	return tupleSlot;
}

/*
 * MongoEndForeignScan
 *		Finishes scanning the foreign table, closes the cursor and the
 *		connection to MongoDB, and reclaims scan related resources.
 */
static void
MongoEndForeignScan(ForeignScanState *node)
{
	MongoFdwScanState *fsstate;

	fsstate = (MongoFdwScanState *) node->fdw_state;
	/* If we executed a query, reclaim mongo related resources */
	if (fsstate != NULL)
	{
		if (fsstate->options)
		{
			mongo_free_options(fsstate->options);
			fsstate->options = NULL;
		}
		MongoFreeScanState(fsstate);
	}
}

/*
 * MongoReScanForeignScan
 *		Rescans the foreign table.
 *
 * Note that rescans in Mongo end up being notably more expensive than what the
 * planner expects them to be, since MongoDB cursors don't provide reset/rewind
 * functionality.
 */
static void
MongoReScanForeignScan(ForeignScanState *node)
{
	MongoFdwScanState *fsstate = (MongoFdwScanState *) node->fdw_state;

	/* Close down the old cursor */
	if (fsstate->mongoCursor)
	{
		MongoCursorDestroy(fsstate->mongoCursor);
		fsstate->mongoCursor = NULL;
	}
}

static List *
MongoPlanForeignModify(PlannerInfo *root,
					   ModifyTable *plan,
					   Index resultRelation,
					   int subplan_index)
{
	CmdType		operation = plan->operation;
	RangeTblEntry *rte = planner_rt_fetch(resultRelation, root);
	Relation	rel;
	List	   *targetAttrs = NIL;

	/*
	 * Core code already has some lock on each rel being planned, so we can
	 * use NoLock here.
	 */
#if PG_VERSION_NUM < 130000
	rel = heap_open(rte->relid, NoLock);
#else
	rel = table_open(rte->relid, NoLock);
#endif
	if (operation == CMD_INSERT)
	{
		TupleDesc	tupdesc = RelationGetDescr(rel);
		int			attnum;

		for (attnum = 1; attnum <= tupdesc->natts; attnum++)
		{
#if PG_VERSION_NUM < 110000
			Form_pg_attribute attr = tupdesc->attrs[attnum - 1];
#else
			Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);
#endif

			if (!attr->attisdropped)
				targetAttrs = lappend_int(targetAttrs, attnum);
		}
	}
	else if (operation == CMD_UPDATE)
	{
#if PG_VERSION_NUM >= 90500
		Bitmapset  *tmpset = bms_copy(rte->updatedCols);
#else
		Bitmapset  *tmpset = bms_copy(rte->modifiedCols);
#endif
		AttrNumber	col;

		while ((col = bms_first_member(tmpset)) >= 0)
		{
			col += FirstLowInvalidHeapAttributeNumber;
			if (col <= InvalidAttrNumber)	/* Shouldn't happen */
				elog(ERROR, "system-column update is not supported");

			/*
			 * We also disallow updates to the first column which happens to
			 * be the row identifier in MongoDb (_id)
			 */
			if (col == 1)		/* Shouldn't happen */
				elog(ERROR, "row identifier column update is not supported");

			targetAttrs = lappend_int(targetAttrs, col);
		}
		/* We also want the rowid column to be available for the update */
		targetAttrs = lcons_int(1, targetAttrs);
	}
	else
		targetAttrs = lcons_int(1, targetAttrs);

	/*
	 * RETURNING list not supported
	 */
	if (plan->returningLists)
		elog(ERROR, "RETURNING is not supported by this FDW");

#if PG_VERSION_NUM < 130000
	heap_close(rel, NoLock);
#else
	table_close(rel, NoLock);
#endif

	return list_make1(targetAttrs);
}

/*
 * MongoBeginForeignModify
 *		Begin an insert/update/delete operation on a foreign table.
 */
static void
MongoBeginForeignModify(ModifyTableState *mtstate,
						ResultRelInfo *resultRelInfo,
						List *fdw_private,
						int subplan_index,
						int eflags)
{
	MongoFdwModifyState *fmstate;
	Relation	rel = resultRelInfo->ri_RelationDesc;
	AttrNumber	n_params;
	Oid			typefnoid = InvalidOid;
	bool		isvarlena = false;
	ListCell   *lc;
	Oid			foreignTableId;
	Oid			userid;
	ForeignServer *server;
	UserMapping *user;
	ForeignTable *table;

	/*
	 * Do nothing in EXPLAIN (no ANALYZE) case.  resultRelInfo->ri_FdwState
	 * stays NULL.
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	foreignTableId = RelationGetRelid(rel);
	userid = GetUserId();

	/* Get info about foreign table. */
	table = GetForeignTable(foreignTableId);
	server = GetForeignServer(table->serverid);
	user = GetUserMapping(userid, server->serverid);

	/* Begin constructing MongoFdwModifyState. */
	fmstate = (MongoFdwModifyState *) palloc0(sizeof(MongoFdwModifyState));

	fmstate->rel = rel;
	fmstate->options = mongo_get_options(foreignTableId);

	/*
	 * Get connection to the foreign server.  Connection manager will
	 * establish new connection if necessary.
	 */
	fmstate->mongoConnection = mongo_get_connection(server, user,
													fmstate->options);

	fmstate->target_attrs = (List *) list_nth(fdw_private, 0);

	n_params = list_length(fmstate->target_attrs) + 1;
	fmstate->p_flinfo = (FmgrInfo *) palloc(sizeof(FmgrInfo) * n_params);
	fmstate->p_nums = 0;

	if (mtstate->operation == CMD_UPDATE)
	{
		Form_pg_attribute attr;
#if PG_VERSION_NUM >= 140000
		Plan	   *subplan = outerPlanState(mtstate)->plan;
#else
		Plan	   *subplan = mtstate->mt_plans[subplan_index]->plan;
#endif

		Assert(subplan != NULL);

		attr = TupleDescAttr(RelationGetDescr(rel), 0);

		/* Find the rowid resjunk column in the subplan's result */
		fmstate->rowidAttno = ExecFindJunkAttributeInTlist(subplan->targetlist,
														   NameStr(attr->attname));
		if (!AttributeNumberIsValid(fmstate->rowidAttno))
			elog(ERROR, "could not find junk row identifier column");
	}

	/* Set up for remaining transmittable parameters */
	foreach(lc, fmstate->target_attrs)
	{
		int			attnum = lfirst_int(lc);
#if PG_VERSION_NUM < 110000
		Form_pg_attribute attr = RelationGetDescr(rel)->attrs[attnum - 1];
#else
		Form_pg_attribute attr = TupleDescAttr(RelationGetDescr(rel),
											   attnum - 1);
#endif

		Assert(!attr->attisdropped);

		getTypeOutputInfo(attr->atttypid, &typefnoid, &isvarlena);
		fmgr_info(typefnoid, &fmstate->p_flinfo[fmstate->p_nums]);
		fmstate->p_nums++;
	}
	Assert(fmstate->p_nums <= n_params);

	resultRelInfo->ri_FdwState = fmstate;
}

/*
 * MongoExecForeignInsert
 *		Insert one row into a foreign table.
 */
static TupleTableSlot *
MongoExecForeignInsert(EState *estate,
					   ResultRelInfo *resultRelInfo,
					   TupleTableSlot *slot,
					   TupleTableSlot *planSlot)
{
	BSON	   *bsonDoc;
	Oid			typoid;
	Datum		value;
	bool		isnull = false;
	MongoFdwModifyState *fmstate;

	fmstate = (MongoFdwModifyState *) resultRelInfo->ri_FdwState;

	bsonDoc = BsonCreate();

	typoid = get_atttype(RelationGetRelid(resultRelInfo->ri_RelationDesc), 1);

	/* Get following parameters from slot */
	if (slot != NULL && fmstate->target_attrs != NIL)
	{
		ListCell   *lc;

		foreach(lc, fmstate->target_attrs)
		{
			int			attnum = lfirst_int(lc);

			value = slot_getattr(slot, attnum, &isnull);

			/* First column of MongoDB's foreign table must be _id */
#if PG_VERSION_NUM < 110000
			if (strcmp(slot->tts_tupleDescriptor->attrs[0]->attname.data, "_id") != 0)
#else
			if (strcmp(TupleDescAttr(slot->tts_tupleDescriptor, 0)->attname.data, "_id") != 0)
#endif
				elog(ERROR, "first column of MongoDB's foreign table must be \"_id\"");

			if (typoid != NAMEOID)
				elog(ERROR, "type of first column of MongoDB's foreign table must be \"NAME\"");
#if PG_VERSION_NUM < 110000
			if (strcmp(slot->tts_tupleDescriptor->attrs[0]->attname.data, "__doc") == 0)
#else
			if (strcmp(TupleDescAttr(slot->tts_tupleDescriptor, 0)->attname.data, "__doc") == 0)
#endif
				continue;

			/*
			 * Ignore the value of first column which is row identifier in
			 * MongoDb (_id) and let MongoDB to insert the unique value for
			 * that column.
			 */
			if (attnum == 1)
				continue;

#if PG_VERSION_NUM < 110000
			AppendMongoValue(bsonDoc,
							 slot->tts_tupleDescriptor->attrs[attnum - 1]->attname.data,
							 value,
							 isnull,
							 slot->tts_tupleDescriptor->attrs[attnum - 1]->atttypid);
#else
			AppendMongoValue(bsonDoc,
							 TupleDescAttr(slot->tts_tupleDescriptor, attnum - 1)->attname.data,
							 value,
							 isnull,
							 TupleDescAttr(slot->tts_tupleDescriptor, attnum - 1)->atttypid);
#endif
		}
	}
	BsonFinish(bsonDoc);

	/* Now we are ready to insert tuple/document into MongoDB */
	MongoInsert(fmstate->mongoConnection, fmstate->options->svr_database,
				fmstate->options->collectionName, bsonDoc);

	BsonDestroy(bsonDoc);

	return slot;
}

/*
 * MongoAddForeignUpdateTargets
 *		Add column(s) needed for update/delete on a foreign table, we are using
 *		first column as row identification column, so we are adding that into
 *		target list.
 */
#if PG_VERSION_NUM >= 140000
static void
MongoAddForeignUpdateTargets(PlannerInfo *root,
							 Index rtindex,
							 RangeTblEntry *target_rte,
							 Relation target_relation)
#else
static void
MongoAddForeignUpdateTargets(Query *parsetree,
							 RangeTblEntry *target_rte,
							 Relation target_relation)
#endif
{
	Var		   *var;
	const char *attrname;
#if PG_VERSION_NUM < 140000
	TargetEntry *tle;
#endif

	/*
	 * What we need is the rowid which is the first column
	 */
#if PG_VERSION_NUM < 110000
	Form_pg_attribute attr = RelationGetDescr(target_relation)->attrs[0];
#else
	Form_pg_attribute attr = TupleDescAttr(RelationGetDescr(target_relation),
										   0);
#endif

	/* Make a Var representing the desired value */
#if PG_VERSION_NUM >= 140000
	var = makeVar(rtindex,
#else
	var = makeVar(parsetree->resultRelation,
#endif
				  1,
				  attr->atttypid,
				  attr->atttypmod,
				  InvalidOid,
				  0);

	/* Get name of the row identifier column */
	attrname = NameStr(attr->attname);

#if PG_VERSION_NUM >= 140000
	/* Register it as a row-identity column needed by this target rel */
	add_row_identity_var(root, var, rtindex, attrname);
#else
	/* Wrap it in a TLE with the right name ... */
	tle = makeTargetEntry((Expr *) var,
						  list_length(parsetree->targetList) + 1,
						  pstrdup(attrname),
						  true);

	/* ... And add it to the query's targetlist */
	parsetree->targetList = lappend(parsetree->targetList, tle);
#endif
}

static TupleTableSlot *
MongoExecForeignUpdate(EState *estate,
					   ResultRelInfo *resultRelInfo,
					   TupleTableSlot *slot,
					   TupleTableSlot *planSlot)
{
	Datum		datum;
	bool		isNull = false;
	Oid			foreignTableId;
	char	   *columnName;
	Oid			typoid;
	BSON	   *document;
	BSON	   *op = NULL;
	BSON		set;
	MongoFdwModifyState *fmstate;

	fmstate = (MongoFdwModifyState *) resultRelInfo->ri_FdwState;
	foreignTableId = RelationGetRelid(resultRelInfo->ri_RelationDesc);

	/* Get the id that was passed up as a resjunk column */
	datum = ExecGetJunkAttribute(planSlot, fmstate->rowidAttno, &isNull);

#if PG_VERSION_NUM < 110000
	columnName = get_relid_attribute_name(foreignTableId, 1);
#else
	columnName = get_attname(foreignTableId, 1, false);
#endif

	typoid = get_atttype(foreignTableId, 1);

	document = BsonCreate();
	BsonAppendStartObject(document, "$set", &set);

	/* Get following parameters from slot */
	if (slot != NULL && fmstate->target_attrs != NIL)
	{
		ListCell   *lc;

		foreach(lc, fmstate->target_attrs)
		{
			int			attnum = lfirst_int(lc);
#if PG_VERSION_NUM < 110000
			Form_pg_attribute attr = slot->tts_tupleDescriptor->attrs[attnum - 1];
#else
			Form_pg_attribute attr = TupleDescAttr(slot->tts_tupleDescriptor,
												   attnum - 1);
#endif
			Datum		value;
			bool		isnull;

			if (strcmp("_id", attr->attname.data) == 0)
				continue;

			if (strcmp("__doc", attr->attname.data) == 0)
				elog(ERROR, "system column '__doc' update is not supported");

			value = slot_getattr(slot, attnum, &isnull);
#ifdef META_DRIVER
			AppendMongoValue(&set, attr->attname.data, value,
							 isnull ? true : false, attr->atttypid);
#else
			AppendMongoValue(document, attr->attname.data, value,
							 isnull ? true : false, attr->atttypid);
#endif
		}
	}
	BsonAppendFinishObject(document, &set);
	BsonFinish(document);

	op = BsonCreate();
	if (!AppendMongoValue(op, columnName, datum, false, typoid))
	{
		BsonDestroy(document);
		return NULL;
	}
	BsonFinish(op);

	/* We are ready to update the row into MongoDB */
	MongoUpdate(fmstate->mongoConnection, fmstate->options->svr_database,
				fmstate->options->collectionName, op, document);

	BsonDestroy(op);
	BsonDestroy(document);

	/* Return NULL if nothing was updated on the remote end */
	return slot;
}

/*
 * MongoExecForeignDelete
 *		Delete one row from a foreign table
 */
static TupleTableSlot *
MongoExecForeignDelete(EState *estate,
					   ResultRelInfo *resultRelInfo,
					   TupleTableSlot *slot,
					   TupleTableSlot *planSlot)
{
	Datum		datum;
	bool		isNull = false;
	Oid			foreignTableId;
	char	   *columnName = NULL;
	Oid			typoid;
	BSON	   *document;
	MongoFdwModifyState *fmstate;

	fmstate = (MongoFdwModifyState *) resultRelInfo->ri_FdwState;

	foreignTableId = RelationGetRelid(resultRelInfo->ri_RelationDesc);

	/* Get the id that was passed up as a resjunk column */
	datum = ExecGetJunkAttribute(planSlot, 1, &isNull);

#if PG_VERSION_NUM < 110000
	columnName = get_relid_attribute_name(foreignTableId, 1);
#else
	columnName = get_attname(foreignTableId, 1, false);
#endif

	typoid = get_atttype(foreignTableId, 1);

	document = BsonCreate();
	if (!AppendMongoValue(document, columnName, datum, false, typoid))
	{
		BsonDestroy(document);
		return NULL;
	}
	BsonFinish(document);

	/* Now we are ready to delete a single document from MongoDB */
	MongoDelete(fmstate->mongoConnection, fmstate->options->svr_database,
				fmstate->options->collectionName, document);

	BsonDestroy(document);

	/* Return NULL if nothing was updated on the remote end */
	return slot;
}

/*
 * MongoEndForeignModify
 *		Finish an insert/update/delete operation on a foreign table
 */
static void
MongoEndForeignModify(EState *estate, ResultRelInfo *resultRelInfo)
{
	MongoFdwModifyState *fmstate;

	fmstate = (MongoFdwModifyState *) resultRelInfo->ri_FdwState;
	if (fmstate)
	{
		if (fmstate->options)
		{
			mongo_free_options(fmstate->options);
			fmstate->options = NULL;
		}
		MongoFreeModifyState(fmstate);
		pfree(fmstate);
	}
}

/*
 * ForeignTableDocumentCount
 * 		Connects to the MongoDB server, and queries it for the number of
 * 		documents in the foreign collection. On success, the function returns
 * 		the document count.  On failure, the function returns -1.0.
 */
static double
ForeignTableDocumentCount(Oid foreignTableId)
{
	MongoFdwOptions *options;
	MONGO_CONN *mongoConnection;
	const BSON *emptyQuery = NULL;
	double 		documentCount;
	Oid			userid = GetUserId();
	ForeignServer *server;
	UserMapping *user;
	ForeignTable *table;

	/* Get info about foreign table. */
	table = GetForeignTable(foreignTableId);
	server = GetForeignServer(table->serverid);
	user = GetUserMapping(userid, server->serverid);

	/* Resolve foreign table options; and connect to mongo server */
	options = mongo_get_options(foreignTableId);

	/*
	 * Get connection to the foreign server.  Connection manager will
	 * establish new connection if necessary.
	 */
	mongoConnection = mongo_get_connection(server, user, options);

	documentCount = MongoAggregateCount(mongoConnection, options->svr_database,
										options->collectionName, emptyQuery);

	mongo_free_options(options);

	return documentCount;
}

/*
 * FillTupleSlot
 *		Walks over all key/value pairs in the given document.
 *
 * For each pair, the function checks if the key appears in the column mapping
 * hash, and if the value type is compatible with the one specified for the
 * column.  If so, the function converts the value and fills the corresponding
 * tuple position.  The bsonDocumentKey parameter is used for recursion, and
 * should always be passed as NULL.
 */
static void
FillTupleSlot(const BSON *bsonDocument,
			  const char *bsonDocumentKey,
			  MongoPlanerInfo *plannerInfo,
			  TupleDesc tupleDescriptor,
			  Datum *columnValues,
			  bool *columnNulls,
			  bool is_agg)
{
	if (is_agg)
	{
		/* Fill tuple slot for aggregation query */
		FillTupleSlotAgg(bsonDocument, bsonDocumentKey, plannerInfo,
						 tupleDescriptor, columnValues, columnNulls);
	}
	else
		FillTupleSlotAttr(bsonDocument, bsonDocumentKey, plannerInfo,
						  tupleDescriptor, columnValues, columnNulls);
}

/*
 * Fill Tuple Slot for aggregation.
 */
static void
FillTupleSlotAgg(const BSON *bsonDocument,
					const char *bsonDocumentKey,
					MongoPlanerInfo *plannerInfo,
					TupleDesc tupleDescriptor,
					Datum *columnValues,
					bool *columnNulls)
{
	BSON_ITERATOR bsonIterator = {NULL, 0};
	ListCell	*lc;

	if (BsonIterInit(&bsonIterator, (BSON *) bsonDocument) == false)
		elog(ERROR, "failed to initialize BSON iterator");

	while (BsonIterNext(&bsonIterator))
	{
		const char *bsonKey = BsonIterKey(&bsonIterator);
		BSON_TYPE	bsonType = BsonIterType(&bsonIterator);
		Oid			pgTypeId = InvalidOid;
		Oid			pgArrayTypeId = InvalidOid;
		int32		pgTypeMod;
		bool		compatibleTypes = false;
		bool		handleFound = false;
		const char *bsonFullKey;
		int32		targetIndex;

		bsonFullKey = bsonKey;

		/* Look up the corresponding target for this bson key */
		foreach(lc, plannerInfo->retrieved_attrs)
		{
			int			attnum = lfirst_int(lc) - 1;
			char	   *ref_target = psprintf("ref%d", attnum);

			if (strcmp(bsonFullKey, ref_target) == 0)
			{
				pgTypeId = TupleDescAttr(tupleDescriptor, attnum)->atttypid;
				pgArrayTypeId = get_element_type(pgTypeId);
				pgTypeMod = TupleDescAttr(tupleDescriptor, attnum)->atttypmod;
				targetIndex = attnum;
				handleFound = true;
				break;
			}
		}

		/* Recurse into nested objects */
		if (bsonType == BSON_TYPE_DOCUMENT)
		{
			char *innerel_name = NULL;

			foreach(lc, plannerInfo->joininfo_list)
			{
				MongoPlanerJoinInfo *join_info = (MongoPlanerJoinInfo *)lfirst(lc);

				if (join_info->innerel_name != NULL &&
					strcmp(bsonFullKey, join_info->innerel_name) == 0)
				{
					innerel_name = join_info->innerel_name;
					break;
				}
			}

			if (innerel_name)
			{
				BSON		subObject;

				BsonIterSubObject(&bsonIterator, &subObject);
				FillTupleSlotAgg(&subObject,
								  bsonFullKey,
								  plannerInfo,
								  tupleDescriptor,
								  columnValues,
								  columnNulls);
				continue;
			}
		}

		/* If no corresponding target or null BSON value, continue */
		if (handleFound == false || bsonType == BSON_TYPE_NULL)
			continue;

		/* Check if target have compatible types */
		if (OidIsValid(pgArrayTypeId) && bsonType == BSON_TYPE_ARRAY)
			compatibleTypes = true;
		else
			compatibleTypes = ColumnTypesCompatible(bsonType, pgTypeId);

		/* If types are incompatible, leave this target null */
		if (!compatibleTypes)
			continue;

		/* Fill in corresponding target value and null flag */
		if (OidIsValid(pgArrayTypeId))
			columnValues[targetIndex] = ColumnValueArray(&bsonIterator,
														 pgArrayTypeId);
		else
			columnValues[targetIndex] = ColumnValue(&bsonIterator,
													pgTypeId,
													pgTypeMod);
		columnNulls[targetIndex] = false;
	}
}

/*
 * Fill Tuple Slot for attributes.
 */
static void
FillTupleSlotAttr(const BSON *bsonDocument,
					const char *bsonDocumentKey,
					MongoPlanerInfo *plannerInfo,
					TupleDesc tupleDescriptor,
					Datum *columnValues,
					bool *columnNulls)
{
	BSON_ITERATOR bsonIterator = {NULL, 0};
	ListCell	*lc;

	if (BsonIterInit(&bsonIterator, (BSON *) bsonDocument) == false)
		elog(ERROR, "failed to initialize BSON iterator");

	foreach(lc, plannerInfo->retrieved_attrs)
	{
		int			attnum = lfirst_int(lc) - 1;
		Oid			pgtype = TupleDescAttr(tupleDescriptor, attnum)->atttypid;
		char	   *colname = get_attname(plannerInfo->rel_oid, attnum + 1, false);

		if (strcmp(colname, "__doc") == 0)
		{
			JsonLexContext *lex;
			text	   *result;
			Datum		columnValue;
			char	   *str;

			str = BsonAsJson(bsonDocument);
			result = cstring_to_text_with_len(str, strlen(str));
			lex = makeJsonLexContext(result, false);
			pg_parse_json(lex, &nullSemAction);
			columnValue = PointerGetDatum(result);

			switch (pgtype)
			{
				case BOOLOID:
				case INT2OID:
				case INT4OID:
				case INT8OID:
				case BOXOID:
				case BYTEAOID:
				case CHAROID:
				case VARCHAROID:
				case NAMEOID:
				case JSONOID:
				case XMLOID:
				case POINTOID:
				case LSEGOID:
				case LINEOID:
				case UUIDOID:
				case LSNOID:
				case TEXTOID:
				case CASHOID:
				case DATEOID:
				case MACADDROID:
				case TIMESTAMPOID:
				case TIMESTAMPTZOID:
				case BPCHAROID:
					columnValue = PointerGetDatum(result);
					break;
				case JSONBOID:
					columnValue = DirectFunctionCall1(jsonb_in,
													PointerGetDatum(str));
					break;
				default:
					ereport(ERROR,
							(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
							errmsg("unsupported type for column __doc"),
							errhint("Column type: %u",
									(uint32) pgtype)));
					break;
			}

			columnValues[attnum] = columnValue;
			columnNulls[attnum] = false;

			return;
		}
	}

	while (BsonIterNext(&bsonIterator))
	{
		const char *bsonKey = BsonIterKey(&bsonIterator);
		BSON_TYPE	bsonType = BsonIterType(&bsonIterator);
		Oid			columnTypeId = InvalidOid;
		Oid			columnArrayTypeId = InvalidOid;
		int32		columnTypeMod;
		bool		compatibleTypes = false;
		bool		handleFound = false;
		const char *bsonFullKey;
		int32		columnIndex;

		if (bsonDocumentKey != NULL)
		{
			/*
			 * For fields in nested BSON objects, we use fully qualified field
			 * name to check the column mapping.
			 */
			StringInfo	bsonFullKeyString = makeStringInfo();

			appendStringInfo(bsonFullKeyString, "%s.%s", bsonDocumentKey,
							 bsonKey);
			bsonFullKey = bsonFullKeyString->data;
		}
		else
			bsonFullKey = bsonKey;

		/* Look up the corresponding column for this bson key */
		foreach(lc, plannerInfo->retrieved_attrs)
		{
			int			attnum = lfirst_int(lc) - 1;
			char	   *colname = get_attname(plannerInfo->rel_oid, attnum + 1, false);

			if (strcmp(bsonFullKey, colname) == 0)
			{
				columnTypeId = TupleDescAttr(tupleDescriptor, attnum)->atttypid;
				columnArrayTypeId = get_element_type(columnTypeId);
				columnTypeMod = TupleDescAttr(tupleDescriptor, attnum)->atttypmod;
				columnIndex = attnum;
				handleFound = true;
				break;
			}
		}

		/* Recurse into nested objects */
		if (bsonType == BSON_TYPE_DOCUMENT)
		{
			if (columnTypeId != JSONOID && columnTypeId != JSONBOID)
			{
				BSON		subObject;

				BsonIterSubObject(&bsonIterator, &subObject);
				FillTupleSlotAttr(&subObject,
								  bsonFullKey,
								  plannerInfo,
								  tupleDescriptor,
								  columnValues,
								  columnNulls);
				continue;
			}
		}

		/* If no corresponding column or null BSON value, continue */
		if (handleFound == false || bsonType == BSON_TYPE_NULL)
			continue;

		/* Check if columns have compatible types */
		if (OidIsValid(columnArrayTypeId) && bsonType == BSON_TYPE_ARRAY)
			compatibleTypes = true;
		else
			compatibleTypes = ColumnTypesCompatible(bsonType, columnTypeId);

		/* If types are incompatible, leave this column null */
		if (!compatibleTypes)
			continue;

		/* Fill in corresponding column value and null flag */
		if (OidIsValid(columnArrayTypeId))
			columnValues[columnIndex] = ColumnValueArray(&bsonIterator,
														 columnArrayTypeId);
		else
			columnValues[columnIndex] = ColumnValue(&bsonIterator,
													columnTypeId,
													columnTypeMod);
		columnNulls[columnIndex] = false;
	}
}

/*
 * ColumnTypesCompatible
 * 		Checks if the given BSON type can be converted to the given PostgreSQL
 * 		type.
 *
 * In this check, the function also uses its knowledge of internal conversions
 * applied by BSON APIs.
 */
static bool
ColumnTypesCompatible(BSON_TYPE bsonType, Oid columnTypeId)
{
	bool		compatibleTypes = false;

	/* We consider the PostgreSQL column type as authoritative */
	switch (columnTypeId)
	{
		case INT2OID:
		case INT4OID:
		case INT8OID:
		case FLOAT4OID:
		case FLOAT8OID:
		case NUMERICOID:
			if (bsonType == BSON_TYPE_INT32 || bsonType == BSON_TYPE_INT64 ||
				bsonType == BSON_TYPE_DOUBLE)
				compatibleTypes = true;
			break;
		case BOOLOID:
			if (bsonType == BSON_TYPE_INT32 || bsonType == BSON_TYPE_INT64 ||
				bsonType == BSON_TYPE_DOUBLE || bsonType == BSON_TYPE_BOOL)
				compatibleTypes = true;
			break;
		case BPCHAROID:
		case VARCHAROID:
		case TEXTOID:
			if (bsonType == BSON_TYPE_UTF8)
				compatibleTypes = true;
			break;
		case BYTEAOID:
			if (bsonType == BSON_TYPE_BINDATA)
				compatibleTypes = true;
#ifdef META_DRIVER
			if (bsonType == BSON_TYPE_OID)
				compatibleTypes = true;
#endif
			break;
		case NAMEOID:

			/*
			 * We currently overload the NAMEOID type to represent the BSON
			 * object identifier.  We can safely overload this 64-byte data
			 * type since it's reserved for internal use in PostgreSQL.
			 */
			if (bsonType == BSON_TYPE_OID ||
				bsonType == BSON_TYPE_UTF8)
				compatibleTypes = true;
			break;
		case DATEOID:
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			if (bsonType == BSON_TYPE_DATE_TIME)
				compatibleTypes = true;
			break;
		case NUMERICARRAY_OID:
			if (bsonType == BSON_TYPE_ARRAY)
				compatibleTypes = true;
			break;
		case JSONBOID:
		case JSONOID:
				compatibleTypes = true;
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
					 errmsg("cannot convert BSON type to column type"),
					 errhint("Column type: %u", (uint32) columnTypeId)));
			break;
	}

	return compatibleTypes;
}

/*
 * ColumnValueArray
 * 		Uses array element type id to read the current array pointed to by the
 * 		BSON iterator, and converts each array element (with matching type) to
 * 		the corresponding PostgreSQL datum.
 *
 * Then, the function constructs an array datum from element datums, and
 * returns the array datum.
 */
static Datum
ColumnValueArray(BSON_ITERATOR *bsonIterator, Oid valueTypeId)
{
	Datum	   *columnValueArray = palloc(INITIAL_ARRAY_CAPACITY * sizeof(Datum));
	uint32		arrayCapacity = INITIAL_ARRAY_CAPACITY;
	uint32		arrayIndex = 0;
	ArrayType  *columnValueObject;
	Datum		columnValueDatum;
	bool		typeByValue;
	char		typeAlignment;
	int16		typeLength;

	BSON_ITERATOR bsonSubIterator = {NULL, 0};

	BsonIterSubIter(bsonIterator, &bsonSubIterator);
	while (BsonIterNext(&bsonSubIterator))
	{
		BSON_TYPE	bsonType = BsonIterType(&bsonSubIterator);
		bool		compatibleTypes = false;

		compatibleTypes = ColumnTypesCompatible(bsonType, valueTypeId);
		if (bsonType == BSON_TYPE_NULL || !compatibleTypes)
			continue;

		if (arrayIndex >= arrayCapacity)
		{
			/* Double the array capacity. */
			arrayCapacity *= 2;
			columnValueArray = repalloc(columnValueArray,
										arrayCapacity * sizeof(Datum));
		}

		/* Use default type modifier (0) to convert column value */
		columnValueArray[arrayIndex] = ColumnValue(&bsonSubIterator,
												   valueTypeId, 0);
		arrayIndex++;
	}

	get_typlenbyvalalign(valueTypeId, &typeLength, &typeByValue,
						 &typeAlignment);
	columnValueObject = construct_array(columnValueArray,
										arrayIndex,
										valueTypeId,
										typeLength,
										typeByValue,
										typeAlignment);

	columnValueDatum = PointerGetDatum(columnValueObject);

	pfree(columnValueArray);

	return columnValueDatum;
}

/*
 * ColumnValue
 * 		Uses column type information to read the current value pointed to by
 * 		the BSON iterator, and converts this value to the corresponding
 * 		PostgreSQL datum.  The function then returns this datum.
 */
static Datum
ColumnValue(BSON_ITERATOR *bsonIterator, Oid columnTypeId, int32 columnTypeMod)
{
	BSON_TYPE	bsonType = BsonIterType(bsonIterator);
	Datum		columnValue;

	switch (columnTypeId)
	{
		case INT2OID:
			{
				int16		value;

				MONGO_BSON_CONVERSION_DATA_TYPE(bsonIterator, columnTypeId, bsonType, value, int16);

				columnValue = Int16GetDatum(value);
			}
			break;
		case INT4OID:
			{
				int32		value;

				MONGO_BSON_CONVERSION_DATA_TYPE(bsonIterator, columnTypeId, bsonType, value, int32);

				columnValue = Int32GetDatum(value);
			}
			break;
		case INT8OID:
			{
				int64		value;

				MONGO_BSON_CONVERSION_DATA_TYPE(bsonIterator, columnTypeId, bsonType, value, int64);

				columnValue = Int64GetDatum(value);
			}
			break;
		case FLOAT4OID:
			{
				float4		value;

				MONGO_BSON_CONVERSION_DATA_TYPE(bsonIterator, columnTypeId, bsonType, value, float4);

				columnValue = Float4GetDatum(value);
			}
			break;
		case FLOAT8OID:
			{
				float8		value;

				MONGO_BSON_CONVERSION_DATA_TYPE(bsonIterator, columnTypeId, bsonType, value, float8);

				columnValue = Float8GetDatum(value);
			}
			break;
		case NUMERICOID:
			{
				float8		value;
				Datum		valueDatum;

				MONGO_BSON_CONVERSION_DATA_TYPE(bsonIterator, columnTypeId, bsonType, value, float8);

				valueDatum = Float8GetDatum(value);

				/* Overlook type modifiers for numeric */
				columnValue = DirectFunctionCall1(float8_numeric, valueDatum);
			}
			break;
		case BOOLOID:
			{
				bool		value;

				MONGO_BSON_CONVERSION_DATA_TYPE(bsonIterator, columnTypeId, bsonType, value, bool);

				columnValue = BoolGetDatum(value);
			}
			break;
		case BPCHAROID:
			{
				const char *value = BsonIterString(bsonIterator);
				Datum		valueDatum = CStringGetDatum(value);

				columnValue = DirectFunctionCall3(bpcharin, valueDatum,
												  ObjectIdGetDatum(InvalidOid),
												  Int32GetDatum(columnTypeMod));
			}
			break;
		case VARCHAROID:
			{
				const char *value = BsonIterString(bsonIterator);
				Datum		valueDatum = CStringGetDatum(value);

				columnValue = DirectFunctionCall3(varcharin, valueDatum,
												  ObjectIdGetDatum(InvalidOid),
												  Int32GetDatum(columnTypeMod));
			}
			break;
		case TEXTOID:
			{
				const char *value = BsonIterString(bsonIterator);

				columnValue = CStringGetTextDatum(value);
			}
			break;
		case NAMEOID:
			{
				const char *name_val;
				Datum		valueDatum = 0;

				switch (bsonType)
				{
					case BSON_TYPE_OID:
						{
							char		value[NAMEDATALEN];
							bson_oid_t *bsonObjectId = (bson_oid_t *) BsonIterOid(bsonIterator);

							bson_oid_to_string(bsonObjectId, value);
							name_val = value;
						}
						break;
					case BSON_TYPE_UTF8:
						{
							name_val = BsonIterString(bsonIterator);
						}
						break;
					default:
						ereport(ERROR,
							(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
							errmsg("cannot convert BSON type to column type"),
							errhint("Column type: %u", (uint32) columnTypeId)));
				}
				valueDatum = CStringGetDatum(name_val);
				columnValue = DirectFunctionCall3(namein, valueDatum,
												  ObjectIdGetDatum(InvalidOid),
												  Int32GetDatum(columnTypeMod));
			}
			break;
		case BYTEAOID:
			{
				int			value_len;
				char	   *value;
				bytea	   *result;
#ifdef META_DRIVER
				switch (BsonIterType(bsonIterator))
				{
					case BSON_TYPE_OID:
						value = (char *) BsonIterOid(bsonIterator);
						value_len = 12;
						break;
					default:
						value = (char *) BsonIterBinData(bsonIterator,
														 (uint32_t *) &value_len);
						break;
				}
#else
				value_len = BsonIterBinLen(bsonIterator);
				value = (char *) BsonIterBinData(bsonIterator);
#endif
				result = (bytea *) palloc(value_len + VARHDRSZ);
				memcpy(VARDATA(result), value, value_len);
				SET_VARSIZE(result, value_len + VARHDRSZ);
				columnValue = PointerGetDatum(result);
			}
			break;
		case DATEOID:
			{
				int64		valueMillis = BsonIterDate(bsonIterator);
				int64		timestamp = (valueMillis * 1000L) - POSTGRES_TO_UNIX_EPOCH_USECS;
				Datum		timestampDatum = TimestampGetDatum(timestamp);

				columnValue = DirectFunctionCall1(timestamp_date,
												  timestampDatum);
			}
			break;
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			{
				int64		valueMillis = BsonIterDate(bsonIterator);
				int64		timestamp = (valueMillis * 1000L) - POSTGRES_TO_UNIX_EPOCH_USECS;

				/* Overlook type modifiers for timestamp */
				columnValue = TimestampGetDatum(timestamp);
			}
			break;
		case JSONBOID:
		case JSONOID:
			{
				JsonLexContext *lex;
				text	   *result;
				StringInfo	buffer = makeStringInfo();

				BSON_TYPE	type = BSON_ITER_TYPE(bsonIterator);

				mongo_BsonToStringValue(buffer, bsonIterator, type);

				if (columnTypeId == JSONOID)
				{
					result = cstring_to_text_with_len(buffer->data, buffer->len);
					lex = makeJsonLexContext(result, false);
					pg_parse_json(lex, &nullSemAction);
					columnValue = PointerGetDatum(result);
				}
				else if (columnTypeId == JSONBOID)
					columnValue = DirectFunctionCall1(jsonb_in, PointerGetDatum(buffer->data));
			}
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
					 errmsg("cannot convert BSON type to column type"),
					 errhint("Column type: %u", (uint32) columnTypeId)));
			break;
	}

	return columnValue;
}

void
BsonToJsonString(StringInfo output, BSON_ITERATOR i, bool isArray)
{
	const char *key;
	bool		isFirstElement;
	char		beginSymbol = '{';
	char		endSymbol = '}';
	BSON_TYPE	bsonType;

	if (isArray)
	{
		beginSymbol = '[';
		endSymbol = ']';
	}

#ifndef META_DRIVER
	{
		const char *bsonData = bson_iterator_value(&i);

		bson_iterator_from_buffer(&i, bsonData);
	}
#endif

	appendStringInfoChar(output, beginSymbol);

	isFirstElement = true;
	while (BsonIterNext(&i))
	{
		if (!isFirstElement)
			appendStringInfoChar(output, ',');

		bsonType = BsonIterType(&i);
		if (bsonType == 0)
			break;

		key = BsonIterKey(&i);

		if (!isArray)
			appendStringInfo(output, "\"%s\":", key);

		switch (bsonType)
		{
			case BSON_TYPE_DOUBLE:
				appendStringInfo(output, "%f", BsonIterDouble(&i));
				break;
			case BSON_TYPE_UTF8:
				appendStringInfo(output, "\"%s\"",
								 EscapeJsonString(BsonIterString(&i)));
				break;
			case BSON_TYPE_SYMBOL:
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
						 errmsg("\"symbol\" BSON type is deprecated and unsupported"),
						 errhint("Symbol: %s", BsonIterString(&i))));
				break;
			case BSON_TYPE_OID:
				{
					char		oidhex[25];

					BsonOidToString(BsonIterOid(&i), oidhex);
					appendStringInfo(output, "{\"$oid\":\"%s\"}", oidhex);
					break;
				}
			case BSON_TYPE_BOOL:
				appendStringInfoString(output,
									   BsonIterBool(&i) ? "true" : "false");
				break;
			case BSON_TYPE_DATE_TIME:
				appendStringInfo(output, "{\"$date\":%ld}",
								 (long int) BsonIterDate(&i));
				break;
			case BSON_TYPE_BINDATA:
				/* It's possible to encode the data with base64 here. */
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
						 errmsg("support for \"binary data\" BSON type is not implemented")));
				break;
			case BSON_TYPE_UNDEFINED:
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
						 errmsg("\"undefined\" BSON type is deprecated and unsupported")));
				break;
			case BSON_TYPE_NULL:
				appendStringInfoString(output, "null");
				break;
			case BSON_TYPE_REGEX:
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
						 errmsg("support for \"regex\" BSON type is not implemented"),
						 errhint("Regex: %s", BsonIterRegex(&i))));
				break;
			case BSON_TYPE_CODE:
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
						 errmsg("support for \"code\" BSON type is not implemented"),
						 errhint("Code: %s", BsonIterCode(&i))));
				break;
			case BSON_TYPE_CODEWSCOPE:
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
						 errmsg("support for \"code\" with scope` BSON type is not implemented")));
				break;
			case BSON_TYPE_INT32:
				appendStringInfo(output, "%d", BsonIterInt32(&i));
				break;
			case BSON_TYPE_INT64:
				appendStringInfo(output, "%lu", (uint64_t) BsonIterInt64(&i));
				break;
			case BSON_TYPE_TIMESTAMP:
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
						 errmsg("internal `timestamp` BSON type is and unsupported")));
				break;
			case BSON_TYPE_DOCUMENT:
				BsonToJsonString(output, i, false);
				break;
			case BSON_TYPE_ARRAY:
				BsonToJsonString(output, i, true);
				break;
			default:
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
						 errmsg("unsupported BSON type: %x", bsonType)));
		}
		isFirstElement = false;
	}
	appendStringInfoChar(output, endSymbol);
}

/*
 * mongo_BsonToStringValue
 * 	Convert a BSON value into string format.
 */
static void
mongo_BsonToStringValue(StringInfo output, BSON_ITERATOR *bsIterator, BSON_TYPE bsonType)
{
	switch (bsonType)
	{
		case BSON_TYPE_DOUBLE:
			appendStringInfo(output, "%f", BsonIterDouble(bsIterator));
			break;
		case BSON_TYPE_UTF8:
			appendStringInfo(output, "\"%s\"",
								EscapeJsonString(BsonIterString(bsIterator)));
			break;
		case BSON_TYPE_SYMBOL:
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
						errmsg("\"symbol\" BSON type is deprecated and unsupported"),
						errhint("Symbol: %s", BsonIterString(bsIterator))));
			break;
		case BSON_TYPE_OID:
			{
				char		oidhex[25];

				BsonOidToString(BsonIterOid(bsIterator), oidhex);
				appendStringInfo(output, "{\"$oid\":\"%s\"}", oidhex);
				break;
			}
		case BSON_TYPE_BOOL:
			appendStringInfoString(output,
									BsonIterBool(bsIterator) ? "true" : "false");
			break;
		case BSON_TYPE_DATE_TIME:
			appendStringInfo(output, "{\"$date\":%ld}",
								(long int) BsonIterDate(bsIterator));
			break;
		case BSON_TYPE_BINDATA:
			/* It's possible to encode the data with base64 here. */
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
						errmsg("support for \"binary data\" BSON type is not implemented")));
			break;
		case BSON_TYPE_UNDEFINED:
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
						errmsg("\"undefined\" BSON type is deprecated and unsupported")));
			break;
		case BSON_TYPE_NULL:
			appendStringInfoString(output, "null");
			break;
		case BSON_TYPE_REGEX:
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
						errmsg("support for \"regex\" BSON type is not implemented"),
						errhint("Regex: %s", BsonIterRegex(bsIterator))));
			break;
		case BSON_TYPE_CODE:
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
						errmsg("support for \"code\" BSON type is not implemented"),
						errhint("Code: %s", BsonIterCode(bsIterator))));
			break;
		case BSON_TYPE_CODEWSCOPE:
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
						errmsg("support for \"code\" with scope` BSON type is not implemented")));
			break;
		case BSON_TYPE_INT32:
			appendStringInfo(output, "%d", BsonIterInt32(bsIterator));
			break;
		case BSON_TYPE_INT64:
			appendStringInfo(output, "%lu", (uint64_t) BsonIterInt64(bsIterator));
			break;
		case BSON_TYPE_TIMESTAMP:
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
						errmsg("internal `timestamp` BSON type is and unsupported")));
			break;
		case BSON_TYPE_DOCUMENT:
		case BSON_TYPE_ARRAY:
			{
#ifdef META_DRIVER
				/* Convert BSON to JSON value */
				BsonToJsonStringValue(output, bsIterator, BSON_TYPE_ARRAY == bsonType);
#else
				/* Convert BSON to JSON value */
				BsonToJsonString(output, *bsIterator, BSON_TYPE_ARRAY == bsonType);
#endif
			}
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
						errmsg("unsupported BSON type: %x", bsonType)));
	}
}

/*
 * EscapeJsonString
 *		Escapes a string for safe inclusion in JSON.
 */
const char *
EscapeJsonString(const char *string)
{
	StringInfo	buffer;
	const char *ptr;
	int			i;
	int			segmentStartIdx;
	int			len;
	bool		needsEscaping = false;

	for (ptr = string; *ptr; ++ptr)
	{
		if (*ptr == '"' || *ptr == '\r' || *ptr == '\n' || *ptr == '\t' ||
			*ptr == '\\')
		{
			needsEscaping = true;
			break;
		}
	}

	if (!needsEscaping)
		return string;

	buffer = makeStringInfo();
	len = strlen(string);
	segmentStartIdx = 0;
	for (i = 0; i < len; ++i)
	{
		if (string[i] == '"' || string[i] == '\r' || string[i] == '\n' ||
			string[i] == '\t' || string[i] == '\\')
		{
			if (segmentStartIdx < i)
				appendBinaryStringInfo(buffer, string + segmentStartIdx,
									   i - segmentStartIdx);

			appendStringInfoChar(buffer, '\\');
			if (string[i] == '"')
				appendStringInfoChar(buffer, '"');
			else if (string[i] == '\r')
				appendStringInfoChar(buffer, 'r');
			else if (string[i] == '\n')
				appendStringInfoChar(buffer, 'n');
			else if (string[i] == '\t')
				appendStringInfoChar(buffer, 't');
			else if (string[i] == '\\')
				appendStringInfoChar(buffer, '\\');

			segmentStartIdx = i + 1;
		}
	}
	if (segmentStartIdx < len)
		appendBinaryStringInfo(buffer, string + segmentStartIdx,
							   len - segmentStartIdx);
	return buffer->data;
}

/*
 * MongoFreeScanState
 *		Closes the cursor and connection to MongoDB, and reclaims all Mongo
 *		related resources allocated for the foreign scan.
 */
static void
MongoFreeScanState(MongoFdwScanState *fsstate)
{
	if (fsstate == NULL)
		return;

	if (fsstate->queryDocument)
	{
		BsonDestroy(fsstate->queryDocument);
		fsstate->queryDocument = NULL;
	}

	if (fsstate->mongoCursor)
	{
		MongoCursorDestroy(fsstate->mongoCursor);
		fsstate->mongoCursor = NULL;
	}

	/* Release remote connection */
	mongo_release_connection(fsstate->mongoConnection);
}

/*
 * MongoFreeModifyState
 *		Closes the cursor and connection to MongoDB, and reclaims all Mongo
 *		related resources allocated for the foreign scan.
 */
static void
MongoFreeModifyState(MongoFdwModifyState *fmstate)
{
	if (fmstate == NULL)
		return;

	if (fmstate->queryDocument)
	{
		BsonDestroy(fmstate->queryDocument);
		fmstate->queryDocument = NULL;
	}

	if (fmstate->mongoCursor)
	{
		MongoCursorDestroy(fmstate->mongoCursor);
		fmstate->mongoCursor = NULL;
	}

	/* Release remote connection */
	mongo_release_connection(fmstate->mongoConnection);
}

/*
 * MongoAnalyzeForeignTable
 *		Collects statistics for the given foreign table.
 */
static bool
MongoAnalyzeForeignTable(Relation relation,
						 AcquireSampleRowsFunc *func,
						 BlockNumber *totalpages)
{
	BlockNumber pageCount = 0;
	int			attributeCount;
	int32	   *attributeWidths;
	Oid			foreignTableId;
	int32		documentWidth;
	double 		documentCount;
	double		foreignTableSize;

	foreignTableId = RelationGetRelid(relation);
	documentCount = ForeignTableDocumentCount(foreignTableId);

	if (documentCount > 0.0)
	{
		attributeCount = RelationGetNumberOfAttributes(relation);
		attributeWidths = (int32 *) palloc0((attributeCount + 1) * sizeof(int32));

		/*
		 * We estimate disk costs assuming a sequential scan over the data.
		 * This is an inaccurate assumption as Mongo scatters the data over
		 * disk pages, and may rely on an index to retrieve the data.  Still,
		 * this should at least give us a relative cost.
		 */
		documentWidth = get_relation_data_width(foreignTableId,
												attributeWidths);
		foreignTableSize = documentCount * documentWidth;

		pageCount = (BlockNumber) rint(foreignTableSize / BLCKSZ);
	}
	else
		ereport(DEBUG1,
				(errmsg("could not retrieve document count for collection"),
				 errhint("Could not	collect statistics about foreign table.")));

	(*totalpages) = pageCount;
	(*func) = MongoAcquireSampleRows;

	return true;
}

/*
 * MongoAcquireSampleRows
 *		Acquires a random sample of rows from the foreign table.
 *
 * Selected rows are returned in the caller allocated sampleRows array,
 * which must have at least target row count entries.  The actual number of
 * rows selected is returned as the function result.  We also count the number
 * of rows in the collection and return it in total row count.  We also always
 * set dead row count to zero.
 *
 * Note that the returned list of rows is not always in order by physical
 * position in the MongoDB collection.  Therefore, correlation estimates
 * derived later may be meaningless, but it's OK because we don't use the
 * estimates currently (the planner only pays attention to correlation for
 * index scans).
 */
static int
MongoAcquireSampleRows(Relation relation,
					   int errorLevel,
					   HeapTuple *sampleRows,
					   int targetRowCount,
					   double *totalRowCount,
					   double *totalDeadRowCount)
{
	MONGO_CONN *mongoConnection;
	int			sampleRowCount = 0;
	double		rowCount = 0;
	double		rowCountToSkip = -1;	/* -1 means not set yet */
	double		randomState;
	Datum	   *columnValues;
	bool	   *columnNulls;
	Oid			foreignTableId;
	TupleDesc	tupleDescriptor;
	AttrNumber	columnCount;
	AttrNumber	columnId;
	MONGO_CURSOR *mongoCursor;
	BSON	   *queryDocument;
	List	   *columnList = NIL;
	char	   *relationName;
	MemoryContext oldContext = CurrentMemoryContext;
	MemoryContext tupleContext;
	MongoFdwOptions *options;
	ForeignServer *server;
	UserMapping *user;
	ForeignTable *table;
	MongoPlanerInfo *plannerInfo;

	/* Create list of columns in the relation */
	tupleDescriptor = RelationGetDescr(relation);
	columnCount = tupleDescriptor->natts;

	for (columnId = 1; columnId <= columnCount; columnId++)
	{
		Var		   *column = makeNode(Var);
#if PG_VERSION_NUM >= 110000
		Form_pg_attribute attr = TupleDescAttr(tupleDescriptor, columnId - 1);

		column->varattno = columnId;
		column->vartype = attr->atttypid;
		column->vartypmod = attr->atttypmod;
#else
		/* Only assign required fields for column mapping hash */
		column->varattno = columnId;
		column->vartype = tupleDescriptor->attrs[columnId - 1]->atttypid;
		column->vartypmod = tupleDescriptor->attrs[columnId - 1]->atttypmod;
#endif

		columnList = lappend(columnList, column);
	}

	foreignTableId = RelationGetRelid(relation);
	table = GetForeignTable(foreignTableId);
	server = GetForeignServer(table->serverid);
	user = GetUserMapping(GetUserId(), server->serverid);
	options = mongo_get_options(foreignTableId);

	plannerInfo = (MongoPlanerInfo *) palloc0(sizeof(MongoPlanerInfo));
	plannerInfo->rel_oid = foreignTableId;
	plannerInfo->reloptkind = RELOPT_BASEREL;
	pull_varattnos((Node *) columnList, foreignTableId,
				   &plannerInfo->attrs_used);

	/*
	 * Get connection to the foreign server.  Connection manager will establish
	 * new connection if necessary.
	 */
	mongoConnection = mongo_get_connection(server, user, options);

	queryDocument = QueryDocument(foreignTableId, NIL, NULL);

	/* Create cursor for collection name and set query */
	mongoCursor = MongoCursorCreate(mongoConnection, options->svr_database,
									options->collectionName, queryDocument, false);

	/*
	 * Use per-tuple memory context to prevent leak of memory used to read
	 * rows from the file with copy routines.
	 */
#if PG_VERSION_NUM < 110000
	tupleContext = AllocSetContextCreate(CurrentMemoryContext,
										 "mongo_fdw temporary context",
										 ALLOCSET_DEFAULT_MINSIZE,
										 ALLOCSET_DEFAULT_INITSIZE,
										 ALLOCSET_DEFAULT_MAXSIZE);
#else
	tupleContext = AllocSetContextCreate(CurrentMemoryContext,
										 "mongo_fdw temporary context",
										 ALLOCSET_DEFAULT_SIZES);
#endif

	/* Prepare for sampling rows */
	randomState = anl_init_selection_state(targetRowCount);

	columnValues = (Datum *) palloc(columnCount * sizeof(Datum));
	columnNulls = (bool *) palloc(columnCount * sizeof(bool));

	for (;;)
	{
		/* Check for user-requested abort or sleep */
		vacuum_delay_point();

		/* Initialize all values for this row to null */
		memset(columnValues, 0, columnCount * sizeof(Datum));
		memset(columnNulls, true, columnCount * sizeof(bool));

		if (MongoCursorNext(mongoCursor, NULL))
		{
			const BSON *bsonDocument = MongoCursorBson(mongoCursor);
			const char *bsonDocumentKey = NULL; /* Top level document */

			/* Fetch next tuple */
			MemoryContextReset(tupleContext);
			MemoryContextSwitchTo(tupleContext);

			FillTupleSlot(bsonDocument,
							bsonDocumentKey,
							plannerInfo,
							tupleDescriptor,
							columnValues,
							columnNulls,
							false);

			MemoryContextSwitchTo(oldContext);
		}
		else
		{
#ifdef META_DRIVER
			bson_error_t error;

			if (mongoc_cursor_error(mongoCursor, &error))
				ereport(ERROR,
						(errmsg("could not iterate over mongo collection"),
						 errhint("Mongo driver error: %s", error.message)));
#else
			mongo_cursor_error_t errorCode = mongoCursor->err;

			if (errorCode != MONGO_CURSOR_EXHAUSTED)
				ereport(ERROR,
						(errmsg("could not iterate over mongo collection"),
						 errhint("Mongo driver cursor error code: %d",
								 errorCode)));
#endif
			break;
		}

		/*
		 * The first targetRowCount sample rows are simply copied into the
		 * reservoir.  Then we start replacing tuples in the sample until we
		 * reach the end of the relation.  This algorithm is from Jeff
		 * Vitter's paper (see more info in commands/analyze.c).
		 */
		if (sampleRowCount < targetRowCount)
			sampleRows[sampleRowCount++] = heap_form_tuple(tupleDescriptor,
														   columnValues,
														   columnNulls);
		else
		{
			/*
			 * t in Vitter's paper is the number of records already processed.
			 * If we need to compute a new S value, we must use the "not yet
			 * incremented" value of rowCount as t.
			 */
			if (rowCountToSkip < 0)
				rowCountToSkip = anl_get_next_S(rowCount, targetRowCount,
												&randomState);

			if (rowCountToSkip <= 0)
			{
				/*
				 * Found a suitable tuple, so save it, replacing one old tuple
				 * at random.
				 */
				int			rowIndex = (int) (targetRowCount * anl_random_fract());

				Assert(rowIndex >= 0);
				Assert(rowIndex < targetRowCount);

				heap_freetuple(sampleRows[rowIndex]);
				sampleRows[rowIndex] = heap_form_tuple(tupleDescriptor,
													   columnValues,
													   columnNulls);
			}

			rowCountToSkip -= 1;
		}

		rowCount += 1;
	}

	/* Only clean up the query struct, but not its data */
	BsonDestroy(queryDocument);

	/* Clean up */
	MemoryContextDelete(tupleContext);

	pfree(columnValues);
	pfree(columnNulls);

	/* Emit some interesting relation info */
	relationName = RelationGetRelationName(relation);
	ereport(errorLevel,
			(errmsg("\"%s\": collection contains %.0f rows; %d rows in sample",
					relationName, rowCount, sampleRowCount)));

	(*totalRowCount) = rowCount;
	(*totalDeadRowCount) = 0;

	return sampleRowCount;
}

Datum
mongo_fdw_version(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT32(CODE_VERSION);
}

#if PG_VERSION_NUM >= 110000
/*
 * MongoBeginForeignInsert
 * 		Prepare for an insert operation triggered by partition routing
 * 		or COPY FROM.
 *
 * This is not yet supported, so raise an error.
 */
static void
MongoBeginForeignInsert(ModifyTableState *mtstate,
						ResultRelInfo *resultRelInfo)
{
	ereport(ERROR,
			(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
			 errmsg("COPY and foreign partition routing not supported in mongo_fdw")));
}

/*
 * MongoEndForeignInsert
 * 		BeginForeignInsert() is not yet implemented, hence we do not
 * 		have anything to cleanup as of now. We throw an error here just
 * 		to make sure when we do that we do not forget to cleanup
 * 		resources.
 */
static void
MongoEndForeignInsert(EState *estate, ResultRelInfo *resultRelInfo)
{
	ereport(ERROR,
			(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
			 errmsg("COPY and foreign partition routing not supported in mongo_fdw")));
}
#endif

/*
 * Assess whether the join between inner and outer relations can be pushed down
 * to the foreign server. As a side effect, save information we obtain in this
 * function to MongoFdwRelationInfo passed in.
 */
static bool
mongo_foreign_join_ok(PlannerInfo *root, RelOptInfo *joinrel, JoinType jointype,
				RelOptInfo *outerrel, RelOptInfo *innerrel,
				JoinPathExtraData *extra)
{
	MongoFdwRelationInfo *fpinfo;
	MongoFdwRelationInfo *fpinfo_o;
	MongoFdwRelationInfo *fpinfo_i;
	ListCell   *lc;
	List	   *joinclauses;

	/*
	 * We support pushing down LEFT joins.
	 */
	if (jointype != JOIN_LEFT)
		return false;

	/*
	 * If either of the joining relations is marked as unsafe to pushdown, the
	 * join can not be pushed down.
	 */
	fpinfo = (MongoFdwRelationInfo *) joinrel->fdw_private;
	fpinfo_o = (MongoFdwRelationInfo *) outerrel->fdw_private;
	fpinfo_i = (MongoFdwRelationInfo *) innerrel->fdw_private;
	if (!fpinfo_o || !fpinfo_o->pushdown_safe ||
		!fpinfo_i || !fpinfo_i->pushdown_safe)
		return false;

	/*
	 * If joining relations have local conditions, those conditions are
	 * required to be applied before joining the relations. Hence the join can
	 * not be pushed down.
	 */
	if (fpinfo_o->local_conds || fpinfo_i->local_conds)
		return false;

	/*
	 * In case the whole-row reference is under an outer join, currently mongo_fdw
	 * can not support to build the bson query document for it, so we can not push
	 * down.
	 */
	foreach(lc, joinrel->reltarget->exprs)
	{
		Expr *expr = (Expr *) lfirst(lc);
		if (IsA(expr, Var))
		{
			Var *var = (Var *)expr;

			if (var->varattno == 0)
				return false;
		}
	}

	/*
	 * Separate restrict list into join quals and pushed-down (other) quals.
	 *
	 * Join quals belonging to an outer join must all be shippable, else we
	 * cannot execute the join remotely.  Add such quals to 'joinclauses'.
	 *
	 * Add other quals to fpinfo->remote_conds if they are shippable, else to
	 * fpinfo->local_conds.  In an inner join it's okay to execute conditions
	 * either locally or remotely; the same is true for pushed-down conditions
	 * at an outer join.
	 *
	 * Note we might return failure after having already scribbled on
	 * fpinfo->remote_conds and fpinfo->local_conds.  That's okay because we
	 * won't consult those lists again if we deem the join unshippable.
	 */
	joinclauses = NIL;
	foreach(lc, extra->restrictlist)
	{
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);
		bool		is_remote_clause = mongo_is_foreign_expr(root, joinrel,
															 rinfo->clause);

		if (IS_OUTER_JOIN(jointype) &&
			!RINFO_IS_PUSHED_DOWN(rinfo, joinrel->relids))
		{
			if (!is_remote_clause)
				return false;
			joinclauses = lappend(joinclauses, rinfo);
		}
		else
		{
			if (is_remote_clause)
				fpinfo->remote_conds = lappend(fpinfo->remote_conds, rinfo);
			else
				fpinfo->local_conds = lappend(fpinfo->local_conds, rinfo);
		}
	}

	/*
	 * mongo_build_column_doc() isn't smart enough to handle anything other
	 * than a Var.  In particular, if there's some PlaceHolderVar that would
	 * need to be evaluated within this join tree (because there's an upper
	 * reference to a quantity that may go to NULL as a result of an outer
	 * join), then we can't try to push the join down because we'll fail when
	 * we get to mongo_build_column_doc().  However, a PlaceHolderVar that
	 * needs to be evaluated *at the top* of this join tree is OK, because we
	 * can do that locally after fetching the results from the remote side.
	 */
	foreach(lc, root->placeholder_list)
	{
		PlaceHolderInfo *phinfo = lfirst(lc);
		Relids		relids;

		/* PlaceHolderInfo refers to parent relids, not child relids. */
		relids = IS_OTHER_REL(joinrel) ?
			joinrel->top_parent_relids : joinrel->relids;

		if (bms_is_subset(phinfo->ph_eval_at, relids) &&
			bms_nonempty_difference(relids, phinfo->ph_eval_at))
			return false;
	}

	/* Save the join clauses, for later use. */
	fpinfo->joinclauses = joinclauses;

	fpinfo->outerrel = outerrel;
	fpinfo->innerrel = innerrel;
	fpinfo->jointype = jointype;

	/*
	 * Pull the other remote conditions from the joining relations into join
	 * clauses or other remote clauses (remote_conds) of this relation
	 * wherever possible. This avoids building subqueries at every join step.
	 *
	 * For an inner join, clauses from both the relations are added to the
	 * other remote clauses. For LEFT and RIGHT OUTER join, the clauses from
	 * the outer side are added to remote_conds since those can be evaluated
	 * after the join is evaluated. The clauses from inner side are added to
	 * the joinclauses, since they need to be evaluated while constructing the
	 * join.
	 *
	 * For a FULL OUTER JOIN, the other clauses from either relation can not
	 * be added to the joinclauses or remote_conds, since each relation acts
	 * as an outer relation for the other.
	 *
	 * The joining sides can not have local conditions, thus no need to test
	 * shippability of the clauses being pulled up.
	 */
	switch (jointype)
	{
		case JOIN_LEFT:
			fpinfo->joinclauses = list_concat(fpinfo->joinclauses,
											  fpinfo_i->remote_conds);
			fpinfo->remote_conds = list_concat(fpinfo->remote_conds,
											   fpinfo_o->remote_conds);
			break;

		default:
			/* Should not happen, we have just checked this above */
			elog(ERROR, "unsupported join type %d", jointype);
	}

	/* Mark that this join can be pushed down safely */
	fpinfo->pushdown_safe = true;

	/*
	 * Set the relation index.  This is defined as the position of this
	 * joinrel in the join_rel_list list plus the length of the rtable list.
	 * Note that since this joinrel is at the end of the join_rel_list list
	 * when we are called, we can get the position by list_length.
	 */
	Assert(fpinfo->relation_index == 0);	/* shouldn't be set yet */
	fpinfo->relation_index =
		list_length(root->parse->rtable) + list_length(root->join_rel_list);

	return true;
}

/*
 * MongoGetForeignJoinPaths
 *		Add possible ForeignPath to joinrel, if join is safe to push down.
 */
static void
MongoGetForeignJoinPaths(PlannerInfo *root,
							RelOptInfo *joinrel,
							RelOptInfo *outerrel,
							RelOptInfo *innerrel,
							JoinType jointype,
							JoinPathExtraData *extra)
{
	MongoFdwRelationInfo *fpinfo;
	ForeignPath *joinpath;
	double		rows;
	int			width;
	Cost		startup_cost;
	Cost		total_cost;
	Path	   *epq_path;		/* Path to create plan to be executed when
								 * EvalPlanQual gets triggered. */

	/*
	 * Skip if this join combination has been considered already.
	 */
	if (joinrel->fdw_private)
		return;

	/* Does not support JOIN for UPDATE and DELETE operation. */
	if (root->parse->commandType == CMD_DELETE ||
		root->parse->commandType == CMD_UPDATE)
		return;

	/*
	 * This code does not work for joins with lateral references, since those
	 * must have parameterized paths, which we don't generate yet.
	 */
	if (!bms_is_empty(joinrel->lateral_relids))
		return;

	/*
	 * Create unfinished MongoFdwRelationInfo entry which is used to indicate
	 * that the join relation is already considered, so that we won't waste
	 * time in judging safety of join pushdown and adding the same paths again
	 * if found safe. Once we know that this join can be pushed down, we fill
	 * the entry.
	 */
	fpinfo = (MongoFdwRelationInfo *) palloc0(sizeof(MongoFdwRelationInfo));
	fpinfo->pushdown_safe = false;
	joinrel->fdw_private = fpinfo;

	/*
	 * If there is a possibility that EvalPlanQual will be executed, we need
	 * to be able to reconstruct the row using scans of the base relations.
	 * GetExistingLocalJoinPath will find a suitable path for this purpose in
	 * the path list of the joinrel, if one exists.  We must be careful to
	 * call it before adding any ForeignPath, since the ForeignPath might
	 * dominate the only suitable local path available.  We also do it before
	 * calling foreign_join_ok(), since that function updates fpinfo and marks
	 * it as pushable if the join is found to be pushable.
	 */
	if (root->rowMarks)
	{
		epq_path = GetExistingLocalJoinPath(joinrel);
		if (!epq_path)
		{
			elog(DEBUG3, "could not push down foreign join because a local path suitable for EPQ checks was not found");
			return;
		}
	}
	else
		epq_path = NULL;

	if (!mongo_foreign_join_ok(root, joinrel, jointype, outerrel, innerrel, extra))
	{
		/* Free path required for EPQ if we copied one; we don't need it now */
		if (epq_path)
			pfree(epq_path);
		return;
	}

	/* Estimate the cost of push down */
	rows = startup_cost = total_cost = width = 0;

	/* Now update this information in the joinrel */
	joinrel->rows = rows;
	joinrel->reltarget->width = width;

	/*
	 * Create a new join path and add it to the joinrel which represents a
	 * join between foreign tables.
	 */
	joinpath = create_foreign_join_path(root,
										joinrel,
										NULL,	/* default pathtarget */
										rows,
										startup_cost,
										total_cost,
										NIL,	/* no pathkeys */
										joinrel->lateral_relids,
										epq_path,
										NIL);	/* no fdw_private */

	/* Add generated path into joinrel by add_path(). */
	add_path(joinrel, (Path *) joinpath);

	/* XXX Consider parameterized paths for the join relation */
}

/*
 * Assess whether the aggregation, grouping and having operations can be pushed
 * down to the foreign server.  As a side effect, save information we obtain in
 * this function to PgFdwRelationInfo of the input relation.
 */
static bool
mongo_foreign_grouping_ok(PlannerInfo *root, RelOptInfo *grouped_rel,
					Node *havingQual)
{
	Query	   *query = root->parse;
	MongoFdwRelationInfo *fpinfo = (MongoFdwRelationInfo *) grouped_rel->fdw_private;
	PathTarget *grouping_target = grouped_rel->reltarget;
	MongoFdwRelationInfo *ofpinfo;
	ListCell   *lc;
	int			i;
	List	   *tlist = NIL;

	/* We currently don't support pushing Grouping Sets. */
	if (query->groupingSets)
		return false;

	/* Get the fpinfo of the underlying scan relation. */
	ofpinfo = (MongoFdwRelationInfo *) fpinfo->outerrel->fdw_private;

	/*
	 * If underlying scan relation has any local conditions, those conditions
	 * are required to be applied before performing aggregation.  Hence the
	 * aggregate cannot be pushed down.
	 */
	if (ofpinfo->local_conds)
		return false;

	/*
	 * Examine grouping expressions, as well as other expressions we'd need to
	 * compute, and check whether they are safe to push down to the foreign
	 * server.  All GROUP BY expressions will be part of the grouping target
	 * and thus there is no need to search for them separately.  Add grouping
	 * expressions into target list which will be passed to foreign server.
	 *
	 * A tricky fine point is that we must not put any expression into the
	 * target list that is just a foreign param (that is, something that
	 * deparse.c would conclude has to be sent to the foreign server).  If we
	 * do, the expression will also appear in the fdw_exprs list of the plan
	 * node, and setrefs.c will get confused and decide that the fdw_exprs
	 * entry is actually a reference to the fdw_scan_tlist entry, resulting in
	 * a broken plan.  Somewhat oddly, it's OK if the expression contains such
	 * a node, as long as it's not at top level; then no match is possible.
	 */
	i = 0;
	foreach(lc, grouping_target->exprs)
	{
		Expr	   *expr = (Expr *) lfirst(lc);
		Index		sgref = get_pathtarget_sortgroupref(grouping_target, i);
		ListCell   *l;

		/* Check whether this expression is part of GROUP BY clause */
		if (sgref && get_sortgroupref_clause_noerr(sgref, query->groupClause))
		{
			TargetEntry *tle;

			/*
			 * If any GROUP BY expression is not shippable, then we cannot
			 * push down aggregation to the foreign server.
			 */
			if (!mongo_is_foreign_expr(root, grouped_rel, expr))
				return false;

			/*
			 * If it would be a foreign param, we can't put it into the tlist,
			 * so we have to fail.
			 */
			if (mongo_is_foreign_param(root, grouped_rel, expr))
				return false;

			/*
			 * Pushable, so add to tlist.  We need to create a TLE for this
			 * expression and apply the sortgroupref to it.  We cannot use
			 * add_to_flat_tlist() here because that avoids making duplicate
			 * entries in the tlist.  If there are duplicate entries with
			 * distinct sortgrouprefs, we have to duplicate that situation in
			 * the output tlist.
			 */
			tle = makeTargetEntry(expr, list_length(tlist) + 1, NULL, false);
			tle->ressortgroupref = sgref;
			tlist = lappend(tlist, tle);
		}
		else
		{
			/*
			 * Non-grouping expression we need to compute.  Can we ship it
			 * as-is to the foreign server?
			 */
			if (mongo_is_foreign_expr(root, grouped_rel, expr) &&
				!mongo_is_foreign_param(root, grouped_rel, expr))
			{
				/* Yes, so add to tlist as-is; OK to suppress duplicates */
				tlist = add_to_flat_tlist(tlist, list_make1(expr));
			}
			else
			{
				/* Not pushable as a whole; extract its Vars and aggregates */
				List	   *aggvars;

				aggvars = pull_var_clause((Node *) expr,
										  PVC_INCLUDE_AGGREGATES);

				/*
				 * If any aggregate expression is not shippable, then we
				 * cannot push down aggregation to the foreign server.  (We
				 * don't have to check is_foreign_param, since that certainly
				 * won't return true for any such expression.)
				 */
				if (!mongo_is_foreign_expr(root, grouped_rel, (Expr *) aggvars))
					return false;

				/*
				 * Add aggregates, if any, into the targetlist.  Plain Vars
				 * outside an aggregate can be ignored, because they should be
				 * either same as some GROUP BY column or part of some GROUP
				 * BY expression.  In either case, they are already part of
				 * the targetlist and thus no need to add them again.  In fact
				 * including plain Vars in the tlist when they do not match a
				 * GROUP BY column would cause the foreign server to complain
				 * that the shipped query is invalid.
				 */
				foreach(l, aggvars)
				{
					Expr	   *expr = (Expr *) lfirst(l);

					if (IsA(expr, Aggref))
						tlist = add_to_flat_tlist(tlist, list_make1(expr));
				}
			}
		}

		i++;
	}

	/*
	 * Classify the pushable and non-pushable HAVING clauses and save them in
	 * remote_conds and local_conds of the grouped rel's fpinfo.
	 */
	if (havingQual)
	{
		ListCell   *lc;

		foreach(lc, (List *) havingQual)
		{
			Expr	   *expr = (Expr *) lfirst(lc);
			RestrictInfo *rinfo;

			/*
			 * Currently, the core code doesn't wrap havingQuals in
			 * RestrictInfos, so we must make our own.
			 */
			Assert(!IsA(expr, RestrictInfo));
#if PG_VERSION_NUM >= 140000
			rinfo = make_restrictinfo(root,
#else
			rinfo = make_restrictinfo(
#endif
									  expr,
									  true,
									  false,
									  false,
									  root->qual_security_level,
									  grouped_rel->relids,
									  NULL,
									  NULL);
			if (mongo_is_foreign_expr(root, grouped_rel, expr))
				fpinfo->remote_conds = lappend(fpinfo->remote_conds, rinfo);
			else
				fpinfo->local_conds = lappend(fpinfo->local_conds, rinfo);
		}
	}

	/*
	 * If there are any local conditions, pull Vars and aggregates from it and
	 * check whether they are safe to pushdown or not.
	 */
	if (fpinfo->local_conds)
	{
		List	   *aggvars = NIL;
		ListCell   *lc;

		foreach(lc, fpinfo->local_conds)
		{
			RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

			aggvars = list_concat(aggvars,
								  pull_var_clause((Node *) rinfo->clause,
												  PVC_INCLUDE_AGGREGATES));
		}

		foreach(lc, aggvars)
		{
			Expr	   *expr = (Expr *) lfirst(lc);

			/*
			 * If aggregates within local conditions are not safe to push
			 * down, then we cannot push down the query.  Vars are already
			 * part of GROUP BY clause which are checked above, so no need to
			 * access them again here.  Again, we need not check
			 * is_foreign_param for a foreign aggregate.
			 */
			if (IsA(expr, Aggref))
			{
				if (!mongo_is_foreign_expr(root, grouped_rel, expr))
					return false;

				tlist = add_to_flat_tlist(tlist, list_make1(expr));
			}
		}
	}

	/* Store generated targetlist */
	fpinfo->grouped_tlist = tlist;

	/* Safe to pushdown */
	fpinfo->pushdown_safe = true;

	return true;
}

/*
 * mongo_add_foreign_grouping_paths
 *		Add foreign path for grouping and/or aggregation.
 *
 * Given input_rel represents the underlying scan.  The paths are added to the
 * given grouped_rel.
 */
static void
mongo_add_foreign_grouping_paths(PlannerInfo *root, RelOptInfo *input_rel,
						   RelOptInfo *grouped_rel,
						   GroupPathExtraData *extra)
{
	Query	   *parse = root->parse;
	MongoFdwRelationInfo *fpinfo = grouped_rel->fdw_private;
	ForeignPath *grouppath;
	double		rows;
	Cost		startup_cost;
	Cost		total_cost;

	/* Nothing to be done, if there is no grouping or aggregation required. */
	if (!parse->groupClause && !parse->groupingSets && !parse->hasAggs &&
		!root->hasHavingQual)
		return;

	Assert(extra->patype == PARTITIONWISE_AGGREGATE_NONE ||
		   extra->patype == PARTITIONWISE_AGGREGATE_FULL);

	/* save the input_rel as outerrel in fpinfo */
	fpinfo->outerrel = input_rel;

	/*
	 * Assess if it is safe to push down aggregation and grouping.
	 *
	 * Use HAVING qual from extra. In case of child partition, it will have
	 * translated Vars.
	 */
	if (!mongo_foreign_grouping_ok(root, grouped_rel, extra->havingQual))
		return;

	/*
	 * If no grouping, numGroups - possible number of groups should be set 1.
	 * When creating upper path, rows is passed to pathnode->path.rows.
	 * When creating aggregation plan, somehow path.rows is passed to numGroups.
	 */
	if (!parse->groupClause)
	{
		/* Not grouping */
		rows = 1;
	}
	else if (parse->hasAggs || root->hasHavingQual)
	{
		/* Plain aggregation, one result row */
		rows = 1;
	}
	else
	{
		rows = 0;
	}

	/* Estimate the cost of push down */
	startup_cost = total_cost = 0;


	/* Create and add foreign path to the grouping relation. */
	grouppath = create_foreign_upper_path(root,
										  grouped_rel,
										  grouped_rel->reltarget,
										  rows,
										  startup_cost,
										  total_cost,
										  NIL,	/* no pathkeys */
										  NULL,
										  NIL); /* no fdw_private */

	/* Add generated path into grouped_rel by add_path(). */
	add_path(grouped_rel, (Path *) grouppath);
}

/*
 * add_foreign_final_paths
 *		Add foreign paths for performing the final processing remotely.
 *
 * Given input_rel contains the source-data Paths.  The paths are added to the
 * given final_rel.
 */
static void
mongo_add_foreign_final_paths(PlannerInfo *root, RelOptInfo *input_rel,
						RelOptInfo *final_rel,
						FinalPathExtraData *extra)
{
	Query	   *parse = root->parse;
	MongoFdwRelationInfo *ifpinfo = (MongoFdwRelationInfo *) input_rel->fdw_private;
	MongoFdwRelationInfo *fpinfo = (MongoFdwRelationInfo *) final_rel->fdw_private;
	double		rows;
	int			width;
	Cost		startup_cost;
	Cost		total_cost;
	List	   *fdw_private;
	ForeignPath *final_path;

	/*
	 * Currently, we only support this for SELECT commands
	 */
	if (parse->commandType != CMD_SELECT)
		return;

	/*
	 * We do not support LIMIT with FOR UPDATE/SHARE.
	 * Also, if there is no FOR UPDATE/SHARE clause and
	 * there is no LIMIT, don't need to add Foreign final path.
	 */
	if (parse->rowMarks || !extra->limit_needed)
		return;

	/* We don't support cases where there are any SRFs in the targetlist */
	if (parse->hasTargetSRFs)
		return;

	/* Save the input_rel as outerrel in fpinfo */
	fpinfo->outerrel = input_rel;

	Assert(extra->limit_needed);

	/* The input_rel should be a base, join, or grouping relation */
	Assert(input_rel->reloptkind == RELOPT_BASEREL ||
		   input_rel->reloptkind == RELOPT_JOINREL ||
		   (input_rel->reloptkind == RELOPT_UPPER_REL &&
			ifpinfo->stage == UPPERREL_GROUP_AGG));

	/*
	 * If the underlying relation has any local conditions, the LIMIT/OFFSET
	 * cannot be pushed down.
	 */
	if (ifpinfo->local_conds)
		return;

	/*
	 * Also, the LIMIT/OFFSET cannot be pushed down, if their expressions are
	 * not safe to remote.
	 */
	if (!mongo_is_foreign_expr(root, input_rel, (Expr *) parse->limitOffset) ||
		!mongo_is_foreign_expr(root, input_rel, (Expr *) parse->limitCount))
		return;

	/* Safe to push down */
	fpinfo->pushdown_safe = true;

	/*
	 * Use small cost to push down limit always
	 */
	rows = width = startup_cost = total_cost = 0;

	/*
	 * Build the fdw_private list that will be used by MongoGetForeignPlan.
	 * Items in the list must match order in enum FdwPathPrivateIndex.
	 */
	fdw_private = list_make2(makeInteger(false),
							 makeInteger(extra->limit_needed));

	/*
	 * Create foreign final path; this gets rid of a no-longer-needed outer
	 * plan (if any), which makes the EXPLAIN output look cleaner
	 */
	final_path = create_foreign_upper_path(root,
										   input_rel,
										   root->upper_targets[UPPERREL_FINAL],
										   rows,
										   startup_cost,
										   total_cost,
										   NULL, /* no pathkeys */
										   NULL,	/* no extra plan */
										   fdw_private);

	/* and add it to the final_rel */
	add_path(final_rel, (Path *) final_path);
}

/*
 * MongoGetForeignUpperPaths
 *		Add paths for post-join operations like aggregation, grouping etc. if
 *		corresponding operations are safe to push down.
 */
static void
MongoGetForeignUpperPaths(PlannerInfo *root, UpperRelationKind stage,
						  RelOptInfo *input_rel, RelOptInfo *output_rel,
						  void *extra)
{
	MongoFdwRelationInfo *fpinfo;

	/*
	 * If input rel is not safe to pushdown, then simply return as we cannot
	 * perform any post-join operations on the foreign server.
	 */
	if (!input_rel->fdw_private ||
		!((MongoFdwRelationInfo *) input_rel->fdw_private)->pushdown_safe)
		return;

	/* Ignore stages we don't support; and skip any duplicate calls. */
	if ((stage != UPPERREL_GROUP_AGG &&
		 stage != UPPERREL_FINAL) ||
		output_rel->fdw_private)
		return;

	fpinfo = (MongoFdwRelationInfo *) palloc0(sizeof(MongoFdwRelationInfo));
	fpinfo->pushdown_safe = false;
	fpinfo->stage = stage;
	output_rel->fdw_private = fpinfo;

	switch (stage)
	{
		case UPPERREL_GROUP_AGG:
			mongo_add_foreign_grouping_paths(root, input_rel, output_rel,
									   (GroupPathExtraData *) extra);
			break;
		case UPPERREL_FINAL:
			mongo_add_foreign_final_paths(root, input_rel, output_rel,
									(FinalPathExtraData *) extra);
			break;
		default:
			elog(ERROR, "unexpected upper relation: %d", (int) stage);
			break;
	}
}

/*
 * Force assorted GUC parameters to settings that ensure that we'll output
 * data values in a form that is unambiguous to the remote server.
 *
 * This is rather expensive and annoying to do once per row, but there's
 * little choice if we want to be sure values are transmitted accurately;
 * we can't leave the settings in place between rows for fear of affecting
 * user-visible computations.
 *
 * We use the equivalent of a function SET option to allow the settings to
 * persist only until the caller calls mongo_reset_transmission_modes().  If an
 * error is thrown in between, guc.c will take care of undoing the settings.
 *
 * The return value is the nestlevel that must be passed to
 * mongo_reset_transmission_modes() to undo things.
 */
int
mongo_set_transmission_modes(void)
{
	int			nestlevel = NewGUCNestLevel();

	/*
	 * The values set here should match what pg_dump does.  See also
	 * configure_remote_session in connection.c.
	 */
	if (DateStyle != USE_ISO_DATES)
		(void) set_config_option("datestyle", "ISO",
								 PGC_USERSET, PGC_S_SESSION,
								 GUC_ACTION_SAVE, true, 0, false);
	if (IntervalStyle != INTSTYLE_POSTGRES)
		(void) set_config_option("intervalstyle", "postgres",
								 PGC_USERSET, PGC_S_SESSION,
								 GUC_ACTION_SAVE, true, 0, false);
	if (extra_float_digits < 3)
		(void) set_config_option("extra_float_digits", "3",
								 PGC_USERSET, PGC_S_SESSION,
								 GUC_ACTION_SAVE, true, 0, false);

	return nestlevel;
}

/*
 * Undo the effects of mongo_set_transmission_modes().
 */
void
mongo_reset_transmission_modes(int nestlevel)
{
	AtEOXact_GUC(true, nestlevel);
}

/*
 * mongo_get_join_planner_info
 */
static void mongo_get_join_planner_info(RelOptInfo *scanrel, MongoPlanerInfo *plannerInfo)
{
	MongoFdwRelationInfo *f_joininfo = (MongoFdwRelationInfo *) scanrel->fdw_private;
	RelOptInfo *outerrel = f_joininfo->outerrel;
	RelOptInfo *innerrel = f_joininfo->innerrel;
	MongoPlanerJoinInfo *join_info = NULL;

	Assert(IS_JOIN_REL(scanrel));

	join_info = (MongoPlanerJoinInfo *) palloc0(sizeof (MongoPlanerJoinInfo));

	/* Pickup information of JOIN relation */
	if (IS_SIMPLE_REL(outerrel))
	{
		join_info->outerrel_relid = outerrel->relid;
	}
	else if (IS_JOIN_REL(outerrel))
	{
		mongo_get_join_planner_info(outerrel, plannerInfo);
	}

	if (IS_SIMPLE_REL(innerrel))
	{
		join_info->innerrel_relid = innerrel->relid;
	}
	else if (IS_JOIN_REL(innerrel))
	{
		mongo_get_join_planner_info(innerrel, plannerInfo);
	}

	if (IS_JOIN_REL(scanrel))
		join_info->joinclauses = f_joininfo->joinclauses;

	plannerInfo->joininfo_list = lappend(plannerInfo->joininfo_list, join_info);
}

/*
 * Get LIMIT/OFFSET information.
 *  If LIMIT NULL | ALL, there is no need LIMIT.
 *  If OFFSET NULL, it is treated as OFFSET 0, then
 * 	there is no need OFFSET.
 * Refer from limit_needed() function
 */
static void mongo_get_limit_info(PlannerInfo *root, MongoPlanerInfo *plannerInfo)
{
	Query *parse = root->parse;
	Node *node;

	node = parse->limitCount;
	if (node)
	{
		if (IsA(node, Const))
		{
			/* NULL indicates LIMIT ALL, ie, no limit */
			if (!((Const *) node)->constisnull)
				plannerInfo->limitCount = parse->limitCount;	/* LIMIT with a constant value */
		}
		else
			plannerInfo->limitCount = parse->limitCount;		/* non-constant LIMIT */
	}

	node = parse->limitOffset;
	if (node)
	{
		if (IsA(node, Const))
		{
			/* Treat NULL as no offset; the executor would too */
			if (!((Const *) node)->constisnull)
			{
				int64		offset = DatumGetInt64(((Const *) node)->constvalue);

				if (offset != 0)
					plannerInfo->limitOffset = parse->limitOffset;	/* OFFSET with a nonzero value */
			}
		}
		else
			plannerInfo->limitOffset = parse->limitOffset;		/* non-constant OFFSET */
	}
}
