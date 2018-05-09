/*-------------------------------------------------------------------------
 *
 * diag_planner.c
 *		light-weight diagnostic tool for planner
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "fmgr.h"

#include "nodes/nodes.h"
#include "lib/stringinfo.h"
#include "optimizer/plancat.h"
#include "optimizer/planner.h"
#include "optimizer/paths.h"
#include "optimizer/prep.h"
#include "parser/parsetree.h"
#include "lib/stringinfo.h"
#include "nodes/extensible.h"
#include "nodes/plannodes.h"
#include "nodes/relation.h"
#include "utils/datum.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"

PG_MODULE_MAGIC;

static set_rel_pathlist_hook_type prev_set_rel_pathlist = NULL;
static set_join_pathlist_hook_type prev_set_join_pathlist = NULL;

void _PG_init(void);
void my_set_rel_pathlist (PlannerInfo *root,
						  RelOptInfo *rel,
						  Index rti,
						  RangeTblEntry *rte);
void my_set_join_pathlist (PlannerInfo *root,
						   RelOptInfo *joinrel,
						   RelOptInfo *outerrel,
						   RelOptInfo *innerrel,
						   JoinType jointype,
						   JoinPathExtraData *extra);

static void
_outJoinPath(PlannerInfo *root, Path *path, StringInfo str_out)
{
	StringInfoData str;

	initStringInfo(&str);
	switch (path->pathtype)
	{
		case T_HashJoin:
			appendStringInfo(&str, "hashjoin ");
			break;
		case T_MergeJoin:
			appendStringInfo(&str, "mergejoin ");
			break;
		case T_NestLoop:
			appendStringInfo(&str, "nestloop ");
			break;
		case T_Append:
			appendStringInfo(&str, "append ");
			break;
	}
}

static char *
_outPath(PlanerInfo *root, Path *path, Oid relid)
{
	StringInfoData str;

	initStringInfo(&str);
	switch (path->pathtype)
	{
		case T_SeqScan:
			appendStringInfo(&str, "sequential\t");
			break;
		case T_SampleScan:
			appendStringInfo(&str, "sample\t");
			break;
		case T_IndexScan:
			appendStringInfo(&str, "index\t");
			break;
		case T_IndexOnlyScan:
			appendStringInfo(&str, "indexonly\t");
			break;
		case T_BitmapIndexScan:
			appendStringInfo(&str, "bitmapindex\t");
			break;
		case T_BitmapHeapScan:
			appendStringInfo(&str, "bitmapheap\t");
			break;
		case T_HashJoin:
			appendStringInfo(&str, "hashjoin ");
			break;
		case T_MergeJoin:
			appendStringInfo(&str, "mergejoin ");
			break;
		case T_NestLoop:
			appendStringInfo(&str, "nestloop ");
			break;
		case T_Append:
			appendStringInfo(&str, "append ");
			break;
		default:
			appendStringInfo(&str, "<>");
	}

	/* Cost */
	appendStringInfo(&str, "%s%s\t(%.2f..%.2f)",
					 OidIsValid(relid) ? "on " : "",
					 OidIsValid(relid) ? get_rel_name(relid) : "",
					 path->startup_cost, path->total_cost);

	return str.data;
}

void
_PG_init(void)
{

	prev_set_rel_pathlist = set_rel_pathlist_hook;
	set_rel_pathlist_hook = my_set_rel_pathlist;

	prev_set_join_pathlist = set_join_pathlist_hook;
	set_join_pathlist_hook = my_set_join_pathlist;
}

void
my_set_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte)
{
	ListCell		*cell;
	elog(NOTICE, "----- SCAN PATH LIST for \"%s\" -----", get_rel_name(rte->relid));

	/* Scan method */
	foreach (cell, rel->pathlist)
	{
		Path *path = lfirst(cell);

		elog(NOTICE, "SCAN : %s", _outPath(path, rte->relid));
	}
}

void
my_set_join_pathlist(PlannerInfo *root, RelOptInfo *joinrel, RelOptInfo *outerrel,
					 RelOptInfo *innerrel, JoinType jointype, JoinPathExtraData *extra)
{
	StringInfoData	str;
	ListCell 		*cell;
	int				idx = 0;
	RangeTblEntry	*outer_rte, *inner_rte;
	char			*jointype_str;

	/* Join relations */
	idx = bms_next_member(outerrel->relids, 0);
	outer_rte = planner_rt_fetch(idx, root);

	idx = bms_next_member(innerrel->relids, 0);
	inner_rte = planner_rt_fetch(idx, root);

	/* Join type */
	switch (jointype)
	{
		case JOIN_INNER:
			jointype_str = "Inner";
			break;
		case JOIN_LEFT:
			jointype_str = "Left";
			break;
		case JOIN_RIGHT:
			jointype_str = "Rigth";
			break;
		case JOIN_FULL:
			jointype_str = "Full";
			break;
		case JOIN_SEMI:
			jointype_str = "Semi";
			break;
		case JOIN_ANTI:
			jointype_str = "Anti";
			break;
		case JOIN_UNIQUE_OUTER:
			jointype_str = "UniqueOuter";
			break;
		case JOIN_UNIQUE_INNER:
			jointype_str = "UniqueInner";
			break;
	}

	elog(NOTICE, "----- JOIN PATH LIST for \"%s\" and \"%s\" -----",
		 get_rel_name(outer_rte->relid), get_rel_name(inner_rte->relid));

	foreach (cell, joinrel->pathlist)
	{
		Path *path = lfirst(cell);
		JoinPath *joinpath = (JoinPath *) path;

		initStringInfo(&str);

		elog(NOTICE, "JOIN : %s %s", jointype_str, _outPath(path, InvalidOid));

		/* Join relations */
		elog(NOTICE, "\t |- %s", _outPath(joinpath->outerjoinpath, outer_rte->relid));
		elog(NOTICE, "\t |- %s", _outPath(joinpath->innerjoinpath, inner_rte->relid));
	}

}
