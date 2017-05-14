/* -------------------------------------------------------------------------
 *
 * pgha.h
 *
 * Header file for pg_keeper.
 *
 * -------------------------------------------------------------------------
 */

/* These are always necessary for a bgworker */
#include "access/xlog.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"

#include "tcop/utility.h"
#include "libpq-int.h"

typedef struct PgHaNode
{
	slock_t	mutex;
	char	name[NAMEDATALEN];
	char	conninfo[MAXPGPATH];
	char	type;
	bool	in_use;
	bool	myself;
	bool	live;
	bool	is_sync;
	int		retry_count;
} PgHaNode;

typedef struct PgHaCtlData
{
	LWLock	*lock;
	int	n_nodes;
	PgHaNode	nodes[FLEXIBLE_ARRAY_MEMBER];
} PgHaCtlData;

extern void PgHaMain(Datum main_arg);
