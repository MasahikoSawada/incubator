/* -------------------------------------------------------------------------
 *
 * pgha.c
 *
 * Simple clustering extension module for PostgreSQL.
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "pgha.h"

/* These are always necessary for a bgworker */
#include "access/htup_details.h"
#include "access/xlog.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "replication/syncrep.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "utils/builtins.h"
#include "utils/ps_status.h"

/* these headers are used by this particular worker's code */
#include "tcop/utility.h"
#include "libpq-int.h"

#define am_master() (!InRecovery)

PG_MODULE_MAGIC;

void	_PG_init(void);
void	_PG_fini(void);
void	PgHaMain(Datum);

PG_FUNCTION_INFO_V1(add_node);
PG_FUNCTION_INFO_V1(del_node);
PG_FUNCTION_INFO_V1(join_node);

static void checkParameter(void);
static void doHeartbeat(void);
static bool checkClusterStatus(void);
static bool addNode(const char *name, const char *conninfo, bool myself,
					bool dup_ok);
static bool delNode(const char *name);
static bool execSQL(const char *conninfo, const char *sql);

/* master */
static bool	PgHaMasterLoop(void);
static void changeToAsync(void);

/* slave */
static bool	PgHaStandbyLoop(void);

static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static void pgha_shmem_startup(void);
static Size pgha_shmemsize(void);
static bool check_pgha_node_name(char **newval, void **extra, GucSource source);
static bool check_pgha_conninfo(char **newval, void **extra, GucSource source);
static int	get_hanode_count(void);

/* Function for signal handler */
static void pgha_sigterm(SIGNAL_ARGS);
static void pgha_sighup(SIGNAL_ARGS);

/* debug fucns */
static void debug_show(void);

/* flags set by signal handlers */
sig_atomic_t got_sighup = false;
sig_atomic_t got_sigterm = false;

/* GUC variables */
int	pgha_keepalives_time;
int	pgha_retry_count;
int pgha_max_nodes;
char *pgha_node_name;
char *pgha_my_conninfo;
char *pgha_after_command;

bool	in_syncrep = false;

PgHaCtlData *PgHaCtl = NULL;
PgHaNode *MyHa = NULL;


/*
 * Entrypoint of this module.
 *
 * We register more than one worker process here, to demonstrate how that can
 * be done.
 */
void
_PG_init(void)
{
	BackgroundWorker worker;

	if (!process_shared_preload_libraries_in_progress)
		return;

	/* get the configuration */
	DefineCustomIntVariable("pgha.max_ha_nodes",
							"The maximim number of nodes",
							NULL,
							&pgha_max_nodes,
							10,
							0,
							INT_MAX,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("pgha.keepalives_time",
							"Specific time between polling to primary server",
							NULL,
							&pgha_keepalives_time,
							5,
							1,
							INT_MAX,
							PGC_SIGHUP,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("pgha.retry_count",
							"Specific retry count until promoting standby server",
							NULL,
							&pgha_retry_count,
							4,
							1,
							INT_MAX,
							PGC_SIGHUP,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomStringVariable("pgha.my_conninfo",
							   "My connection information used for ALTER SYSTEM",
							   NULL,
							   &pgha_my_conninfo,
							   NULL,
							   PGC_POSTMASTER,
							   0,
							   check_pgha_conninfo,
							   NULL,
							   NULL);

	DefineCustomStringVariable("pgha.node_name",
							   "Node name",
							   NULL,
							   &pgha_node_name,
							   NULL,
							   PGC_POSTMASTER,
							   0,
							   check_pgha_node_name,
							   NULL,
							   NULL);

	DefineCustomStringVariable("pgha.after_command",
							   "Shell command that will be called after promoted",
							   NULL,
							   &pgha_after_command,
							   NULL,
							   PGC_SIGHUP,
							   GUC_NOT_IN_SAMPLE,
							   NULL,
							   NULL,
							   NULL);

	/* Install hook */
    prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = pgha_shmem_startup;

	/* request additional sharedresource */
	RequestAddinShmemSpace(pgha_shmemsize());
	RequestNamedLWLockTranche("pgha", 1);

	/* set up common data for all our workers */
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	snprintf(worker.bgw_library_name, BGW_MAXLEN, "pgha");
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "PgHaMain");
	snprintf(worker.bgw_name, BGW_MAXLEN, "pgha");

	/* Now fill in worker-specific data, and do the actual registrations */
	worker.bgw_main_arg = Int32GetDatum(1);
	RegisterBackgroundWorker(&worker);
}

void _PG_fini(void)
{
	/* Uninstall hook */
    shmem_startup_hook = prev_shmem_startup_hook;
}

static void
pgha_shmem_startup(void)
{
	bool found;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	PgHaCtl = ShmemInitStruct("pgha",
							  pgha_shmemsize(),
							  &found);
	if (!found)
	{
		int i;

		PgHaCtl->lock = &(GetNamedLWLockTranche("pgha"))->lock;
		PgHaCtl->n_nodes = 0;
		
		/* Initialize spin lock of each nodes */
		for (i = 0; i < pgha_max_nodes; i++)
		{
			PgHaNode *n = &PgHaCtl->nodes[i];

			n->in_use = false;
			SpinLockInit(&n->mutex);
		}
	}

	LWLockRelease(AddinShmemInitLock);
}

/*
 * Signal handler for SIGTERM
 *		Set a flag to let the main loop to terminate, and set our latch to wake
 *		it up.
 */
static void
pgha_sigterm(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sigterm = true;
	if (MyProc)
		SetLatch(&MyProc->procLatch);

	errno = save_errno;
}

/*
 * Signal handler for SIGHUP
 *		Set a flag to let the main loop to reread the config file, and set
 *		our latch to wake it up.
 */
static void
pgha_sighup(SIGNAL_ARGS)
{
	got_sighup = true;
	if (MyProc)
		SetLatch(&MyProc->procLatch);
}

/*
 * Entry point for pgha.
 */
void
PgHaMain(Datum main_arg)
{
	bool	ret;
	
	/* Sanity check */
	checkParameter();

	/* Establish signal handlers before unblocking signals */
	pqsignal(SIGHUP, pgha_sighup);
	pqsignal(SIGTERM, pgha_sigterm);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Connect to our database */
	BackgroundWorkerInitializeConnection("postgres", NULL);

	/* Register my info first */
	addNode(pgha_node_name, pgha_my_conninfo, true, false);
	MyHa = &PgHaCtl->nodes[0];

	if (am_master())
	{
		ret = PgHaMasterLoop();
	}
	else
	{
		ret = PgHaStandbyLoop();
	}
	
	proc_exit(ret);
}

/* Check the mandatory parameteres */
static void
checkParameter()
{
	if (!EnableHotStandby)
		ereport(ERROR, (errmsg("hot_standby must be enabled.")));

	if (pgha_my_conninfo == NULL || pgha_my_conninfo[0] == '\0')
		ereport(ERROR, (errmsg("pgha.my_conninfo must be specified.")));

	if (pgha_node_name == NULL || pgha_node_name[0] == '\0')
		ereport(ERROR, (errmsg("pgha.node_name must be specified.")));
}

static Size
pgha_shmemsize(void)
{
	Size size;

	size = MAXALIGN(sizeof(PgHaCtlData));
	size = add_size(size, sizeof(PgHaNode) * pgha_max_nodes);

	return size;
}

static bool
check_pgha_node_name(char **newval, void **extra, GucSource source)
{
	if (*newval != NULL && (*newval)[0] != '\0')
	{
		if (strlen(*newval) >= NAMEDATALEN)
		{
			GUC_check_errmsg("pgha.node_name must be lower than %d", NAMEDATALEN);
			return false;
		}
	}

	return true;
}

static bool
check_pgha_conninfo(char **newval, void **extra, GucSource source)
{
	if (*newval != NULL && (*newval)[0] != '\0')
	{
		if (strlen(*newval) >= NAMEDATALEN)
		{
			GUC_check_errmsg("pgha.my_conninfo must be lower than %d", MAXPGPATH);
			return false;
		}
	}

	return true;
}

static bool
addNode(const char *name, const char *conninfo, bool myself,
		bool dup_ok)
{
	int i;
	PgHaNode *new_node;

	LWLockAcquire(PgHaCtl->lock, LW_EXCLUSIVE);

	/* Check uniques */
	for (i = 0; i < pgha_max_nodes; i++)
	{
		PgHaNode *node = &PgHaCtl->nodes[i];

		if (node->in_use && strcmp(node->name, name) == 0)
		{
			if (!dup_ok)
			{
				/* return false if there is a duplication */
				ereport(ERROR, (errmsg("duplicate node name \"%s\"", name)));
				return false;
			}
			else
				/* Since we accept a duplication, return true and do nothing */
				return true;
		}
	}

	new_node = &PgHaCtl->nodes[PgHaCtl->n_nodes];

	strcpy(new_node->name, name);
	strcpy(new_node->conninfo, conninfo);
	new_node->in_use = true;
	new_node->myself = myself;
	new_node->type = am_master() ? 'm' : 's';
	new_node->is_sync = false;
	new_node->retry_count = 0;

	PgHaCtl->n_nodes++;

	LWLockRelease(PgHaCtl->lock);

	debug_show();
	
	return true;
}

static bool
delNode(const char *name)
{
	int i;
	PgHaNode *node = NULL;
	bool found = false;

	LWLockAcquire(PgHaCtl->lock, LW_EXCLUSIVE);

	for (i = 0; i < pgha_max_nodes; i++)
	{
		node = &PgHaCtl->nodes[i];

		if (node->in_use && strcmp(node->name, name) == 0)
		{
			found = true;
			break;
		}
	}

	if (!found)
	{
		ereport(ERROR, (errmsg("didn't find given name node \"%s\"", name)));
		LWLockRelease(PgHaCtl->lock);
		return false;
	}

	/* Move tail node to deleting node position */
	memcpy(node, &PgHaCtl->nodes[PgHaCtl->n_nodes], sizeof(PgHaNode));
	memset(&PgHaCtl->nodes[PgHaCtl->n_nodes], 0 , sizeof(PgHaNode));
	PgHaCtl->n_nodes--;

	LWLockRelease(PgHaCtl->lock);

	debug_show();

	return true;
}

static void
debug_show(void)
{
	int i;

	LWLockAcquire(PgHaCtl->lock, LW_SHARED);

	for (i = 0; i < pgha_max_nodes; i++)
	{
		PgHaNode *n = &PgHaCtl->nodes[i];

		if (!n->in_use)
			continue;

		elog(WARNING, "[%d] name = \"%s\", conn = \"%s\", type = \'%c\'",
			 i, n->name, n->conninfo, n->type);
	}

	LWLockRelease(PgHaCtl->lock);
}

Datum
add_node(PG_FUNCTION_ARGS)
{
	text *name = PG_GETARG_TEXT_P(0);
	text *conninfo = PG_GETARG_TEXT_P(1);
	bool ret;

	ret = addNode(text_to_cstring(name), text_to_cstring(conninfo), false,
				  false);

	PG_RETURN_BOOL(ret);
}

Datum
del_node(PG_FUNCTION_ARGS)
{
	text *name = PG_GETARG_TEXT_P(0);
	bool ret;

	ret = delNode(text_to_cstring(name));

	PG_RETURN_BOOL(ret);
}

Datum
join_node(PG_FUNCTION_ARGS)
{
#define RETURN_COLS 2

	text *name = PG_GETARG_TEXT_P(0);
	text *conninfo = PG_GETARG_TEXT_P(1);
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	MemoryContext oldcontext;
	Tuplestorestate *tupstore;
	int i;

	if (!am_master())
		ereport(ERROR, (errmsg("join_node can be executed only on master node")));

	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize) ||
		rsinfo->expectedDesc == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not allowed in this context")));

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	/* Build tuplestore to hold the result rows */
	oldcontext = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	/*
	 * Aadd a given node. Since re-started node might try to join again
	 * this adding node ignore a duplication.
	 */
	addNode(text_to_cstring(name), text_to_cstring(conninfo), false, true);
	
	LWLockAcquire(PgHaCtl->lock, LW_SHARED);
	
	for (i = 0; i < pgha_max_nodes; i++)
	{
		PgHaNode *node = &PgHaCtl->nodes[i];
		Datum		values[RETURN_COLS];
		bool		nulls[RETURN_COLS];

		if (!node->in_use)
			continue;
		
		values[0] = CStringGetTextDatum(node->name);
		values[1] = CStringGetTextDatum(node->conninfo);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}
	
	LWLockRelease(PgHaCtl->lock);
	tuplestore_donestoring(tupstore);
	MemoryContextSwitchTo(oldcontext);

	return (Datum) 0;
}

static
bool PgHaMasterLoop(void)
{
	ereport(LOG, (errmsg("pgha : entered master mode")));

	while (!got_sigterm)
	{
		int		rc;
		int		n_nodes;

		/*
		 * Background workers mustn't call usleep() or any direct equivalent:
		 * instead, they may wait on their process latch, which sleeps as
		 * necessary, but is awakened if postmaster dies.  That way the
		 * background process goes away immediately in an emergency.
		 */
		rc = WaitLatch(&MyProc->procLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   pgha_keepalives_time * 1000L, PG_WAIT_EXTENSION);
		ResetLatch(&MyProc->procLatch);

		/* Emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
			return false;

		/* If got SIGHUP, reload the configuration file */
		if (got_sighup)
		{
			got_sighup = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		/* get number of registered nodes. Always more than 1. */
		n_nodes = get_hanode_count();

		if (n_nodes > 1)
		{
			/* heartbeat to all nodes */
			doHeartbeat();

			/*
			 * Check current cluster status if any of retry count
			 * exceeded threshold.
			 */
			if (in_syncrep && !checkClusterStatus())
				changeToAsync();
		}
	}

	return true;
}

static bool
PgHaStandbyLoop(void)
{
	return true;
}

static void
doHeartbeat(void)
{
	int i;

	LWLockAcquire(PgHaCtl->lock, LW_SHARED);

	for (i = 0; i < pgha_max_nodes; i++)
	{
		PgHaNode *node = &PgHaCtl->nodes[i];
		bool	ret;

		if (!node->in_use || node->myself)
			continue;

		/* If I'm a slave, not interested in ping to other slaves */
		if (!am_master() && node->type == 's')
			continue;
			
		/* I'm master server, so ingnore myself */
		if (!node->in_use || node->myself)
			continue;

		/* Send ping */
		ret = execSQL(node->conninfo, "SELECT 1");

		SpinLockAcquire(&node->mutex);
		if (!ret)
			node->retry_count++;
		else
			node->retry_count = 0;
		SpinLockRelease(&node->mutex);
	}

	LWLockRelease(PgHaCtl->lock);
}

static void
changeToAsync(void)
{
	int ret;

	ereport(LOG, (errmsg("pgha: changes replication mode to asynchronous replication")));

	if (!execSQL(pgha_my_conninfo, "ALTER SYSTEM SET synchronous_standby_names TO ''"))
		ereport(ERROR, (errmsg("pgha: fialed to change replication mode")));

	if ((ret = kill(PostmasterPid, SIGHUP)) != 0)
		ereport(ERROR, (errmsg("pgha: failed to send SIGHUP to postmaster")));
}

static bool
checkClusterStatus(void)
{
	return true;
}

static int
get_hanode_count(void)
{
	int count;

	LWLockAcquire(PgHaCtl->lock, LW_SHARED);
	count = PgHaCtl->n_nodes;
	LWLockRelease(PgHaCtl->lock);

	return count;
}

bool
execSQL(const char *conninfo, const char *sql)
{
	PGconn		*con;
	PGresult 	*res;

	/* Try to connect to primary server */
	if ((con = PQconnectdb(conninfo)) == NULL)
	{
		ereport(LOG,
				(errmsg("could not establish conenction to server : \"%s\"",
					conninfo)));

		PQfinish(con);
		return false;
	}

	res = PQexec(con, sql);

	if (PQresultStatus(res) != PGRES_TUPLES_OK &&
		PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		/* Failed to ping to master server, report the number of retrying */
		ereport(LOG,
				(errmsg("could not get tuple from server : \"%s\"",
					conninfo)));

		PQfinish(con);
		return false;
	}

	PQfinish(con);
	return true;
}
