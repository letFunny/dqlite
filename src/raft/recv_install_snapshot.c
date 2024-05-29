#include "recv_install_snapshot.h"

#include "../tracing.h"
#include "assert.h"
#include "convert.h"
#include "flags.h"
#include "log.h"
#include "recv.h"
#include "replication.h"

#include "../lib/sm.h"
#include "src/raft.h"
#include "src/raft/recv_install_snapshot.h"

/**
 * =Overview
 *
 * This detailed level design is based on PL018 and describes
 * significant implementation details of data structures, RPCs
 * introduced in it; provides model of operation and failure handling
 * based on Leader's and Follower's states.
 *
 * =Data structures
 *
 * Among other structures it's needed to introduce a (persistent)
 * container `HT` to efficiently store and map checksums to their page
 * numbers on the follower's side. HT is implemented on top of sqlite3
 * database with unix VFS. Every database corresponds to a
 * raft-related database and maintains the following schema:
 *
 * CREATE TABLE "map" ("checksum" INTEGER NOT NULL, "pageno" INTEGER NOT NULL)
 * CREATE INDEX map_idx on map(checksum);
 *
 * Each database stores a mapping from checksum to page number. This
 * provides an efficient way to insert and lookup records
 * corresponding to the checksums and page numbers.
 */

/**
 * =Operation
 *
 * 0. Leader creates one state machine per Follower to track of their
 * states and moves it to FOLLOWER_ONLINE state. Follower creates a
 * state machine to track of its states and moves it to NORMAL state.
 *
 * 1. The Leader learns the Follower’s follower.lastLogIndex during
 * receiving replies on AppendEntries() RPC, fails to find
 * follower.lastLogIndex in its RAFT log or tries and fails to
 * construct an AppendEntries() message because of the WALs that
 * contained some necessary frames has been rotated out, and
 * understands that the snapshot installation procedure is
 * required.
 *
 * Leader calls leader_tick() putting struct raft_message as a
 * parameter which logic moves it from FOLLOWER_ONLINE to
 * FOLLOWER_NEEDS_SNAPSHOT state.
 *
 * 2. The Leader initiates snapshot installation by sending
 * InstallSnapshot() message.
 *
 * Upon receiving this message on the Follower's side, Follower calls
 * follower_tick() putting struct raft_message as a parameter which
 * logic moves it from NORMAL to SIGNATURES_CALC_STARTED state.
 *
 * 3. The Follower calculates [page_checksum_t] for its local
 * persistent state. When this process ends it puts itself into
 * SIGNATURES_CALC_DONE state by calling follower_tick(..., NULL).
 *
 * 4. The Follower sends Signature() messages to the Leader, transfers
 * the whole [page_checksum_t] and puts itself into
 * SIGNATURES_PART_SENT state. When all signatures transferred the
 * Leader, it sends Signature(done=true) message.
 *
 * 5. When the Leader received the first Signature() message it moves
 * corresponding state machine into SIGNATURES_CALC_STARTED state and
 * creates HT. The Leader puts incomming payloads of Signature()
 * message into the HT. When the Leader received Signature(done=true)
 * message it moves into SNAPSHOT_INSTALLATION_STARTED state.
 *
 * 6. In SNAPSHOT_INSTALLATION_STARTED state, the Leader starts
 * iterating over the local persistent state, and calculates the
 * checksum for each page the state has. Then, it tries to find the
 * checksum it calculated in HT. Based on the result of this
 * calculation, the Leader sends InstallShapshot(CP..) or
 * InstallShapshot(MV..) to the Follower.
 *
 * Upon receving these messages the Follower moves into
 * SNAPSHOT_CHUNCK_RECEIVED state. The Leader moves into
 * SNAPSHOT_CHUNCK_SENT state after receiving first reply from the
 * Follower.
 *
 * 7. When the iteration has finished the Leader sends
 * InstallShapshot(..., done=true) message to the Follower. It moves
 * the Follower back to NORMAL state and the state machine
 * corresponding to the Follower on the Leader is moved to
 * SNAPSHOT_DONE_SENT state.
 *
 * 8. The Leader sends AppendEntries() RPC to the Follower and
 * restarts the algorithm from (1). The Leader's state machine is
 * being moved to FOLLOWER_ONLINE state.
 *
 * =Failure model
 *
 * ==Unavailability of the Leader and Follower.
 *
 * To handle use-cases when any party of the communication becomes
 * unavailable for a while without crash the following assumtions are
 * made:
 *
 * - Signature() or InstallSnapshot(MV/CP) messages are idempotent and
 *   can be applied to the persistent state many times resulting the
 *   same transition.
 *
 * - Each message with data chuncks has an information about the
 *   "chunk index". Chunk indexes come in monotonically increasing
 *   order.
 *
 * - Each reply message acknowledges that the data received (or
 *   ignored) by sending `result` field back to the counter part along
 *   with last known chunk index as a confirmation that the receiver
 *   "knows everything up to the given chunck index".
 *
 * - If a party notices that last known chunk index sent back to it
 *   doesn't match it's own, the communication get's restarted from
 *   the lowest known index.
 *
 * ==Crashes of the Leader and Follower.
 *
 * @TODO: We'd need to review other options here.
 *
 * The Follower maintains persistent bootcounter variable incremented
 * every time during process start and sends it back to the Leader as
 * a part of heart bit in AppendEntriesResult() message.
 *
 * Bootcounters are being used to restart snapshot installation
 * procedures crashed in the middle of their execution on the Leaders'
 * and Follower's sides.  When Leader notices that bootcounter changed
 * it restarts snapshot installation and moves its state machine to
 * FOLLOWER_ONLINE state, starting over the scenario described in
 * "Operation" section.
 *
 * =State model
 *
 * Definitions:
 *
 * Rf -- raft index sent in AppendEntriesResult() from Follower to Leader
 * Bf -- bootcounter sent in AppendEntriesResult() from Follower to Leader
 * Tf -- Follower's term sent in AppendEntriesResult() from Follower to Leader
 *
 * Tl -- Leader's term
 * Rl -- raft index of the Leader
 * Bl -- bootcounter of the Follower_i saved in Leader's volatile state
 *
 * Leader's state machine:
 *
 *					AppendEntriesResult() received
 *					raft_log.find(Rf) == "FOUND"
 *                                         -----------+
 * +---------------------> FOLLOWER_ONLINE <----------+
 * |  AppendEntriesResult()      |
 * | received, Bf>Bl && Rf<<Rl   | AppendEntriesResult() received
 * | raft_log.find(Rf) == ENOENT V Rf << Rl && raft_log.find(Rf) == "ENOENTRY"
 * | 	+----------> FOLLOWER_NEEDS_SNAPSHOT
 * | 	|			 |
 * | 	|			 | InstallSnapshotResult() received
 * | 	|			 V
 * | 	+----------- FOLLOWER_WAS_NOTIFIED
 * | 	|			 |
 * | 	|			 | Signature() received
 * | 	|			 V
 * | 	+---------- SIGNATURES_CALC_STARTED  -------+ SignatureResult() received
 * | 	|			 |           <------+
 * | 	|			 | Signature(done=true) received
 * | 	|			 V
 * | 	+--------- SNAPSHOT_INSTALLATION_STARTED
 * | 	|			 |
 * | 	|			 | CP_Result()/MV_Result() received
 * | 	|			 V
 * | 	+------------- SNAPSHOT_CHUNCK_SENT -------+ CP_Result()/MV_Result()
 * | 	|			 |          <------+       received
 * | 	|			 | Raw snapshot iteration done
 * | 	|			 V
 * | 	+ ------------ SNAPSHOT_DONE_SENT
 * | 				 | InstallSnapshotResult() received
 * +-----------------------------+
 *
 * Follower's state machine:
 *
 *	+-------------------> NORMAL
 *      |     	+----------->   |
 *	|   (@)	|		| InstallSnapshot() received
 *	|	|		V
 *	|	+--- SIGNATURES_CALC_STARTED
 *	|	|		|
 *	|	|		| Signatures for all db pages
 *	|	|		|    have been calculated.
 *	|	|		V
 *	|	+---- SIGNATURES_CALC_DONE
 *	|	|		|
 *	|	|		| First Signature() sent
 *	|	|		V
 *	|	+---- SIGNATURES_PART_SENT ---------+ SignatureResult() received
 *	|	|		|          <--------+ Signature() sent
 *	|	|		| CP()/MV() received
 *	|	|		V
 *	|	+--- SNAPSHOT_CHUNCK_RECEIVED -------+ CP()/MV() received
 *	|			|             <------+
 *	|			| InstallSnapshot(done=true) received
 *	+-----------------------+
 *
 * (@) -- AppendEntries() received && Tf < Tl
 */

enum follower_states {
	FS_NORMAL,
	FS_SIGNATURES_CALC_STARTED,
	FS_SIGNATURES_CALC_DONE,
	FS_SIGNATURES_PART_SENT,
	FS_SNAPSHOT_CHUNCK_RECEIVED,
	FS_NR,
};

static const struct sm_conf follower_states[FS_NR] = {
	[FS_NORMAL] = {
		.flags = SM_INITIAL | SM_FINAL,
		.name = "normal",
		.allowed = BITS(FS_SIGNATURES_CALC_STARTED),
	},
/*	[PS_DRAINING] = {
	    .name = "draining",
	    .allowed = BITS(PS_DRAINING)
		     | BITS(PS_NOTHING)
		     | BITS(PS_BARRIER),
	},
*/
};

__attribute__((unused)) void follower_tick(struct sm *follower,
					   const struct raft_message *msg)
{
	(void)follower;
	(void)msg;
	(void)follower_states;
	// switch (sm_state(follower)) {
	// }
}

__attribute__((unused)) static bool follower_invariant(const struct sm *m,
						       int prev_state)
{
	(void)m;
	(void)prev_state;
	// pool_impl_t *pi = CONTAINER_OF(m, pool_impl_t, planner_sm);
	return true;
}

enum leader_states {
	LS_FOLLOWER_ONLINE,
	LS_FOLLOWER_NEEDS_SNAPSHOT,
	LS_FOLLOWER_WAS_NOTIFIED,
	LS_SIGNATURES_CALC_STARTED,
	LS_SNAPSHOT_INSTALLATION_STARTED,
	LS_SNAPSHOT_CHUNCK_SENT,
	LS_SNAPSHOT_DONE_SENT,
	LS_NR,
};

static const struct sm_conf leader_states[LS_NR] = {
	[LS_FOLLOWER_ONLINE] = {
		.flags = SM_INITIAL | SM_FINAL,
		.name = "online",
		.allowed = BITS(LS_FOLLOWER_NEEDS_SNAPSHOT),
	},
	[LS_FOLLOWER_NEEDS_SNAPSHOT] = {
		.flags = 0,
		.name = "needs_snapshot",
		.allowed = BITS(LS_FOLLOWER_WAS_NOTIFIED),
	},
	[LS_FOLLOWER_WAS_NOTIFIED] = {
		.flags = 0,
		.name = "follower_notified",
		.allowed = BITS(LS_SIGNATURES_CALC_STARTED)|BITS(LS_FOLLOWER_NEEDS_SNAPSHOT),
	},
	[LS_SIGNATURES_CALC_STARTED] = {
		.flags = 0,
		.name = "signature_calc_started",
		.allowed = BITS(LS_SNAPSHOT_INSTALLATION_STARTED)|BITS(LS_FOLLOWER_NEEDS_SNAPSHOT),
	},
	[LS_SNAPSHOT_INSTALLATION_STARTED] = {
		.flags = 0,
		.name = "installation_started",
		.allowed = BITS(LS_SNAPSHOT_CHUNCK_SENT)|BITS(LS_FOLLOWER_NEEDS_SNAPSHOT),
	},
	[LS_SNAPSHOT_CHUNCK_SENT] = {
		.flags = 0,
		.name = "chuck_sent",
		.allowed = BITS(LS_SNAPSHOT_DONE_SENT)|BITS(LS_FOLLOWER_NEEDS_SNAPSHOT),
	},
	[LS_SNAPSHOT_DONE_SENT] = {
		.flags = 0,
		.name = "done_send",
		.allowed = BITS(LS_FOLLOWER_NEEDS_SNAPSHOT),
	},
};

static bool log_index_not_found(struct raft *r, raft_index follower_index)
{
	return follower_index < logLastIndex(r->log) &&
	       logGet(r->log, follower_index) == NULL;
}

struct insert_checksum_data {
	sqlite3_stmt *stmt;
	struct page_checksum_t *cs;
	unsigned int cs_nr;
};

static int insert_checksums(struct raft_io_async_work *req)
{
	int rv;

	struct insert_checksum_data *data = (struct insert_checksum_data *)req;
	for (unsigned int i = 0; i < data->cs_nr; i++) {
		rv = sqlite3_bind_int(data->stmt, 0, (int)data->cs[i].checksum);
		if (rv != SQLITE_OK) {
			return rv;
		}
		rv = sqlite3_bind_int(data->stmt, 1, (int)data->cs[i].page_no);
		if (rv != SQLITE_OK) {
			return rv;
		}
		rv = sqlite3_step(data->stmt);
		if (rv != SQLITE_OK) {
			return rv;
		}
		rv = sqlite3_reset(data->stmt);
		if (rv != SQLITE_OK) {
			return rv;
		}
	}
	return SQLITE_OK;
}

static void async_insert_checksums(struct snapshot_state *state,
				   struct page_checksum_t *cs,
				   unsigned int cs_nr)
{
	struct insert_checksum_data data = {
		.stmt = state->stmt,
		.cs = cs,
		.cs_nr = cs_nr,
	};
	struct raft_io_async_work work = {
		.work = insert_checksums,
		.data = &data,
	};
	state->r->io->async_work(state->r->io, &work, NULL);
}

static int create_ht_db_and_stmt(struct raft_io_async_work *req)
{
	char db_filename[30];
	char *err_msg;
	int rv;

	struct snapshot_state *state = (struct snapshot_state *)req->data;
	sprintf(db_filename, "ht-%lld", state->follower_id);
	rv = sqlite3_open_v2(db_filename, &state->db, 0, "unix");
	assert(rv == SQLITE_OK);
	rv = sqlite3_exec(state->db,
			  "CREATE TABLE map (checksum INTEGER NOT NULL, pageno "
			  "INTEGER NOT NULL UNIQUE);",
			  NULL, NULL, &err_msg);
	assert(rv == SQLITE_OK);
	rv = sqlite3_exec(state->db, "CREATE INDEX map_idx on map(checksum);",
			  NULL, NULL, &err_msg);
	assert(rv == SQLITE_OK);
	rv = sqlite3_prepare_v2(state->db,
				"INSERT OR IGNORE INTO map VALUES (?, ?);", -1,
				&state->stmt, NULL);
	assert(rv == SQLITE_OK);
	return SQLITE_OK;
}

static void async_create_ht_db_and_stmt(struct snapshot_state *state)
{
	struct raft_io_async_work work = {
		.work = create_ht_db_and_stmt,
		.data = state,
	};
	state->r->io->async_work(state->r->io, &work, NULL);
}

bool is_main_thread(void)
{
	// TODO: thread local storage.
	return true;
}

void leader_tick(struct sm *leader, const struct raft_message *msg)
{
	(void)leader_states;

	assert(leader != NULL);
	assert(msg != NULL);

	struct snapshot_state *snapshot_state =
		CONTAINER_OF(leader, struct snapshot_state, sm);
	struct raft *r = snapshot_state->r;
	assert(snapshot_state != NULL && r != NULL);

	PRE(is_main_thread());
	// TODO think about callsite of this function (triggers: e.g. message
	// recv, timeouts).
	switch (sm_state(leader)) {
	case LS_FOLLOWER_ONLINE:
		switch (msg->type) {
		case RAFT_IO_APPEND_ENTRIES:
			raft_index follower_index =
				msg->append_entries_result.last_log_index;
			// assert(follower_index >= 0);
			// Follower needs an entry which is not
			// on the Raft log anymore.
			if (log_index_not_found(r, follower_index)) {
				sm_move(leader, LS_FOLLOWER_NEEDS_SNAPSHOT);
				// sendInstallSnapshot();
				// TODO: counter tracking in
				// receiver and sender for pages
				// to resend pages.
			} else {
				sm_move(leader, LS_FOLLOWER_ONLINE);
			}
		}
		// TODO check that entry is in wal (I need the `entry` struct here to
		// access the field). mxFrame: number of valid and committed frames in
		// the WAL. nPage: size of the database in pages. nBackfill: Number of
		// WAL frames that have already been backfilled into the database by
		// prior checkpoints }
		break;
	case LS_FOLLOWER_WAS_NOTIFIED:
		switch (msg->type) {
		case RAFT_IO_SIGNATURE:
			PRE(snapshot_state->db == NULL &&
			    snapshot_state->stmt == NULL);

			// TODO probably we need to use callbacks here to send the messages
			// after inserting in SQLite.
			async_create_ht_db_and_stmt(snapshot_state);
			async_insert_checksums(snapshot_state,
					       msg->signature.cs,
					       msg->signature.cs_nr);
			sm_move(leader, LS_SIGNATURES_CALC_STARTED);
			POST(snapshot_state->db != NULL &&
			     snapshot_state->stmt != NULL);
			break;
		}
		break;
	case LS_SIGNATURES_CALC_STARTED:
		switch (msg->type) {
		case RAFT_IO_SIGNATURE:
			PRE(snapshot_state->db != NULL &&
			    snapshot_state->stmt != NULL);

			async_insert_checksums(snapshot_state,
					       msg->signature.cs,
					       msg->signature.cs_nr);
			// sendSignatureResponse();
			break;
		}
		break;
	}
}

int index_snapshot_sm(struct raft_leader_state *leader_state, raft_id id)
{
	for (int i = 0; i < (int)leader_state->n_snapshot_sms; i++) {
		if (leader_state->snapshot_sms[i].id == id) {
			return i;
		}
	}
	return -1;
}

__attribute__((unused)) static bool leader_invariant(const struct sm *sm,
						     int prev_state)
{
	bool rv;
	// TODO if we need msg pointer it is better to store it in the
	// state thanto pass it.
	// TODO What happens when nodes join the cluster.

	struct snapshot_state *state =
		CONTAINER_OF(sm, struct snapshot_state, sm);

	// State transitions.
	rv = CHECK(ERGO(sm_state(sm) == LS_FOLLOWER_WAS_NOTIFIED,
		   prev_state == LS_FOLLOWER_NEEDS_SNAPSHOT) &&
	      ERGO(sm_state(sm) == LS_FOLLOWER_WAS_NOTIFIED,
		   prev_state == LS_FOLLOWER_NEEDS_SNAPSHOT) &&
	      ERGO(sm_state(sm) == LS_SIGNATURES_CALC_STARTED,
		   prev_state == LS_FOLLOWER_WAS_NOTIFIED ||
			   prev_state == LS_SIGNATURES_CALC_STARTED) &&
	      ERGO(sm_state(sm) == LS_SNAPSHOT_INSTALLATION_STARTED,
		   prev_state == LS_SIGNATURES_CALC_STARTED) &&
	      ERGO(sm_state(sm) == LS_SNAPSHOT_CHUNCK_SENT,
		   prev_state == LS_SNAPSHOT_INSTALLATION_STARTED) &&
	      ERGO(sm_state(sm) == LS_SNAPSHOT_DONE_SENT,
		   prev_state == LS_SNAPSHOT_CHUNCK_SENT) &&
	      ERGO(sm_state(sm) == LS_FOLLOWER_ONLINE,
		   prev_state == LS_SNAPSHOT_DONE_SENT ||
			   prev_state == LS_SNAPSHOT_DONE_SENT));
	if (!rv) {
		return false;
	}

	if (sm_state(sm) == LS_SIGNATURES_CALC_STARTED ||
	    sm_state(sm) == LS_SNAPSHOT_INSTALLATION_STARTED ||
	    sm_state(sm) == LS_SNAPSHOT_CHUNCK_SENT) {
		rv = CHECK(state->db != NULL && state->stmt != NULL);
		if (!rv) {
			return false;
		}
	} else {
		rv = CHECK(state->db == NULL && state->stmt == NULL);
		if (!rv) {
			return false;
		}
	}
	return true;
}

static void installSnapshotSendCb(struct raft_io_send *req, int status)
{
	(void)status;
	raft_free(req);
}

int recvInstallSnapshot(struct raft *r,
			const raft_id id,
			const char *address,
			struct raft_install_snapshot *args)
{
	struct raft_io_send *req;
	struct raft_message message;
	struct raft_append_entries_result *result =
	    &message.append_entries_result;
	int rv;
	int match;
	bool async;

	assert(address != NULL);
	tracef(
	    "self:%llu from:%llu@%s conf_index:%llu last_index:%llu "
	    "last_term:%llu "
	    "term:%llu",
	    r->id, id, address, args->conf_index, args->last_index,
	    args->last_term, args->term);

	result->rejected = args->last_index;
	result->last_log_index = logLastIndex(r->log);
	result->version = RAFT_APPEND_ENTRIES_RESULT_VERSION;
	result->features = RAFT_DEFAULT_FEATURE_FLAGS;

	rv = recvEnsureMatchingTerms(r, args->term, &match);
	if (rv != 0) {
		return rv;
	}

	if (match < 0) {
		tracef("local term is higher -> reject ");
		goto reply;
	}

	/* TODO: this logic duplicates the one in the AppendEntries handler */
	assert(r->state == RAFT_FOLLOWER || r->state == RAFT_CANDIDATE);
	assert(r->current_term == args->term);
	if (r->state == RAFT_CANDIDATE) {
		assert(match == 0);
		tracef("discovered leader -> step down ");
		convertToFollower(r);
	}

	rv = recvUpdateLeader(r, id, address);
	if (rv != 0) {
		return rv;
	}
	r->election_timer_start = r->io->time(r->io);

	rv = replicationInstallSnapshot(r, args, &result->rejected, &async);
	if (rv != 0) {
		tracef("replicationInstallSnapshot failed %d", rv);
		return rv;
	}

	if (async) {
		return 0;
	}

	if (result->rejected == 0) {
		/* Echo back to the leader the point that we reached. */
		result->last_log_index = args->last_index;
	}

reply:
	result->term = r->current_term;

	/* Free the snapshot data. */
	raft_configuration_close(&args->conf);
	raft_free(args->data.base);

	message.type = RAFT_IO_APPEND_ENTRIES_RESULT;
	message.server_id = id;
	message.server_address = address;

	req = raft_malloc(sizeof *req);
	if (req == NULL) {
		return RAFT_NOMEM;
	}
	req->data = r;

	rv = r->io->send(r->io, req, &message, installSnapshotSendCb);
	if (rv != 0) {
		raft_free(req);
		return rv;
	}

	return 0;
}

#undef tracef
