#include "recv_install_snapshot.h"
#include <assert.h>
#include <stdint.h>
#include <stdio.h>

#include "../tracing.h"
#include "assert.h"
#include "convert.h"
#include "flags.h"
#include "log.h"
#include "recv.h"
#include "replication.h"

#include "../lib/sm.h"
#include "../raft.h"
#include "../raft/recv_install_snapshot.h"
#include "../raft/uv_os.h"

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
 * Among other structures it's needed to introduce a (persistent) container
 * `HT` to efficiently store and map checksums to their page numbers on both
 * the leader's and  follower's side. HT is implemented on top of sqlite3
 * database with unix VFS. Every database corresponds to a raft-related
 * database and maintains the following schema:
 *
 * CREATE TABLE "map" ("checksum" INTEGER NOT NULL, "pageno" INTEGER NOT NULL UNIQUE)
 * CREATE INDEX map_idx on map(checksum);
 *
 * Each database stores a mapping from checksum to page number. This
 * provides an efficient way to insert and lookup records
 * corresponding to the checksums and page numbers.
 */

/**
 * =Operation
 *
 * 0. Leader creates one state machine per Follower to track of their states
 * and moves it to FOLLOWER_ONLINE state. Follower creates a state machine to
 * keep track of its states and moves it to NORMAL state.
 *
 * 1. The Leader learns the Follower’s follower.lastLogIndex during receiving
 * replies on AppendEntries() RPC, fails to find follower.lastLogIndex in its
 * RAFT log or tries and fails to construct an AppendEntries() message because
 * of the WAL that contained some necessary frames has been rotated out, and
 * understands that the snapshot installation procedure is required.
 *
 * Leader calls leader_tick() putting struct raft_message as a parameter which
 * logic moves it from FOLLOWER_ONLINE to FOLLOWER_NEEDS_SNAPSHOT state.
 *
 * 2. The Leader initiates the snapshot installation by sending
 * InstallSnapshot() message.
 *
 * 3. Upon receiving this message on the Follower's side, Follower calls
 * follower_tick() putting struct raft_message as a parameter which logic moves
 * it from NORMAL to SIGNATURES_CALC_STARTED state. The Follower then creates
 * its HT and starts calculating checksums and recording them. Once finished it
 * sends the leader the InstallSnapshotResult() message and the Leader moves to
 * SIGNATURES_CALC_STARTED and creates its HT.
 *
 * 3. The Leader sends Signature() messages to the Follower containing the page
 * range for which we want to get the checksums.
 *
 * The Follower sends the requested checksums in a SignatureResult() message
 * back to the Leader and the leader puts incomming payloads of Signature()
 * message into the HT.
 *
 * 4. When the follower sends the checksum of its highest numbered page to the
 * Leader, it sends the SignatureResult() message using the done=true flag,
 * upon receiving it the Leader moves into SNAPSHOT_INSTALLATION_STARTED state.
 *
 * 5. In SNAPSHOT_INSTALLATION_STARTED state, the Leader starts iterating over
 * the local persistent state, and calculates the checksum for each page the
 * state has. Then, it tries to find the checksum it calculated in HT. Based on
 * the result of this calculation, the Leader sends InstallShapshot(CP..) or
 * InstallShapshot(MV..) to the Follower.
 *
 * Upon receving these messages, the Follower moves into
 * SNAPSHOT_CHUNCK_RECEIVED state. The Leader moves into SNAPSHOT_CHUNCK_SENT
 * state after receiving first reply from the Follower.
 *
 * 6. When the iteration has finished the Leader sends
 * InstallShapshot(..., done=true) message to the Follower. It moves the
 * Follower back to NORMAL state and the state machine corresponding to the
 * Follower on the Leader is moved to SNAPSHOT_DONE_SENT state.
 *
 * 7. The Leader sends AppendEntries() RPC to the Follower and restarts the
 * algorithm from (1). The Leader's state machine is being moved to
 * FOLLOWER_ONLINE state.
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
 * If a reply is not received the Leader will eventually timeout and retry
 * sending the same message.
 *
 * ==Crashes of the Leader and Follower.
 *
 * Crashes of the Leader are handled by Raft when a new leader is elected
 * and the snapshot process is restarted.
 *
 * If the Follower receives an message which is not expected in the Follower's
 * current state, the Follower will reply using the message's result RPC
 * setting the unexpected=true flag. This response suggests the Leader to
 * restart the snapshot installation procedure.
 *
 * In particular, if the follower crashes it will restart its state machine to
 * the NORMAL state and reply using the unexpected=true flag for any messages
 * not expected in the NORMAL state, suggesting the Leader to restart the
 * procedure.
 *
 * =State model
 *
 * Definitions:
 *
 * Rf -- raft index sent in AppendEntriesResult() from Follower to Leader
 * Tf -- Follower's term sent in AppendEntriesResult() from Follower to Leader
 *
 * Tl -- Leader's term
 * Rl -- raft index of the Leader
 *
  * Leader's state machine:
 *
 * +---------------------------------+
 * |                                 |     AppendEntriesResult() received
 * |        *Result(unexpected=true) |     raft_log.find(Rf) == "FOUND"
 * |        received                 |      +------------+
 * |        +---------------> FOLLOWER_ONLINE <----------+
 * |        |                        |
 * |        |                        | AppendEntriesResult() received
 * |        |                        V Rf << Rl && raft_log.find(Rf) == "ENOENTRY"
 * |        +------------- FOLLOWER_NEEDS_SNAPSHOT
 * |        |                        |
 * |        |                        | InstallSnapshotResult() received
 * |        |                        V
 * |        +------------ SIGNATURES_CALC_STARTED  -------+ SignatureResult() received
 * |        |                        |             <------+
 * |        |                        | SignatureResult(done=true) received
 * |        |                        V
 * |        +----------- SNAPSHOT_INSTALLATION_STARTED
 * |        |                        |
 * |        |                        | CP_Result()/MV_Result() received
 * |        |                        V
 * |        +--------------- SNAPSHOT_CHUNCK_SENT -------+ CP_Result()/MV_Result()
 * |        |                        |            <------+  received
 * |        |                        | Raw snapshot iteration done
 * |        |                        V
 * |        +--------------- SNAPSHOT_DONE_SENT
 * |                                 | InstallSnapshotResult() received
 * +---------------------------------+
 *
 * Follower's state machine:
 *
 *                            +------+ (%)
 * +-------------------> NORMAL <----+
 * |       +----------->   |
 * |       |               | InstallSnapshot() received
 * |       |               V
 * |       +--- SIGNATURES_CALC_STARTED
 * |       |               |
 * |       |               | Signatures for all db pages have been calculated.
 * |       |               | InstallSnapshotResult() sent.
 * |       |               V
 * |       +---- SIGNATURES_CALC_DONE
 * |       |               |
 * |       |               | First SignatureResult() sent
 * |       |               V
 * |       +---- SIGNATURES_PART_SENT ---------+ Signature() received
 * |       |               |          <--------+ SignatureResult() sent
 * |       |               |
 * |       |               | CP()/MV() received
 * |       |               V
 * |       +--- SNAPSHOT_CHUNCK_RECEIVED -------+ CP()/MV() received
 * |    (@ || %)           |             <------+
 * |                       | InstallSnapshot(done=true) received
 * +-----------------------+
 *
 * (@) -- AppendEntries() received && Tf < Tl
 * (%) -- Signature()/CP()/MV() received and in the current state receving a
 *        message of such type is unexpected. *Result(unexpected=true) sent.
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
		.allowed = BITS(FS_SIGNATURES_CALC_STARTED)|BITS(FS_NORMAL),
	},
	[FS_SIGNATURES_CALC_STARTED] = {
		.flags = 0,
		.name = "signatures_calc_started",
		.allowed = BITS(FS_SIGNATURES_CALC_DONE)|BITS(FS_NORMAL),
	},
	[FS_SIGNATURES_CALC_DONE] = {
		.flags = 0,
		.name = "signatures_calc_done",
		.allowed = BITS(FS_SIGNATURES_PART_SENT)|BITS(FS_NORMAL),
	},
	[FS_SIGNATURES_PART_SENT] = {
		.flags = 0,
		.name = "signatures_part_sent",
		.allowed = BITS(FS_SNAPSHOT_CHUNCK_RECEIVED)|BITS(FS_NORMAL),
	},
	[FS_SNAPSHOT_CHUNCK_RECEIVED] = {
		.flags = 0,
		.name = "snapshot_chunk_received",
		.allowed = BITS(FS_NORMAL),
	},
};

static bool is_main_thread(void)
{
	// TODO: thread local storage.
	return true;
}

static int create_ht_and_stmt(char *db_filename, sqlite3 **db, sqlite3_stmt **stmt)
{
	// TODO tracef with err_msg and tracef in general.
	char *err_msg;
	int rv;

	rv = UvOsUnlink(db_filename);
	assert(rv == 0 || errno == ENOENT);
	rv = sqlite3_open_v2(db_filename, db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "unix");
	assert(rv == SQLITE_OK);
	rv = sqlite3_exec(*db,
			  "CREATE TABLE map (checksum INTEGER NOT NULL, pageno "
			  "INTEGER NOT NULL UNIQUE);",
			  NULL, NULL, &err_msg);
	assert(rv == SQLITE_OK);
	rv = sqlite3_exec(*db, "CREATE INDEX map_idx on map(checksum);",
			  NULL, NULL, &err_msg);
	assert(rv == SQLITE_OK);
	rv = sqlite3_prepare_v2(*db,
				"INSERT OR IGNORE INTO map VALUES (?, ?);", -1,
				stmt,
				NULL);
	assert(rv == SQLITE_OK);

	return SQLITE_OK;
}

static int insert_checksum(sqlite3_stmt *stmt, checksum_t checksum, pageno_t page_no) {
	int rv;
	rv = sqlite3_bind_int(stmt, 1, (int)checksum);
	if (rv != SQLITE_OK) {
		return rv;
	}
	rv = sqlite3_bind_int(stmt, 2, (int)page_no);
	if (rv != SQLITE_OK) {
		return rv;
	}
	rv = sqlite3_step(stmt);
	if (rv != SQLITE_DONE) {
		return rv;
	}
	rv = sqlite3_reset(stmt);
	if (rv != SQLITE_OK) {
		return rv;
	}
	return SQLITE_OK;
}

struct send_install_snapshot_result_data {
	struct snapshot_follower_state *state;
	int next_state;
};

static void send_install_snapshot_result_cb(struct raft_io_send *req, int status) {
	struct send_install_snapshot_result_data *data = req->data;

	if (status == 0) {
		sm_move(&data->state->sm, data->next_state);
	}

	raft_free(req->data);
	raft_free(req);
	return;
}

static void async_send_install_snapshot_result(struct snapshot_follower_state *state, int next_state) {
	struct send_install_snapshot_result_data *data;
	data = raft_malloc(sizeof *data);
	assert(data != NULL);
	*data = (struct send_install_snapshot_result_data) {
		.state = state,
		.next_state = next_state,
	};

	struct raft_io_send *reply_req;
	reply_req = raft_malloc(sizeof *reply_req);
	assert(reply_req != NULL);
	*reply_req = (struct raft_io_send) {
		.data = data,
		.cb = send_install_snapshot_result_cb,
	};

	struct raft_message *reply;
	reply = raft_malloc(sizeof *reply);
	assert(reply != NULL);
	// TODO: proper message.
	*reply = (struct raft_message) {
		.type = RAFT_IO_INSTALL_SNAPSHOT_RESULT,
		.server_id = state->id,
	};

	state->io->async_send_message(reply_req, reply, send_install_snapshot_result_cb);
}

static void calculate_and_insert_signatures_cb(struct raft_io_async_work *req, int status) {
	(void)status;

	struct snapshot_follower_state *state = req->data;

	async_send_install_snapshot_result(state, FS_SIGNATURES_CALC_DONE);
}

static checksum_t checksum_chunk(uint8_t *chunk, uint64_t chunk_size) {
	// TODO: Use blake2.
	(void)chunk_size;
	checksum_t checksum = { 0 };
	uint8_t *checksum_bytes = (uint8_t *)&checksum;
	checksum_bytes[0] = chunk[0];
	return checksum;
}

static int calculate_and_insert_signatures(struct raft_io_async_work *req) {
	int rv;
	sqlite3_stmt *ht_insert_stmt;
	struct snapshot_follower_state *state = req->data;

	char *db_filename = "ht";

	rv = create_ht_and_stmt(db_filename, &state->ht, &ht_insert_stmt);
	assert(rv == SQLITE_OK);

	rv = sqlite3_prepare_v2(state->ht,
				"SELECT * FROM map WHERE pageno <= ? AND pageno >= ?;", -1,
				&state->ht_select_stmt, NULL);
	assert(rv == SQLITE_OK);

	uint64_t offset = 0;
	/* chunk_size is size in number of pages. */
	uint64_t chunk_size = 1;
	uint8_t *chunk = raft_malloc(chunk_size);
	assert(chunk != NULL);
	pageno_t page_no = 0;
	while ((rv = state->io->read_chunk(offset, 1, chunk)) == 0) {
		rv = insert_checksum(ht_insert_stmt, checksum_chunk(chunk, chunk_size), page_no);
		// TODO: do not use asserts.
		assert(rv == SQLITE_OK);
		page_no += 1;
		// TODO: page size properly.
		offset = page_no * 1024;
	}
	state->last_page = page_no - 1;

	sqlite3_finalize(ht_insert_stmt);
	raft_free(chunk);
	return rv == EOF ? 0 : -1;
}

static void async_calculate_signatures(struct snapshot_follower_state *state) {
	struct raft_io_async_work *work;
	work = raft_malloc(sizeof(*work));
	assert(work != NULL);
	*work = (struct raft_io_async_work) {
		.work = calculate_and_insert_signatures,
		.data = state,
	};

	state->io->async_work(work, calculate_and_insert_signatures_cb);
}

struct send_signature_result_data {
	struct snapshot_follower_state *state;
	int next_state;
};

static void send_signature_result_cb(struct raft_io_send *req, int status) {
	if (status != 0) {
		goto cleanup;
	}
	struct send_signature_result_data *data = req->data;
	sm_move(&data->state->sm, FS_SIGNATURES_PART_SENT);
cleanup:
	raft_free(req->data);
	raft_free(req);
}

static int get_checksums_ht(struct snapshot_follower_state *state,
		pageno_t first_page, unsigned page_nr, struct page_checksum *cs) {
	int rv;
	rv = sqlite3_bind_int(state->ht_select_stmt, 1, (int)(first_page + page_nr));
	if (rv != SQLITE_OK) {
		return rv;
	}
	rv = sqlite3_bind_int(state->ht_select_stmt, 2, (int)first_page);
	if (rv != SQLITE_OK) {
		return rv;
	}
	for (unsigned i = 0; i < page_nr; i++) {
		rv = sqlite3_step(state->ht_select_stmt);
		if (rv != SQLITE_ROW) {
			return rv;
		}
		long long checksum = sqlite3_column_int64(state->ht_select_stmt, 0);
		long long page_no = sqlite3_column_int64(state->ht_select_stmt, 1);
		cs[i] = (struct page_checksum) {
			.checksum = (unsigned)checksum,
			.page_no = (unsigned)page_no,
		};
	}
	rv = sqlite3_reset(state->ht_select_stmt);
	if (rv != SQLITE_OK) {
		return rv;
	}
	return SQLITE_OK;
}

static void async_send_signature_result(struct snapshot_follower_state *state,
		const struct raft_message *msg) {
	(void)msg;

	struct raft_message *reply;
	reply = raft_malloc(sizeof(*reply));
	assert(reply != NULL);

	struct page_from_to from_to = msg->signature.page_from_to;
	assert(from_to.to >= from_to.from && from_to.from <= state->last_page);
	unsigned cs_nr = MIN(from_to.to, state->last_page) - from_to.from + 1;
	bool done = from_to.from + cs_nr >= state->last_page;

	struct page_checksum *cs;
	cs = raft_malloc(sizeof(*cs) * cs_nr);
	assert(cs != NULL);
	get_checksums_ht(state, from_to.from, cs_nr, cs);

	reply->type = RAFT_IO_SIGNATURE_RESULT;
	reply->server_id = state->id;
	reply->signature_result = (struct raft_signature_result) {
		.result = done ? RAFT_RESULT_DONE : RAFT_RESULT_OK,
		.cs_nr = cs_nr,
		.cs = cs,
	};

	struct send_signature_result_data *data;
	data = raft_malloc(sizeof(*data));
	*data = (struct send_signature_result_data) {
		.state = state,
	};

	struct raft_io_send *reply_req;
	reply_req = raft_malloc(sizeof(*reply_req));
	assert(reply_req != NULL);
	reply_req->data = data;

	state->io->async_send_message(reply_req, reply, send_signature_result_cb);
}

void process_cp_or_mv_result_cb(struct raft_io_send *req, int status) {
	if (status != 0) {
		goto cleanup;
	}
	struct snapshot_follower_state *state = req->data;
	if (state->ht != NULL) {
		// TODO: remove the file on disk and same for leader.
		sqlite3_finalize(state->ht_select_stmt);
		state->ht_select_stmt = NULL;
		sqlite3_close(state->ht);
		state->ht = NULL;
	}
	sm_move(&state->sm, FS_SNAPSHOT_CHUNCK_RECEIVED);

cleanup:
	raft_free(req);
}

void async_process_cp_or_mv(struct snapshot_follower_state *state,
		const struct raft_message *msg) {
	(void)msg;
	struct raft_message *reply;
	struct raft_io_send *reply_req;

	reply = raft_malloc(sizeof(*reply));
	assert(reply != NULL);
	reply_req = raft_malloc(sizeof(*reply_req));
	assert(reply_req != NULL);
	reply_req->data = state;

	// TODO: construct proper message.
	reply->type = RAFT_IO_INSTALL_SNAPSHOT_CP_RESULT;
	reply->server_id = state->id;
	reply->install_snapshot_cp_result.result = RAFT_RESULT_DONE;
	state->io->async_send_message(reply_req, reply, process_cp_or_mv_result_cb);
}

void send_unexpected_reply_cb(struct raft_io_send *req, int status) {
	(void)status;
	raft_free(req);
}

void async_send_unexpected_reply(struct snapshot_follower_state *state,
		const struct raft_message *msg) {
	(void)state;
	(void)msg;

	struct raft_message *reply;
	reply = raft_malloc(sizeof(*reply));
	assert(reply != NULL);
	reply->type = raft_get_reply_message_type(msg->type);
	reply->server_id = state->id;
	switch (reply->type) {
		case RAFT_IO_INSTALL_SNAPSHOT_RESULT:
			reply->install_snapshot_result.result = RAFT_RESULT_UNEXPECTED;
			break;
		case RAFT_IO_INSTALL_SNAPSHOT_MV_RESULT:
			reply->install_snapshot_mv_result.result = RAFT_RESULT_UNEXPECTED;
			break;
		case RAFT_IO_INSTALL_SNAPSHOT_CP_RESULT:
			reply->install_snapshot_cp_result.result = RAFT_RESULT_UNEXPECTED;
			break;
		case RAFT_IO_SIGNATURE_RESULT:
			reply->signature_result.result = RAFT_RESULT_UNEXPECTED;
			break;
	}

	struct raft_io_send *reply_req;
	reply_req = raft_malloc(sizeof(*reply_req));
	assert(reply_req != NULL);
	reply_req->data = state;

	state->io->async_send_message(reply_req, reply, send_unexpected_reply_cb);
}

__attribute__((unused)) void follower_tick(struct sm *follower,
					   const struct raft_message *msg)
{
	(void)follower_states;

	PRE(follower != NULL);
	PRE(msg != NULL);
	PRE(msg->type > 0);

	struct snapshot_follower_state *state =
		CONTAINER_OF(follower, struct snapshot_follower_state, sm);
	struct snapshot_io *io = state->io;
	PRE(state != NULL && io != NULL);

	PRE(is_main_thread());
	// TODO: leader term checks should happen outside.
	int follower_state = sm_state(follower);
	switch (follower_state) {
	case FS_NORMAL: {
		if (msg->type != RAFT_IO_INSTALL_SNAPSHOT || 
				msg->install_snapshot.result != RAFT_RESULT_OK) {
			// TODO: add this to other state and figure out how to test it.
			async_send_unexpected_reply(state, msg);
			return;
		}
		sm_move(follower, FS_SIGNATURES_CALC_STARTED);
		async_calculate_signatures(state);
	} break;
	case FS_SIGNATURES_CALC_STARTED: {
		/* Nothing to do, wait until checksum calculation ends. */
	} break; 
	case FS_SIGNATURES_CALC_DONE:
	case FS_SIGNATURES_PART_SENT: {
		if (msg->type == RAFT_IO_SIGNATURE) {
			async_send_signature_result(state, msg);
		} else if (msg->type == RAFT_IO_INSTALL_SNAPSHOT_MV ||
				msg->type == RAFT_IO_INSTALL_SNAPSHOT_CP) {
			async_process_cp_or_mv(state, msg);
		}
	} break;
	case FS_SNAPSHOT_CHUNCK_RECEIVED: {
		if (msg->type == RAFT_IO_INSTALL_SNAPSHOT_MV ||
				msg->type == RAFT_IO_INSTALL_SNAPSHOT_CP) {
			async_process_cp_or_mv(state, msg);
		} else if (msg->type == RAFT_IO_INSTALL_SNAPSHOT) {
			async_send_install_snapshot_result(state, FS_NORMAL);
		}
	} break;
	}
}

__attribute__((unused)) static bool follower_invariant(const struct sm *sm,
						       int prev_state)
{
	bool res;
	(void)prev_state;

	struct snapshot_follower_state *state =
		CONTAINER_OF(sm, struct snapshot_follower_state, sm);

	if (sm_state(sm) == FS_SIGNATURES_CALC_DONE ||
	    sm_state(sm) == FS_SIGNATURES_PART_SENT) {
		res = CHECK(state->ht != NULL && state->ht_select_stmt != NULL);
		if (!res) {
			return false;
		}
	} else {
		res = CHECK(state->ht == NULL && state->ht_select_stmt == NULL);
		if (!res) {
			return false;
		}
	}
	// TODO: Add to invariants check for the amount of pages we have processed.
	return true;
}

void snapshot_follower_state_init(struct snapshot_follower_state *state,
		struct snapshot_io *io,
		raft_id id) {
	PRE(io != NULL);

	state->io = io;
	state->ht = NULL;
	state->ht_select_stmt = NULL;
	state->id = id;
	sm_init(&state->sm, follower_invariant, NULL, follower_states, FS_NORMAL);
}

enum leader_states {
	LS_FOLLOWER_ONLINE,
	LS_FOLLOWER_NEEDS_SNAPSHOT,
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
		.allowed = BITS(LS_FOLLOWER_NEEDS_SNAPSHOT)|BITS(LS_FOLLOWER_ONLINE),
	},
	[LS_FOLLOWER_NEEDS_SNAPSHOT] = {
		.flags = 0,
		.name = "needs_snapshot",
		.allowed = BITS(LS_SIGNATURES_CALC_STARTED)|BITS(LS_FOLLOWER_ONLINE),
	},
	[LS_SIGNATURES_CALC_STARTED] = {
		.flags = 0,
		.name = "signatures_calc_started",
		.allowed = BITS(LS_SIGNATURES_CALC_STARTED)|BITS(LS_SNAPSHOT_INSTALLATION_STARTED)|BITS(LS_FOLLOWER_ONLINE),
	},
	[LS_SNAPSHOT_INSTALLATION_STARTED] = {
		.flags = 0,
		.name = "installation_started",
		.allowed = BITS(LS_SNAPSHOT_CHUNCK_SENT)|BITS(LS_FOLLOWER_ONLINE),
	},
	[LS_SNAPSHOT_CHUNCK_SENT] = {
		.flags = 0,
		.name = "chuck_sent",
		.allowed = BITS(LS_SNAPSHOT_CHUNCK_SENT)|BITS(LS_SNAPSHOT_DONE_SENT)|BITS(LS_FOLLOWER_ONLINE),
	},
	[LS_SNAPSHOT_DONE_SENT] = {
		.flags = 0,
		.name = "done_send",
		.allowed = BITS(LS_FOLLOWER_ONLINE),
	},
};

struct insert_checksum_data {
	struct snapshot_leader_state *state;
	struct page_checksum *cs;
	unsigned cs_nr;
	int next_state;
	struct raft_message *reply; /* owned */
	struct raft_io_send *reply_req; /* owned */
};

static int insert_checksums(struct raft_io_async_work *req)
{
	int rv;

	struct insert_checksum_data *data = req->data;
	sqlite3_stmt *stmt = data->state->ht_stmt;
	for (unsigned i = 0; i < data->cs_nr; i++) {
		// TODO Instead of asserts, if we fail we send message unexpected=true and restart our sm.
		rv = insert_checksum(stmt, data->cs[i].checksum, data->cs[i].page_no);
		assert(rv == SQLITE_OK);
	}

	return SQLITE_OK;
}

static void insert_checksums_send_reply_cb(struct raft_io_send *req, int status) {
	(void)status;

	struct insert_checksum_data *data = req->data;
	sm_move(&data->state->sm, data->next_state);

	raft_free(data->reply);
	raft_free(data);
	raft_free(req);
}

static void insert_checksums_send_reply(struct raft_io_async_work *req, int status) {
	struct insert_checksum_data *data = (struct insert_checksum_data *)req->data;

	if (status != 0) {
		raft_free(data->reply);
		raft_free(data);
		raft_free(req);
		return;
	}

	raft_free(req);
	if (data->reply == NULL) {
		raft_free(data);
		return;
	}

	data->state->io->async_send_message(data->reply_req,
			data->reply,
			insert_checksums_send_reply_cb);
}

static void async_insert_checksums_send_reply(struct snapshot_leader_state *state,
				   const struct raft_message *msg,
				   struct raft_message *reply,
				   int next_state)
{
	struct insert_checksum_data *data;
	data = raft_malloc(sizeof(*data));
	assert(data != NULL);
	*data = (struct insert_checksum_data) {
		.state = state,
		.cs = msg->signature_result.cs,
		.cs_nr = msg->signature_result.cs_nr,
		.next_state = next_state,
		.reply = reply,
	};

	struct raft_io_send *reply_req;
	reply_req = raft_malloc(sizeof *reply_req);
	assert(reply_req != NULL);
	/* The data is reference circularly to be able to access it in the send
	 * reply callback as well. */
	reply_req->data = data;
	data->reply_req = reply_req;

	struct raft_io_async_work *work;
	work = raft_malloc(sizeof(*work));
	assert(work != NULL);
	*work = (struct raft_io_async_work) {
		.work = insert_checksums,
		.data = data,
	};
	state->io->async_work(work, insert_checksums_send_reply);
}

struct create_ht_data {
	struct snapshot_leader_state *state;
	int next_state;
	struct raft_message *reply; /* owned */
	struct raft_io_send *reply_req; /* owned */
};

static int leader_create_ht_and_stmt(struct raft_io_async_work *req)
{
	char db_filename[30];

	struct create_ht_data *data = req->data;
	struct snapshot_leader_state *state = data->state;
	snprintf(db_filename, 30, "ht-%lld", state->follower_id);

	return create_ht_and_stmt(db_filename, &state->ht, &state->ht_stmt);
}

static void create_ht_send_reply_cb(struct raft_io_send *req, int status) {
	(void)status;

	struct create_ht_data *data = req->data;
	sm_move(&data->state->sm, data->next_state);

	raft_free(data->reply);
	raft_free(data);
	raft_free(req);
}

static void create_ht_send_reply(struct raft_io_async_work *req, int status) {
	struct create_ht_data *data = req->data;

	if (status != 0) {
		raft_free(data->reply);
		raft_free(data);
		raft_free(req);
		return;
	}

	raft_free(req);
	data->state->io->async_send_message(data->reply_req,
			data->reply,
			create_ht_send_reply_cb);
}


static void async_create_ht_and_reply(struct snapshot_leader_state *state, struct raft_message *reply, int next_state)
{
	struct create_ht_data *data;
	data = raft_malloc(sizeof *data);
	assert(data != NULL);
	*data = (struct create_ht_data) {
		.state = state,
		.next_state = next_state,
		.reply = reply,
	};

	struct raft_io_send *reply_req;
	reply_req = raft_malloc(sizeof *reply_req);
	assert(reply_req != NULL);
	/* The data is reference circularly to be able to access it in the send
	 * reply callback as well. */
	reply_req->data = data;
	data->reply_req = reply_req;

	struct raft_io_async_work *work;
	work = raft_malloc(sizeof *work);
	assert(work != NULL);
	*work = (struct raft_io_async_work) {
		.work = leader_create_ht_and_stmt,
		.data = data,
	};
	state->io->async_work(work, create_ht_send_reply);
}

static void send_snapshot_cb(struct raft_io_send *req, int status) {
	(void)req;
	(void)status;
	struct snapshot_leader_state *state = req->data;

	sm_move(&state->sm, LS_FOLLOWER_NEEDS_SNAPSHOT);

	raft_free(req);
}

static void async_send_install_snapshot(struct snapshot_leader_state *state, const struct raft_message *msg) {
	(void)msg;

	struct raft_io_send *req;
	req = raft_malloc(sizeof(*req));
	assert(req != NULL);

	req->data = (void *)state;

	struct raft_message *reply;
	reply = raft_malloc(sizeof(*reply));
	assert(reply != NULL);

	reply->type = RAFT_IO_INSTALL_SNAPSHOT;
	reply->install_snapshot.result = RAFT_RESULT_OK;

	state->io->async_send_message(req, reply, send_snapshot_cb);
}

static void send_snapshot_done_cb(struct raft_io_send *req, int status) {
	(void)req;
	(void)status;
	struct snapshot_leader_state *state = req->data;

	sqlite3_finalize(state->ht_stmt);
	state->ht_stmt = NULL;
	sqlite3_close(state->ht);
	state->ht = NULL;
	sm_move(&state->sm, LS_SNAPSHOT_DONE_SENT);

	raft_free(req);
}

static void async_send_install_snapshot_done(struct snapshot_leader_state *state, const struct raft_message *msg) {
	(void)msg;

	struct raft_io_send *req;
	req = raft_malloc(sizeof(*req));
	assert(req != NULL);

	req->data = (void *)state;

	struct raft_message *reply;
	reply = raft_malloc(sizeof(*reply));
	assert(reply != NULL);

	reply->type = RAFT_IO_INSTALL_SNAPSHOT;
	reply->install_snapshot.result = RAFT_RESULT_DONE;

	state->io->async_send_message(req, reply, send_snapshot_done_cb);
}

static struct raft_message *get_signature_message(const struct snapshot_leader_state *state, const struct raft_message *msg) {
	(void)msg;
	(void)state;
	struct raft_message *reply;
	reply = raft_malloc(sizeof(*reply));
	assert(reply != NULL);
	// TODO: construct proper message with the next range of checksums required.
	reply->type = RAFT_IO_SIGNATURE;
	reply->signature = (struct raft_signature) {
		.page_from_to = (struct page_from_to) {
			.from = 0,
			.to = 10, // TODO: batches.
		},
	};

	return reply;
}

void process_cp_or_mv_result(struct snapshot_leader_state *state, const struct raft_message *msg) {
	if (msg->type == RAFT_IO_INSTALL_SNAPSHOT_CP_RESULT) {
		state->last_page_acked = msg->install_snapshot_cp_result.last_known_page_no;
	} else if (msg->type == RAFT_IO_INSTALL_SNAPSHOT_MV_RESULT) {
		state->last_page_acked = msg->install_snapshot_mv_result.last_known_page_no;
	}
}

static void send_mv_or_cp_cb(struct raft_io_send *req, int status) {
	(void)status;

	struct snapshot_leader_state *state = req->data;
	sm_move(&state->sm, LS_SNAPSHOT_CHUNCK_SENT);
	raft_free(req);
}

static void async_send_mv_or_cp(struct snapshot_leader_state *state) {
	(void)state;
	struct raft_message *reply;
	struct raft_io_send *reply_req;

	reply = raft_malloc(sizeof(*reply));
	assert(reply != NULL);
	reply_req = raft_malloc(sizeof(*reply_req));
	assert(reply_req != NULL);
	reply_req->data = state;

	// TODO: construct proper message.
	reply->type = RAFT_IO_INSTALL_SNAPSHOT_CP;
	state->io->async_send_message(reply_req, reply, send_mv_or_cp_cb);
}

static void calculate_local_checksums(struct raft_io_async_work *req, int status) {
	(void)status;
	struct insert_checksum_data *data = req->data;
	struct snapshot_leader_state *state = data->state;

	raft_free(req->data);
	raft_free(req);
	// TODO: Implement reading data.
	sm_move(&state->sm, LS_SNAPSHOT_INSTALLATION_STARTED);
	async_send_mv_or_cp(state);
}

static void async_insert_checksums_calculate_local(struct snapshot_leader_state *state, const struct raft_message *msg) {
	struct insert_checksum_data *data;
	data = raft_malloc(sizeof(*data));
	assert(data != NULL);
	*data = (struct insert_checksum_data) {
		.state = state,
		.cs = msg->signature_result.cs,
		.cs_nr = msg->signature_result.cs_nr,
	};

	struct raft_io_async_work *work;
	work = raft_malloc(sizeof(*work));
	assert(work != NULL);
	*work = (struct raft_io_async_work) {
		.work = insert_checksums,
		.data = data,
	};
	state->io->async_work(work, calculate_local_checksums);
}

int get_message_result(const struct raft_message *msg) {
	switch (msg->type) {
		case RAFT_IO_INSTALL_SNAPSHOT:
			return msg->install_snapshot.result;
		case RAFT_IO_INSTALL_SNAPSHOT_RESULT:
			return msg->install_snapshot_result.result;
		case RAFT_IO_INSTALL_SNAPSHOT_CP:
			return msg->install_snapshot_cp.result;
		case RAFT_IO_INSTALL_SNAPSHOT_CP_RESULT:
			return msg->install_snapshot_cp_result.result;
		case RAFT_IO_INSTALL_SNAPSHOT_MV:
			return msg->install_snapshot_mv.result;
		case RAFT_IO_INSTALL_SNAPSHOT_MV_RESULT:
			return msg->install_snapshot_mv_result.result;
		case RAFT_IO_SIGNATURE:
			return msg->signature.result;
		case RAFT_IO_SIGNATURE_RESULT:
			return msg->signature_result.result;
		default:
			/* Should be unreachable. */
			return -1;
	}
}

__attribute__((unused)) void leader_tick(struct sm *leader, const struct raft_message *msg)
{
	(void)leader_states;

	PRE(leader != NULL);
	PRE(msg != NULL);
	PRE(msg->type > 0);

	struct snapshot_leader_state *state =
		CONTAINER_OF(leader, struct snapshot_leader_state, sm);
	struct snapshot_io *io = state->io;

	PRE(state != NULL && io != NULL);
	PRE(is_main_thread());
	PRE(msg->server_id == state->follower_id);

	// TODO: timeouts.
	// TODO: distinction between thread pool and libuv.
	int leader_state = sm_state(leader);
	if (get_message_result(msg) == RAFT_RESULT_UNEXPECTED) {
		sqlite3_finalize(state->ht_stmt);
		state->ht_stmt = NULL;
		sqlite3_close(state->ht);
		state->ht = NULL;
		sm_move(&state->sm, LS_FOLLOWER_ONLINE);
		return;
	}

	switch (leader_state) {
	case LS_FOLLOWER_ONLINE: {
		if (msg->type != RAFT_IO_APPEND_ENTRIES_RESULT) {
			return;
		}
		raft_index follower_index =
			msg->append_entries_result.last_log_index;
		if (!io->log_index_found(follower_index)) {
			// Follower needs an entry which is not on the Raft log anymore.
			async_send_install_snapshot(state, msg);
		} else {
			sm_move(leader, LS_FOLLOWER_ONLINE);
		}
	} break;
	case LS_FOLLOWER_NEEDS_SNAPSHOT: {
		if (msg->type != RAFT_IO_INSTALL_SNAPSHOT_RESULT) {
			return;
		}
		// TODO: control that we do not move from stale state on async callback.
		// what happens if we finish the async callback after we received another message.
		// create type state_transition_t.
		PRE(state->ht == NULL && state->ht_stmt == NULL);

		// TODO: sem_move. Create a mutex to sync.
		struct raft_message *reply = get_signature_message(state, msg);
		async_create_ht_and_reply(state, reply, LS_SIGNATURES_CALC_STARTED);
	} break;
	case LS_SIGNATURES_CALC_STARTED: {
		if (msg->type != RAFT_IO_SIGNATURE_RESULT) {
			return;
		}
		PRE(state->ht != NULL && state->ht_stmt != NULL);

		if (msg->signature_result.result == RAFT_RESULT_DONE) {
			async_insert_checksums_calculate_local(state, msg);
		} else if (msg->signature_result.result == RAFT_RESULT_OK) {
			struct raft_message *reply = get_signature_message(state, msg);
			async_insert_checksums_send_reply(state, msg, reply, LS_SIGNATURES_CALC_STARTED);
		}
	} break;
	case LS_SNAPSHOT_INSTALLATION_STARTED: {
		/* We are not expecting any message. */
	} break;
	case LS_SNAPSHOT_CHUNCK_SENT: {
		if (msg->type != RAFT_IO_INSTALL_SNAPSHOT_MV_RESULT &&
				msg->type != RAFT_IO_INSTALL_SNAPSHOT_CP_RESULT) {
			return;
		}
		process_cp_or_mv_result(state, msg);
		if (state->last_page_acked >= state->last_page) {
			/* No more pages to sent, we are done. */
			async_send_install_snapshot_done(state, msg);
			return;
		}
		async_send_mv_or_cp(state);
	} break;
	case LS_SNAPSHOT_DONE_SENT: {
		/* We are not sending any message, only resetting the state. */
		sm_move(&state->sm, LS_FOLLOWER_ONLINE);
	} break;
	}
}

__attribute__((unused)) static bool leader_invariant(const struct sm *sm,
						     int prev_state)
{
	bool res;
	(void)prev_state;
	// TODO: if we need msg pointer it is better to store it in the
	// state thanto pass it.

	struct snapshot_leader_state *state =
		CONTAINER_OF(sm, struct snapshot_leader_state, sm);

	if (sm_state(sm) == LS_SIGNATURES_CALC_STARTED ||
	    sm_state(sm) == LS_SNAPSHOT_INSTALLATION_STARTED ||
	    sm_state(sm) == LS_SNAPSHOT_CHUNCK_SENT) {
		res = CHECK(state->ht != NULL && state->ht_stmt != NULL);
		if (!res) {
			return false;
		}
	} else {
		res = CHECK(state->ht == NULL && state->ht_stmt == NULL);
		if (!res) {
			return false;
		}
	}
	// TODO: Add to invariants check for the amount of pages we have processed.
	return true;
}

void snapshot_leader_state_init(struct snapshot_leader_state *state,
		struct snapshot_io *io,
		raft_id follower_id) {
	PRE(io != NULL);

	state->follower_id = follower_id;
	state->io = io;
	state->last_page_acked = 0;
	state->last_page = 0;
	state->ht = NULL;
	state->ht_stmt = NULL;
	sm_init(&state->sm, leader_invariant, NULL, leader_states, LS_FOLLOWER_ONLINE);
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
