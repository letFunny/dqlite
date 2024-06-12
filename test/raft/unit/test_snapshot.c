#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include "../lib/runner.h"
#include "../../../src/raft.h"
#include "../../../src/lib/sm.h"
#include "../../../src/raft/recv_install_snapshot.h"
#include "../../../src/raft/recv_install_snapshot.h"

struct fixture
{
	struct snapshot_leader_state leader_state;
};

static void *set_up(MUNIT_UNUSED const MunitParameter params[],
                   MUNIT_UNUSED void *user_data)
{
    struct fixture *f = munit_malloc(sizeof *f);
    return f;
}

static void tear_down(void *data)
{
    free(data);
}

SUITE(snapshot)

bool mock_log_index_found(raft_index index) {
	(void)index;
	fprintf(stderr, "log_index_found\n");
	return false;
}

static struct raft_message LAST_MESSAGE_SENT;

static void mock_send_message(struct raft_io_send *req,
		struct raft_message *msg,
		raft_io_send_cb cb) {
	(void)req;
	(void)msg;
	LAST_MESSAGE_SENT = *msg;
	fprintf(stderr, "send_message of type %d\n", msg->type);
	cb(req, 0);
}

static int mock_async_work(struct raft_io_async_work *req,
		  raft_io_async_work_cb cb) {
	fprintf(stderr, "async_work\n");
	int status = req->work((void *)&req->data);
	cb(req, status);
	return status;
}

static uint8_t *CHUNKS;
static size_t CHUNKS_NR;

static int mock_read_chunk(uint64_t offset, uint64_t chunk_size, uint8_t *chunk) {
	// TODO: proper page size, not hardcode 1024.
	pageno_t page_no = offset / (chunk_size * 1024);
	if (page_no >= CHUNKS_NR) {
		return EOF;
	}
	memcpy(chunk, &CHUNKS[page_no*chunk_size*1024], chunk_size);
	return 0;
}

static void get_checksums_ht(char *db_filename, struct page_checksum *cs, int n_cs) {
	sqlite3 *db;
	sqlite3_stmt *stmt;
	int rv;

	rv = sqlite3_open_v2(db_filename, &db, SQLITE_OPEN_READONLY, "unix");
	munit_assert_int(rv, ==, SQLITE_OK);
	rv = sqlite3_prepare_v2(db, "SELECT * FROM map;", -1, &stmt, NULL);
	munit_assert_int(rv, ==, SQLITE_OK);

	unsigned i = 0;
	while (true) {
		rv = sqlite3_step(stmt);
		if (rv != SQLITE_ROW) {
			munit_assert_int(i, ==, n_cs);
			break;
		}
		long long checksum = sqlite3_column_int64(stmt, 0);
		long long page_no = sqlite3_column_int64(stmt, 1);
		cs[i] = (struct page_checksum) {
			.checksum = checksum,
			.page_no = page_no,
		};
		i++;
	}

	sqlite3_close(db);
}

static void assert_cs_equal(struct page_checksum *cs1, struct page_checksum *cs2, size_t n) {
	for (size_t i = 0; i < n; i++) {
		munit_assert_int(cs1[i].checksum, ==, cs2[i].checksum);
		munit_assert_int(cs1[i].page_no, ==, cs2[i].page_no);
	}
}

static struct raft_message clear_last_message(void) {
	struct raft_message msg = LAST_MESSAGE_SENT;
	LAST_MESSAGE_SENT = (struct raft_message) { 0 };
	return msg;
}

TEST(snapshot, happy_path, set_up, tear_down, 0, NULL) {
	char leader_db_filename[30];

	/* Setup follower and leader state. */
	struct snapshot_io io = {
		.log_index_found = mock_log_index_found,
		.async_send_message = mock_send_message,
		.async_work = mock_async_work,
		.read_chunk = mock_read_chunk,
	};

	raft_id follower_id = 2;
	struct snapshot_follower_state follower_state = {};
	snapshot_follower_state_init(&follower_state, &io, follower_id);
	char* follower_db_filename = "ht";

	raft_id leader_id = 1;
	(void)leader_id;
	struct snapshot_leader_state leader_state = {};
	snapshot_leader_state_init(&leader_state, &io, follower_id);

	uint8_t follower_mock_db[4][1024] = {{1}, {2}, {3}, {4}};
	/* Based of follower_chunks and the fake chucksum. */
	struct page_checksum cs[4] = {
		{.page_no = 0, .checksum = 1},
		{.page_no = 1, .checksum = 2},
		{.page_no = 2, .checksum = 3},
		{.page_no = 3, .checksum = 4},
	};
	CHUNKS = (uint8_t *)follower_mock_db;
	CHUNKS_NR = 4;

	struct raft_message first_msg = {
		.type = RAFT_IO_APPEND_ENTRIES_RESULT,
		.server_id = follower_id,
	};
	struct raft_append_entries_result append_entries_result = {
		.last_log_index = 0, /* Entry not contained in the log. */
	};
	first_msg.append_entries_result = append_entries_result;

	struct page_checksum actual_cs[4];
	struct raft_message msg = first_msg;
	sprintf(leader_db_filename, "ht-%lld", follower_id);

	/* Stage 1: informing follower of snapshot installation. */
	leader_tick(&leader_state.sm, &msg);
	msg = clear_last_message();
	munit_assert_int(msg.type, ==, RAFT_IO_INSTALL_SNAPSHOT);

	follower_tick(&follower_state.sm, &msg);
	msg = clear_last_message();
	munit_assert_int(msg.type, ==, RAFT_IO_INSTALL_SNAPSHOT_RESULT);

	/* At this point the follower has calculated all its checksums. */
	get_checksums_ht(follower_db_filename, actual_cs, 4);
	assert_cs_equal(cs, actual_cs, 4);

	/* Stage 2: send signature messages to get all the checksums. */
	while (true) {
		leader_tick(&leader_state.sm, &msg);
		msg = clear_last_message();
		munit_assert_int(msg.type, ==, RAFT_IO_SIGNATURE);

		follower_tick(&follower_state.sm, &msg);
		msg = clear_last_message();
		munit_assert_int(msg.type, ==, RAFT_IO_SIGNATURE_RESULT);

		if (msg.signature_result.result == RAFT_RESULT_DONE) {
			break;
		}
	}

	/* Stage 3: send cp and mv messages. */
	bool first_cp_or_mv = true;
	while (true) {
		leader_tick(&leader_state.sm, &msg);
		msg = clear_last_message();
		munit_assert(msg.type == RAFT_IO_INSTALL_SNAPSHOT_CP ||
				msg.type == RAFT_IO_INSTALL_SNAPSHOT_MV);

		follower_tick(&follower_state.sm, &msg);
		msg = clear_last_message();
		munit_assert(msg.type == RAFT_IO_INSTALL_SNAPSHOT_CP_RESULT ||
				msg.type == RAFT_IO_INSTALL_SNAPSHOT_MV_RESULT);

		if (first_cp_or_mv) {
			/* Only at this point in time the hash table has all the
			 * information from the signatures. */
			first_cp_or_mv = false;
			get_checksums_ht(leader_db_filename, actual_cs, 4);
			assert_cs_equal(cs, actual_cs, 4);
		}

		if ((msg.type == RAFT_IO_INSTALL_SNAPSHOT_CP_RESULT &&
					msg.install_snapshot_cp_result.result == RAFT_RESULT_DONE) ||
				(msg.type == RAFT_IO_INSTALL_SNAPSHOT_MV_RESULT &&
				 msg.install_snapshot_mv_result.result == RAFT_RESULT_DONE)) {
			break;
		}
	}

	/* Stage 4: finish installation. */
	leader_tick(&leader_state.sm, &msg);
	msg = clear_last_message();
	munit_assert_int(msg.type, ==, RAFT_IO_INSTALL_SNAPSHOT);
	munit_assert_int(msg.install_snapshot.result, ==, RAFT_RESULT_DONE);

	follower_tick(&follower_state.sm, &msg);
	msg = clear_last_message();
	munit_assert_int(msg.type, ==, RAFT_IO_INSTALL_SNAPSHOT_RESULT);

	/* Avoid warnings about leaking a variable from the stack. */
	CHUNKS = NULL;
	CHUNKS_NR = 0;

	return MUNIT_OK;
}

TEST(snapshot, leader, set_up, tear_down, 0, NULL) {
	char db_filename[30];

	raft_id follower_id = 2;
	sprintf(db_filename, "ht-%lld", follower_id);
	struct snapshot_io io = {
		.log_index_found = mock_log_index_found,
		.async_send_message = mock_send_message,
		.async_work = mock_async_work,
	};
	struct snapshot_leader_state state = {};
	snapshot_leader_state_init(&state, &io, follower_id);

	{
		struct raft_message msg = {
			.type = RAFT_IO_APPEND_ENTRIES_RESULT,
			.server_id = follower_id,
		};
		struct raft_append_entries_result append_entries_result = {
			.last_log_index = 0, /* Entry not contained in the log. */
		};
		msg.append_entries_result = append_entries_result;
		leader_tick(&state.sm, &msg);

		munit_assert_int(LAST_MESSAGE_SENT.type, ==, RAFT_IO_INSTALL_SNAPSHOT);
		clear_last_message();
	}
	{
		struct raft_message msg = {
			.type = RAFT_IO_INSTALL_SNAPSHOT_RESULT,
			.server_id = follower_id,
		};
		struct raft_install_snapshot_result install_snapshot_result = { 0 };
		msg.install_snapshot_result = install_snapshot_result;
		leader_tick(&state.sm, &msg);

		munit_assert_not_null(state.ht);
		munit_assert_not_null(state.ht_stmt);
		munit_assert_int(LAST_MESSAGE_SENT.type, ==, RAFT_IO_SIGNATURE);
		clear_last_message();
	}

	struct page_checksum cs[4] = {
		{.page_no = 1, .checksum = 1},
		{.page_no = 2, .checksum = 12},
		{.page_no = 4, .checksum = 1234},
		{.page_no = 5, .checksum = 27},
	};
	{
		struct raft_message msg = {
			.type = RAFT_IO_SIGNATURE_RESULT,
			.server_id = follower_id,
		};
		struct raft_signature_result signature_result = {
			.cs = cs,
			.cs_nr = 2,
			.cs_page_no = 1, // TODO: use this field
			.db = "db",
		};
		msg.signature_result = signature_result;
		leader_tick(&state.sm, &msg);

		struct page_checksum actual_cs[2];
		get_checksums_ht(db_filename, actual_cs, 2);
		assert_cs_equal(cs, actual_cs, 2);
		munit_assert_int(LAST_MESSAGE_SENT.type, ==, RAFT_IO_SIGNATURE);
		clear_last_message();
	}
	{
		struct raft_message msg = {
			.type = RAFT_IO_SIGNATURE_RESULT,
			.server_id = follower_id,
		};
		struct raft_signature_result signature_result = {
			.cs = cs + 2,
			.cs_nr = 1,
			.cs_page_no = 1,
			.db = "db",
		};
		msg.signature_result = signature_result;
		leader_tick(&state.sm, &msg);
		munit_assert_not_null(state.ht);
		munit_assert_not_null(state.ht_stmt);

		struct page_checksum actual_cs[3];
		get_checksums_ht(db_filename, actual_cs, 3);
		assert_cs_equal(cs, actual_cs, 3);
		munit_assert_int(LAST_MESSAGE_SENT.type, ==, RAFT_IO_SIGNATURE);
		clear_last_message();
	}
	{
		struct raft_message msg = {
			.type = RAFT_IO_SIGNATURE_RESULT,
			.server_id = follower_id,
		};
		struct raft_signature_result signature_result = {
			.cs = cs + 3,
			.cs_nr = 1,
			.db = "db",
			.result = RAFT_RESULT_DONE,
		};
		msg.signature_result = signature_result;
		leader_tick(&state.sm, &msg);

		struct page_checksum actual_cs[4];
		get_checksums_ht(db_filename, actual_cs, 4);
		assert_cs_equal(cs, actual_cs, 4);
		munit_assert_int(LAST_MESSAGE_SENT.type, ==, RAFT_IO_INSTALL_SNAPSHOT_CP);
		clear_last_message();
	}
	{
		struct raft_message msg = {
			.type = RAFT_IO_INSTALL_SNAPSHOT_CP_RESULT,
			.server_id = follower_id,
		};
		struct raft_install_snapshot_cp_result install_snapshot_cp_result = {
			.last_known_page_no = 1,
		};
		msg.install_snapshot_cp_result = install_snapshot_cp_result;
		leader_tick(&state.sm, &msg);

		munit_assert_int(LAST_MESSAGE_SENT.type, ==, RAFT_IO_INSTALL_SNAPSHOT);
		munit_assert_int(LAST_MESSAGE_SENT.install_snapshot.result, ==, RAFT_RESULT_DONE);
		clear_last_message();
	}

	return MUNIT_OK;
}

TEST(snapshot, follower, set_up, tear_down, 0, NULL) {
	char* db_filename = "ht";

	raft_id leader_id = 1;
	raft_id follower_id = 2;
	struct snapshot_io io = {
		.log_index_found = mock_log_index_found,
		.async_send_message = mock_send_message,
		.async_work = mock_async_work,
		.read_chunk = mock_read_chunk,
	};
	struct snapshot_follower_state state = {};
	snapshot_follower_state_init(&state, &io, follower_id);


	uint8_t follower_chunks[4][1024] = {{1}, {2}, {3}, {4}};
	/* Based of follower_chunks. */
	struct page_checksum cs[4] = {
		{.page_no = 0, .checksum = 1},
		{.page_no = 1, .checksum = 2},
		{.page_no = 2, .checksum = 3},
		{.page_no = 3, .checksum = 4},
	};
	CHUNKS = (uint8_t *)follower_chunks;
	CHUNKS_NR = 4;
	{
		struct raft_message msg = {
			.type = RAFT_IO_INSTALL_SNAPSHOT,
			.server_id = leader_id,
		};
		struct raft_install_snapshot install_snapshot = { 0 };
		msg.install_snapshot = install_snapshot;
		follower_tick(&state.sm, &msg);

		munit_assert_not_null(state.ht);
		munit_assert_int(LAST_MESSAGE_SENT.type, ==, RAFT_IO_INSTALL_SNAPSHOT_RESULT);
		struct page_checksum actual_cs[4];
		get_checksums_ht(db_filename, actual_cs, 4);
		assert_cs_equal(cs, actual_cs, 4);
		clear_last_message();
	}
	{
		struct raft_message msg = {
			.type = RAFT_IO_SIGNATURE,
		};
		struct raft_signature signature = {
			.page_from_to = {.from = 0, .to = 4},
		};
		msg.signature = signature;
		follower_tick(&state.sm, &msg);

		munit_assert_int(LAST_MESSAGE_SENT.type, ==, RAFT_IO_SIGNATURE_RESULT);
		assert_cs_equal(cs, LAST_MESSAGE_SENT.signature_result.cs, 4);
		clear_last_message();
	}
	{
		struct raft_message msg = {
			.type = RAFT_IO_INSTALL_SNAPSHOT_CP,
		};
		char *raft_buffer_data = "data";
		struct raft_buffer page_data = {
			.base = raft_buffer_data,
			.len = strlen(raft_buffer_data),
		};
		struct raft_install_snapshot_cp install_snapshot_cp = {
			.page_no = 1,
			.page_data = page_data,
		};
		msg.install_snapshot_cp = install_snapshot_cp;
		follower_tick(&state.sm, &msg);

		munit_assert_int(LAST_MESSAGE_SENT.type, ==, RAFT_IO_INSTALL_SNAPSHOT_CP_RESULT);
		clear_last_message();
	}
	{
		struct raft_message msg = {
			.type = RAFT_IO_INSTALL_SNAPSHOT,
		};
		struct raft_install_snapshot install_snapshot = {
			.result = RAFT_RESULT_DONE,
		};
		msg.install_snapshot = install_snapshot;
		follower_tick(&state.sm, &msg);

		munit_assert_int(LAST_MESSAGE_SENT.type, ==, RAFT_IO_INSTALL_SNAPSHOT_RESULT);
		clear_last_message();
	}

	return MUNIT_OK;
}
