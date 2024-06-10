#include <stdio.h>
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

void mock_send_message(struct raft_io_send *req,
		struct raft_message *msg,
		raft_io_send_cb cb) {
	(void)req;
	(void)msg;
	LAST_MESSAGE_SENT = *msg;
	fprintf(stderr, "send_message of type %d\n", msg->type);
	cb(req, 0);
}

int mock_async_work(struct raft_io_async_work *req,
		  raft_io_async_work_cb cb) {
	fprintf(stderr, "async_work\n");
	int status = req->work((void *)&req->data);
	cb(req, status);
	return status;
}

void get_checksums_ht(raft_id follower_id, struct page_checksum_t *cs, int n_cs) {
	char db_filename[30];
	sqlite3 *db;
	sqlite3_stmt *stmt;
	int rv;

	sprintf(db_filename, "ht-%lld", follower_id);
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
		cs[i] = (struct page_checksum_t) {
			.checksum = checksum,
			.page_no = page_no,
		};
		i++;
	}

	sqlite3_close(db);
}

void assert_cs_equal(struct page_checksum_t *cs1, struct page_checksum_t *cs2, size_t n) {
	for (size_t i = 0; i < n; i++) {
		munit_assert_int(cs1[i].checksum, ==, cs2[i].checksum);
		munit_assert_int(cs1[i].page_no, ==, cs2[i].page_no);
	}
}

void clear_last_message(void) {
	LAST_MESSAGE_SENT = (struct raft_message) { 0 };
}

TEST(snapshot, basic, set_up, tear_down, 0, NULL) {
	// raft_id leader_id = 1;
	raft_id follower_id = 2;
	struct snapshot_leader_io io = {
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

	struct page_checksum_t cs[4] = {
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

		struct page_checksum_t actual_cs[2];
		get_checksums_ht(follower_id, actual_cs, 2);
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

		struct page_checksum_t actual_cs[3];
		get_checksums_ht(follower_id, actual_cs, 3);
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

		struct page_checksum_t actual_cs[4];
		get_checksums_ht(follower_id, actual_cs, 4);
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
