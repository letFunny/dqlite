#include "src/raft.h"
#include "src/raft/recv_install_snapshot.h"
#include "../../../src/raft/recv_install_snapshot.h"
#include "../lib/runner.h"

/******************************************************************************
 *
 * Fixture with a single queue and a few test items that can be added to it.
 *
 *****************************************************************************/

struct item
{
    int value;
    queue queue;
};

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
	return false;
}

TEST(snapshot, basic, set_up, tear_down, 0, NULL) {
	// raft_id leader_id = 1;
	raft_id follower_id = 2;
	struct snapshot_leader_io io = {
		.log_index_found = mock_log_index_found,
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
	}
#if 0
	{
		struct raft_message msg = {
			.type = RAFT_IO_INSTALL_SNAPSHOT_RESULT,
			.server_id = raft_follower->id,
		};
		struct raft_install_snapshot_result install_snapshot_result = { 0 };
		msg.install_snapshot_result = install_snapshot_result;
		leader_tick(&state.sm, &msg);
		CLUSTER_STEP_UNTIL_ELAPSED(500);

		munit_assert_not_null(state.ht);
		munit_assert_not_null(state.ht_stmt);
	}

	struct page_checksum_t cs[3] = {
		{.page_no = 1, .checksum = 1},
		{.page_no = 2, .checksum = 12},
		{.page_no = 4, .checksum = 1234},
	};
	{
		struct raft_message msg = {
			.type = RAFT_IO_SIGNATURE_RESULT,
			.server_id = raft_follower->id,
		};
		struct raft_signature_result signature_result = {
			.cs = cs,
			.cs_nr = 2,
			.cs_page_no = 1 /* TODO use this field */,
			.db = "db",
		};
		msg.signature_result = signature_result;
		leader_tick(&state.sm, &msg);
		CLUSTER_STEP_UNTIL_ELAPSED(500);

		struct page_checksum_t actual_cs[2];
		get_checksums_ht(raft_follower->id, actual_cs, 2);
		assert_cs_equal(cs, actual_cs, 2);
	}
	{
		struct raft_message msg = {
			.type = RAFT_IO_SIGNATURE_RESULT,
			.server_id = raft_follower->id,
		};
		struct raft_signature_result signature_result = {
			.cs = cs + 2,
			.cs_nr = 1,
			.cs_page_no = 1,
			.db = "db",
		};
		msg.signature_result = signature_result;
		leader_tick(&state.sm, &msg);
		CLUSTER_STEP_UNTIL_ELAPSED(500);
		munit_assert_not_null(state.ht);
		munit_assert_not_null(state.ht_stmt);

		struct page_checksum_t actual_cs[3];
		get_checksums_ht(raft_follower->id, actual_cs, 3);
		assert_cs_equal(cs, actual_cs, 3);
	}
#endif

	return MUNIT_OK;
}
