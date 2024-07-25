#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <uv.h>
#include "../lib/runner.h"
#include "../../../src/lib/sm.h"
#include "../../../src/raft.h"
#include "../../../src/raft/recv_install_snapshot.h"
#include "../../../src/utils.h"
#include "../../../src/lib/threadpool.h"
#include "test/raft/lib/tcp.h"
#include "test/raft/lib/uv.h"

struct fixture {
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

SUITE(snapshot_leader)
SUITE(snapshot_follower)

static void ut_leader_message_received(struct leader *leader,
				const struct raft_message *incoming)
{
	leader_tick(leader, incoming);
}

static void ut_follower_message_received(struct follower *follower,
				const struct raft_message *incoming)
{
	follower_tick(follower, incoming);
}

static void ut_ht_create_op(pool_work_t *w)
{
	(void)w;
}

static void ut_fill_ht_op(pool_work_t *w)
{
	(void)w;
}

static void ut_write_chunk_op(pool_work_t *w)
{
	(void)w;
}

static void ut_read_sig_op(pool_work_t *w)
{
	(void)w;
}

static void ut_disk_io(struct work *work)
{
	work->work_cb(&work->pool_work);
}

static void ut_disk_io_done(struct work *work)
{
	work->after_cb(&work->pool_work);
}

static void ut_to_expired(struct leader *leader)
{
	leader->timeout.cb(&leader->timeout.handle);
}

static void ut_rpc_sent(struct rpc *rpc)
{
	rpc->sender.cb(&rpc->sender, 0);
}

static void ut_rpc_to_expired(struct rpc *rpc)
{
	rpc->timeout.cb(&rpc->timeout.handle);
}

static const struct raft_message *append_entries_result(void)
{
	static struct raft_message append_entries = {
		.type = RAFT_IO_APPEND_ENTRIES_RESULT,
	};

	return &append_entries;
}

static const struct raft_message *ut_install_snapshot(void)
{
	static struct raft_message ut_install_snapshot = {
		.type = RAFT_IO_INSTALL_SNAPSHOT,
	};

	return &ut_install_snapshot;
}

static const struct raft_message *ut_install_snapshot_result(void)
{
	static struct raft_message ut_install_snapshot_result = {
		.type = RAFT_IO_INSTALL_SNAPSHOT_RESULT,
	};

	return &ut_install_snapshot_result;
}

static const struct raft_message *ut_sign(void)
{
	static struct raft_message ut_sign = {
		.type = RAFT_IO_SIGNATURE,
	};

	return &ut_sign;
}

static const struct raft_message *ut_sign_result(void)
{
	static struct raft_message ut_sign_result = {
		.type = RAFT_IO_SIGNATURE_RESULT,
	};

	return &ut_sign_result;
}

static const struct raft_message *ut_page(void)
{
	static struct raft_message ut_page = {
		.type = RAFT_IO_INSTALL_SNAPSHOT_CP,
	};

	return &ut_page;
}

static const struct raft_message *ut_page_result(void)
{
	static struct raft_message ut_page_result = {
		.type = RAFT_IO_INSTALL_SNAPSHOT_CP_RESULT,
	};

	return &ut_page_result;
}

static void ut_work_queue_op(struct work *w, work_op work_cb, work_op after_cb)
{
	w->work_cb = work_cb;
	w->after_cb = after_cb;
}

static void ut_to_init_op(struct timeout *to)
{
	(void)to;
}

static void ut_to_start_op(struct timeout *to, unsigned delay, to_cb_op cb)
{
	(void)delay;
	to->cb = cb;
}

static void ut_to_stop_op(struct timeout *to)
{
	(void)to;
}

static bool ut_msg_consumed = false;
static struct raft_message ut_last_msg_sent;

struct raft_message ut_get_msg_sent(void) {
	munit_assert(!ut_msg_consumed);
	ut_msg_consumed = true;
	return ut_last_msg_sent;
}

int ut_sender_send_op(struct sender *s,
		struct raft_message *payload,
		sender_cb_op cb) {
	ut_last_msg_sent = *payload;
	ut_msg_consumed = false;
	s->cb = cb;
	return 0;
}

static bool ut_is_main_thread_op(void) {
	return true;
}

TEST(snapshot_follower, basic, set_up, tear_down, 0, NULL) {
	struct follower_ops ops = {
		.ht_create = ut_ht_create_op,
		.work_queue = ut_work_queue_op,
		.sender_send = ut_sender_send_op,
		.read_sig = ut_read_sig_op,
		.write_chunk = ut_write_chunk_op,
		.fill_ht = ut_fill_ht_op,
		.is_main_thread = ut_is_main_thread_op,
	};

	struct follower follower = {
		.ops = &ops,
	};

	sm_init(&follower.sm, follower_sm_invariant,
		NULL, follower_sm_conf, "follower", FS_NORMAL);

	PRE(sm_state(&follower.sm) == FS_NORMAL);
	ut_follower_message_received(&follower, ut_install_snapshot());
	ut_rpc_sent(&follower.rpc);
	munit_assert_int(ut_get_msg_sent().type, ==, RAFT_IO_INSTALL_SNAPSHOT_RESULT);
	ut_disk_io(&follower.work);

	PRE(sm_state(&follower.sm) == FS_HT_WAIT);
	ut_disk_io_done(&follower.work);

	PRE(sm_state(&follower.sm) == FS_SIGS_CALC_LOOP);
	ut_follower_message_received(&follower, ut_sign());
	ut_rpc_sent(&follower.rpc);
	munit_assert_int(ut_get_msg_sent().type, ==, RAFT_IO_SIGNATURE_RESULT);

	PRE(sm_state(&follower.sm) == FS_SIGS_CALC_LOOP);
	ut_disk_io(&follower.work);
	follower.sigs_calculated = true;
	ut_disk_io_done(&follower.work);

	PRE(sm_state(&follower.sm) == FS_SIGS_CALC_LOOP);
	ut_follower_message_received(&follower, ut_sign());
	ut_rpc_sent(&follower.rpc);
	munit_assert_int(ut_get_msg_sent().type, ==, RAFT_IO_SIGNATURE_RESULT);

	PRE(sm_state(&follower.sm) == FS_SIG_RECEIVING);
	ut_follower_message_received(&follower, ut_sign());

	PRE(sm_state(&follower.sm) == FS_SIG_PROCESSED);
	ut_disk_io(&follower.work);
	ut_disk_io_done(&follower.work);

	PRE(sm_state(&follower.sm) == FS_SIG_READ);
	ut_rpc_sent(&follower.rpc);
	munit_assert_int(ut_get_msg_sent().type, ==, RAFT_IO_SIGNATURE_RESULT);

	PRE(sm_state(&follower.sm) == FS_CHUNCK_RECEIVING);
	ut_follower_message_received(&follower, ut_page());
	ut_disk_io(&follower.work);
	ut_disk_io_done(&follower.work);

	PRE(sm_state(&follower.sm) == FS_CHUNCK_APPLIED);
	ut_rpc_sent(&follower.rpc);
	munit_assert_int(ut_get_msg_sent().type, ==, RAFT_IO_INSTALL_SNAPSHOT_CP_RESULT);

	PRE(sm_state(&follower.sm) == FS_SNAP_DONE);
	ut_follower_message_received(&follower, ut_install_snapshot());
	ut_rpc_sent(&follower.rpc);
	munit_assert_int(ut_get_msg_sent().type, ==, RAFT_IO_INSTALL_SNAPSHOT_RESULT);

	sm_fini(&follower.sm);
	return MUNIT_OK;
}

TEST(snapshot_leader, basic, set_up, tear_down, 0, NULL) {
	struct leader_ops ops = {
		.to_init = ut_to_init_op,
		.to_stop = ut_to_stop_op,
		.to_start = ut_to_start_op,
		.ht_create = ut_ht_create_op,
		.work_queue = ut_work_queue_op,
		.sender_send = ut_sender_send_op,
		.is_main_thread = ut_is_main_thread_op,
	};

	struct leader leader = {
		.ops = &ops,

		.sigs_more = false,
		.pages_more = false,
		.sigs_calculated = false,
	};

	sm_init(&leader.sm, leader_sm_invariant,
		NULL, leader_sm_conf, "leader", LS_F_ONLINE);

	PRE(sm_state(&leader.sm) == LS_F_ONLINE);
	ut_leader_message_received(&leader, append_entries_result());

	PRE(sm_state(&leader.sm) == LS_HT_WAIT);
	ut_disk_io(&leader.work);
	ut_disk_io_done(&leader.work);

	PRE(sm_state(&leader.sm) == LS_F_NEEDS_SNAP);
	ut_rpc_sent(&leader.rpc);
	munit_assert_int(ut_get_msg_sent().type, ==, RAFT_IO_INSTALL_SNAPSHOT);
	ut_leader_message_received(&leader, ut_install_snapshot_result());

	PRE(sm_state(&leader.sm) == LS_CHECK_F_HAS_SIGS);
	ut_rpc_sent(&leader.rpc);
	munit_assert_int(ut_get_msg_sent().type, ==, RAFT_IO_SIGNATURE);
	ut_leader_message_received(&leader, ut_sign_result());
	ut_to_expired(&leader);
	leader.sigs_calculated = true;
	ut_rpc_sent(&leader.rpc);
	munit_assert_int(ut_get_msg_sent().type, ==, RAFT_IO_SIGNATURE);
	ut_leader_message_received(&leader, ut_sign_result());

	PRE(sm_state(&leader.sm) == LS_REQ_SIG_LOOP);
	munit_assert_int(ut_get_msg_sent().type, ==, RAFT_IO_SIGNATURE);
	ut_rpc_sent(&leader.rpc);
	PRE(sm_state(&leader.sm) == LS_REQ_SIG_LOOP);
	ut_leader_message_received(&leader, ut_sign_result());
	ut_disk_io(&leader.work);
	ut_disk_io_done(&leader.work);
	ut_disk_io(&leader.work);
	ut_disk_io_done(&leader.work);

	PRE(sm_state(&leader.sm) == LS_PAGE_READ);
	munit_assert_int(ut_get_msg_sent().type, ==, RAFT_IO_INSTALL_SNAPSHOT_CP);
	ut_rpc_sent(&leader.rpc);
	ut_leader_message_received(&leader, ut_page_result());

	PRE(sm_state(&leader.sm) == LS_SNAP_DONE);
	munit_assert_int(ut_get_msg_sent().type, ==, RAFT_IO_INSTALL_SNAPSHOT);
	ut_rpc_sent(&leader.rpc);
	ut_leader_message_received(&leader, ut_install_snapshot_result());

	sm_fini(&leader.sm);
	return MUNIT_OK;
}

TEST(snapshot_leader, timeouts, set_up, tear_down, 0, NULL) {
	struct leader_ops ops = {
		.to_init = ut_to_init_op,
		.to_stop = ut_to_stop_op,
		.to_start = ut_to_start_op,
		.ht_create = ut_ht_create_op,
		.work_queue = ut_work_queue_op,
		.sender_send = ut_sender_send_op,
		.is_main_thread = ut_is_main_thread_op,
	};

	struct leader leader = {
		.ops = &ops,

		.sigs_more = false,
		.pages_more = false,
		.sigs_calculated = false,
	};

	sm_init(&leader.sm, leader_sm_invariant,
		NULL, leader_sm_conf, "leader", LS_F_ONLINE);

	PRE(sm_state(&leader.sm) == LS_F_ONLINE);
	ut_leader_message_received(&leader, append_entries_result());

	PRE(sm_state(&leader.sm) == LS_HT_WAIT);
	ut_disk_io(&leader.work);
	ut_disk_io_done(&leader.work);

	PRE(sm_state(&leader.sm) == LS_F_NEEDS_SNAP);
	ut_rpc_sent(&leader.rpc);
	munit_assert_int(ut_get_msg_sent().type, ==, RAFT_IO_INSTALL_SNAPSHOT);
	ut_rpc_to_expired(&leader.rpc);

	PRE(sm_state(&leader.sm) == LS_F_NEEDS_SNAP);
	ut_rpc_sent(&leader.rpc);
	munit_assert_int(ut_get_msg_sent().type, ==, RAFT_IO_INSTALL_SNAPSHOT);
	ut_leader_message_received(&leader, ut_install_snapshot_result());

	PRE(sm_state(&leader.sm) == LS_CHECK_F_HAS_SIGS);
	ut_rpc_sent(&leader.rpc);
	munit_assert_int(ut_get_msg_sent().type, ==, RAFT_IO_SIGNATURE);
	ut_leader_message_received(&leader, ut_sign_result());
	ut_to_expired(&leader);

	PRE(sm_state(&leader.sm) == LS_CHECK_F_HAS_SIGS);
	ut_rpc_sent(&leader.rpc);
	munit_assert_int(ut_get_msg_sent().type, ==, RAFT_IO_SIGNATURE);
	ut_rpc_to_expired(&leader.rpc);

	PRE(sm_state(&leader.sm) == LS_CHECK_F_HAS_SIGS);
	leader.sigs_calculated = true;
	ut_rpc_sent(&leader.rpc);
	munit_assert_int(ut_get_msg_sent().type, ==, RAFT_IO_SIGNATURE);
	ut_leader_message_received(&leader, ut_sign_result());

	PRE(sm_state(&leader.sm) == LS_REQ_SIG_LOOP);
	ut_rpc_sent(&leader.rpc);
	munit_assert_int(ut_get_msg_sent().type, ==, RAFT_IO_SIGNATURE);
	PRE(sm_state(&leader.sm) == LS_REQ_SIG_LOOP);
	ut_leader_message_received(&leader, ut_sign_result());
	ut_disk_io(&leader.work);
	ut_disk_io_done(&leader.work);
	ut_disk_io(&leader.work);
	ut_disk_io_done(&leader.work);

	PRE(sm_state(&leader.sm) == LS_PAGE_READ);
	ut_rpc_sent(&leader.rpc);
	munit_assert_int(ut_get_msg_sent().type, ==, RAFT_IO_INSTALL_SNAPSHOT_CP);
	ut_rpc_to_expired(&leader.rpc);

	PRE(sm_state(&leader.sm) == LS_PAGE_READ);
	ut_rpc_sent(&leader.rpc);
	munit_assert_int(ut_get_msg_sent().type, ==, RAFT_IO_INSTALL_SNAPSHOT_CP);
	ut_leader_message_received(&leader, ut_page_result());

	PRE(sm_state(&leader.sm) == LS_SNAP_DONE);
	ut_rpc_sent(&leader.rpc);
	munit_assert_int(ut_get_msg_sent().type, ==, RAFT_IO_INSTALL_SNAPSHOT);
	ut_leader_message_received(&leader, ut_install_snapshot_result());

	sm_fini(&leader.sm);
	return MUNIT_OK;
}

struct test_fixture {
	struct leader leader;
	struct raft_io leader_io;
	struct follower follower;
	struct raft_io follower_io;
	/* true when union contains leader, false when it contains follower */
	bool is_leader;

	/* We only expect one message to be in-flight. */
	struct raft_message last_msg_sent;
	bool msg_sent;
	bool msg_received;

	pool_t pool;
	uv_loop_t *loop;

	/* TODO: should accomodate several background jobs in the future. Probably
	 * by scheduling a barrier in the pool after all the works that toggles this
	 * flag. */
	work_op orig_work_cb;
	bool work_done;
};

/* Not problematic because each test runs in a different process. */
static struct test_fixture global_fixture;

#define MAGIC_MAIN_THREAD 0xdef1

static __thread int thread_identifier;

static void *pool_set_up(MUNIT_UNUSED const MunitParameter params[],
                   MUNIT_UNUSED void *user_data)
{
	/* Prevent hangs. */
	alarm(2);

	global_fixture.loop = uv_default_loop();
	pool_init(&global_fixture.pool, global_fixture.loop, 4, POOL_QOS_PRIO_FAIR);
	global_fixture.pool.flags |= POOL_FOR_UT;
	thread_identifier = MAGIC_MAIN_THREAD;

	struct fixture *f = munit_malloc(sizeof *f);
	return f;
}

static void pool_tear_down(void *data)
{
    free(data);
}

static void progress(void) {
	for (unsigned i = 0; i < 20; i++) {
		uv_run(global_fixture.loop, UV_RUN_NOWAIT);
	}
}

static void wait_work(void) {
	fprintf(stderr, "WAIT_WORK start\n");
	while (!global_fixture.work_done) {
		uv_run(global_fixture.loop, UV_RUN_NOWAIT);
	}
	fprintf(stderr, "WAIT_WORK end\n");
}

static void wait_msg_sent(void) {
	fprintf(stderr, "WAIT_MSG start\n");
	while (!global_fixture.msg_sent) {
		uv_run(global_fixture.loop, UV_RUN_NOWAIT);
	}
	fprintf(stderr, "WAIT_MSG end\n");
}

static void wait_msg_received(void) {
	fprintf(stderr, "WAIT_MSG recv start\n");
	while (!global_fixture.msg_received) {
		uv_run(global_fixture.loop, UV_RUN_NOWAIT);
	}
	fprintf(stderr, "WAIT_MSG recv end\n");
}

/* Decorates the callback used when the pool work is done to set the test
 * fixture flag to true, then calls the original callback.*/
static void test_fixture_work_cb(pool_work_t *w) {
	global_fixture.work_done = true;
	global_fixture.orig_work_cb(w);
}

static void pool_to_start_op(struct timeout *to, unsigned delay, to_cb_op cb)
{
	fprintf(stderr, "TIMER START\n");
	uv_timer_start(&to->handle, cb, delay, 0);
	to->cb = cb;
}

static void pool_to_stop_op(struct timeout *to)
{
	fprintf(stderr, "TIMER STOP\n");
	uv_timer_stop(&to->handle);
	// TODO uv_close((uv_handle_t*)&to->handle, NULL);
}

static void pool_to_init_op(struct timeout *to)
{
	fprintf(stderr, "TIMER INIT\n");
	uv_timer_init(global_fixture.loop, &to->handle);
}

static void pool_work_queue_op(struct work *w, work_op work_cb, work_op after_cb)
{
	w->pool_work = (pool_work_t) { 0 };
	global_fixture.orig_work_cb = after_cb;
	global_fixture.work_done = false;
	pool_queue_work(&global_fixture.pool, &w->pool_work, 0/* */, WT_UNORD, work_cb, test_fixture_work_cb);
}

static void pool_to_expired(struct leader *leader)
{
	uv_timer_start(&leader->timeout.handle, leader->timeout.cb, 0, 0);
	progress();
}

static void pool_rpc_to_expired(struct rpc *rpc)
{
	uv_timer_start(&rpc->timeout.handle, rpc->timeout.cb, 0, 0);
	progress();
}

static bool pool_is_main_thread_op(void) {
	return thread_identifier == MAGIC_MAIN_THREAD;
}

static void pool_ht_create_op(pool_work_t *w)
{
	if (global_fixture.is_leader) {
		PRE(!global_fixture.leader.ops->is_main_thread());
	} else {
		PRE(!global_fixture.follower.ops->is_main_thread());
	}
	(void)w;
}

static void pool_fill_ht_op(pool_work_t *w)
{
	if (global_fixture.is_leader) {
		PRE(!global_fixture.leader.ops->is_main_thread());
	} else {
		PRE(!global_fixture.follower.ops->is_main_thread());
	}
	(void)w;
}

static void pool_write_chunk_op(pool_work_t *w)
{
	struct work *work = CONTAINER_OF(w, struct work, pool_work);
	struct follower *follower = CONTAINER_OF(work, struct follower, work);
	PRE(!follower->ops->is_main_thread());
}

static void pool_read_sig_op(pool_work_t *w)
{
	struct work *work = CONTAINER_OF(w, struct work, pool_work);
	struct follower *follower = CONTAINER_OF(work, struct follower, work);
	PRE(!follower->ops->is_main_thread());
}

struct uv_sender_send_data {
	struct sender *s;
	sender_cb_op cb;
};

void uv_sender_send_cb(uv_work_t *req) {
	(void)req;
}

void uv_sender_send_after_cb(uv_work_t *req, int status) {
	(void)req;
	(void)status;
	global_fixture.msg_sent = true;
	struct uv_sender_send_data *data = req->data;
	data->cb(data->s, 0);
}

int uv_sender_send_op(struct sender *s,
		struct raft_message *payload,
		sender_cb_op cb) {
	/* We only expect one message to be in-flight. */
	static uv_work_t req;
	static struct uv_sender_send_data req_data;

	global_fixture.last_msg_sent = *payload;
	/* Flag is only toggled when the after_cb is called, emulating the message
	 * being sent. */
	global_fixture.msg_sent = false;
	s->cb = cb;
	req_data = (struct uv_sender_send_data) {
		.s = s,
		.cb = cb,
	};
	req = (uv_work_t) {
		.data = &req_data,
	};
	uv_queue_work(global_fixture.loop, &req, uv_sender_send_cb, uv_sender_send_after_cb);
	return 0;
}

struct raft_message uv_get_msg_sent(void) {
	munit_assert(global_fixture.msg_sent);
	global_fixture.msg_sent = false;
	global_fixture.msg_received = false;
	return global_fixture.last_msg_sent;
}

TEST(snapshot_leader, pool_timeouts, pool_set_up, pool_tear_down, 0, NULL) {
	struct leader_ops ops = {
		.to_init = pool_to_init_op,
		.to_stop = pool_to_stop_op,
		.to_start = pool_to_start_op,
		.ht_create = pool_ht_create_op,
		.work_queue = pool_work_queue_op,
		.sender_send = uv_sender_send_op,
		.is_main_thread = pool_is_main_thread_op,
	};

	global_fixture.is_leader = true;
	struct leader *leader = &global_fixture.leader;
	*leader = (struct leader) {
		.ops = &ops,

		.sigs_more = false,
		.pages_more = false,
		.sigs_calculated = false,
	};

	sm_init(&leader->sm, leader_sm_invariant,
		NULL, leader_sm_conf, "leader", LS_F_ONLINE);

	PRE(sm_state(&leader->sm) == LS_F_ONLINE);
	ut_leader_message_received(leader, append_entries_result());

	wait_work();

	PRE(sm_state(&leader->sm) == LS_F_NEEDS_SNAP);
	wait_msg_sent();
	munit_assert_int(uv_get_msg_sent().type, ==, RAFT_IO_INSTALL_SNAPSHOT);
	pool_rpc_to_expired(&leader->rpc);

	PRE(sm_state(&leader->sm) == LS_F_NEEDS_SNAP);
	wait_msg_sent();
	munit_assert_int(uv_get_msg_sent().type, ==, RAFT_IO_INSTALL_SNAPSHOT);
	ut_leader_message_received(leader, ut_install_snapshot_result());

	PRE(sm_state(&leader->sm) == LS_CHECK_F_HAS_SIGS);
	wait_msg_sent();
	munit_assert_int(uv_get_msg_sent().type, ==, RAFT_IO_SIGNATURE);
	ut_leader_message_received(leader, ut_sign_result());
	pool_to_expired(leader);

	PRE(sm_state(&leader->sm) == LS_CHECK_F_HAS_SIGS);
	wait_msg_sent();
	munit_assert_int(uv_get_msg_sent().type, ==, RAFT_IO_SIGNATURE);
	pool_rpc_to_expired(&leader->rpc);

	PRE(sm_state(&leader->sm) == LS_CHECK_F_HAS_SIGS);
	leader->sigs_calculated = true;
	wait_msg_sent();
	munit_assert_int(uv_get_msg_sent().type, ==, RAFT_IO_SIGNATURE);
	ut_leader_message_received(leader, ut_sign_result());

	PRE(sm_state(&leader->sm) == LS_REQ_SIG_LOOP);
	wait_msg_sent();
	munit_assert_int(uv_get_msg_sent().type, ==, RAFT_IO_SIGNATURE);
	PRE(sm_state(&leader->sm) == LS_REQ_SIG_LOOP);
	ut_leader_message_received(leader, ut_sign_result());

	wait_work();
	wait_work();

	PRE(sm_state(&leader->sm) == LS_PAGE_READ);
	wait_msg_sent();
	munit_assert_int(uv_get_msg_sent().type, ==, RAFT_IO_INSTALL_SNAPSHOT_CP);
	pool_rpc_to_expired(&leader->rpc);

	PRE(sm_state(&leader->sm) == LS_PAGE_READ);
	wait_msg_sent();
	munit_assert_int(uv_get_msg_sent().type, ==, RAFT_IO_INSTALL_SNAPSHOT_CP);
	ut_leader_message_received(leader, ut_page_result());

	PRE(sm_state(&leader->sm) == LS_SNAP_DONE);
	wait_msg_sent();
	munit_assert_int(uv_get_msg_sent().type, ==, RAFT_IO_INSTALL_SNAPSHOT);
	ut_leader_message_received(leader, ut_install_snapshot_result());

	sm_fini(&leader->sm);
	return MUNIT_OK;
}

TEST(snapshot_follower, pool, pool_set_up, pool_tear_down, 0, NULL) {
	struct follower_ops ops = {
		.ht_create = pool_ht_create_op,
		.work_queue = pool_work_queue_op,
		.sender_send = uv_sender_send_op,
		.read_sig = pool_read_sig_op,
		.write_chunk = pool_write_chunk_op,
		.fill_ht = pool_fill_ht_op,
		.is_main_thread = pool_is_main_thread_op,
	};

	global_fixture.is_leader = false;
	struct follower *follower = &global_fixture.follower;

	*follower = (struct follower) {
		.ops = &ops,
	};

	sm_init(&follower->sm, follower_sm_invariant,
		NULL, follower_sm_conf, "follower", FS_NORMAL);

	PRE(sm_state(&follower->sm) == FS_NORMAL);
	ut_follower_message_received(follower, ut_install_snapshot());
	wait_msg_sent();
	munit_assert_int(uv_get_msg_sent().type, ==, RAFT_IO_INSTALL_SNAPSHOT_RESULT);

	wait_work();

	PRE(sm_state(&follower->sm) == FS_SIGS_CALC_LOOP);
	ut_follower_message_received(follower, ut_sign());
	wait_msg_sent();
	munit_assert_int(uv_get_msg_sent().type, ==, RAFT_IO_SIGNATURE_RESULT);

	PRE(sm_state(&follower->sm) == FS_SIGS_CALC_LOOP);

	follower->sigs_calculated = true;
	wait_work();

	PRE(sm_state(&follower->sm) == FS_SIGS_CALC_LOOP);
	ut_follower_message_received(follower, ut_sign());
	wait_msg_sent();
	munit_assert_int(uv_get_msg_sent().type, ==, RAFT_IO_SIGNATURE_RESULT);

	PRE(sm_state(&follower->sm) == FS_SIG_RECEIVING);
	ut_follower_message_received(follower, ut_sign());

	PRE(sm_state(&follower->sm) == FS_SIG_PROCESSED);

	wait_work();

	PRE(sm_state(&follower->sm) == FS_SIG_READ);
	wait_msg_sent();
	munit_assert_int(uv_get_msg_sent().type, ==, RAFT_IO_SIGNATURE_RESULT);

	PRE(sm_state(&follower->sm) == FS_CHUNCK_RECEIVING);
	ut_follower_message_received(follower, ut_page());

	wait_work();

	PRE(sm_state(&follower->sm) == FS_CHUNCK_APPLIED);
	wait_msg_sent();
	munit_assert_int(uv_get_msg_sent().type, ==, RAFT_IO_INSTALL_SNAPSHOT_CP_RESULT);

	PRE(sm_state(&follower->sm) == FS_SNAP_DONE);
	ut_follower_message_received(follower, ut_install_snapshot());
	wait_msg_sent();
	munit_assert_int(uv_get_msg_sent().type, ==, RAFT_IO_INSTALL_SNAPSHOT_RESULT);

	sm_fini(&follower->sm);
	return MUNIT_OK;
}

SUITE(snapshot)

/* TODO names */
struct peer
{
    struct uv_loop_s *loop;
    struct raft_uv_transport transport;
    struct raft_io io;
};

struct rft_fixture
{
    FIXTURE_UV_DEPS;
    FIXTURE_UV;
	struct peer follower;
	struct peer leader;
};

#define PEER_SETUP(peer, id, address)                                    \
    do {                                                           \
        struct raft_uv_transport *_transport = &peer.transport; \
        struct raft_io *_io = &peer.io;                         \
        int _rv;                                                   \
        _transport->version = 1;                                   \
        _rv = raft_uv_tcp_init(_transport, peer.loop);                 \
        munit_assert_int(_rv, ==, 0);                              \
        _rv = raft_uv_init(_io, peer.loop, f->dir, _transport);        \
        munit_assert_int(_rv, ==, 0);                              \
        _rv = _io->init(_io, id, address);                         \
        munit_assert_int(_rv, ==, 0);                              \
    } while (0)

static void recv_cb(struct raft_io *io, struct raft_message *msg) {
	(void)io;
	fprintf(stderr, "RECV, msg.type:%d, msg.server_address:%s, is_leader:%d\n", msg->type, msg->server_address, global_fixture.is_leader);
	global_fixture.last_msg_sent = *msg;
	global_fixture.msg_received = true;
}

static void *raft_set_up(MUNIT_UNUSED const MunitParameter params[],
                   MUNIT_UNUSED void *user_data) {
	int rv;
	/* Prevent hangs. */
	alarm(2);

	struct rft_fixture *f = munit_malloc(sizeof *f);
	SETUP_UV_DEPS;
	SETUP_UV;

	global_fixture.msg_sent = false;
	global_fixture.msg_received = false;
	global_fixture.loop = &f->loop;

	f->leader.loop = &f->loop;
    PEER_SETUP(f->leader, 1, "127.0.0.1:9001");
	global_fixture.leader_io = f->leader.io;

	f->follower.loop = &f->loop;
    PEER_SETUP(f->follower, 2, "127.0.0.1:9002");
	global_fixture.follower_io = f->follower.io;

	pool_init(&global_fixture.pool, global_fixture.loop, 4, POOL_QOS_PRIO_FAIR);
	global_fixture.pool.flags |= POOL_FOR_UT;
	thread_identifier = MAGIC_MAIN_THREAD;

	rv = global_fixture.leader_io.start(&global_fixture.leader_io, 1000, NULL, recv_cb);
	munit_assert_int(rv, ==, 0);
	rv = global_fixture.follower_io.start(&global_fixture.follower_io, 1000, NULL, recv_cb);
	munit_assert_int(rv, ==, 0);
	return f;
}

void raft_sender_send_after_cb(struct raft_io_send *req, int status) {
	munit_assert_int(status, ==, 0);

	fprintf(stderr, "AFTER SENT, is_leader:%d\n", global_fixture.is_leader);
	global_fixture.msg_sent = true;
	struct uv_sender_send_data *data = req->data;
	data->cb(data->s, status);
}

int raft_sender_send_op(struct sender *s,
		struct raft_message *msg,
		sender_cb_op cb) {
	/* We only expect one message to be in-flight. */
	static struct uv_sender_send_data req_data;
	static struct raft_io_send req;

	global_fixture.msg_sent = false;
	global_fixture.msg_received = false;
	s->cb = cb;
	req_data = (struct uv_sender_send_data) {
		.s = s,
		.cb = cb,
	};
	req = (struct raft_io_send) {
		.data = &req_data,
	};
	struct raft_io *io;
	if (global_fixture.is_leader) {
		io = &global_fixture.leader_io;
	} else {
		io = &global_fixture.follower_io;
	}

	int rv = io->send(io, &req, msg, raft_sender_send_after_cb);
	munit_assert_int(rv, ==, 0);
	fprintf(stderr, "SENT, msg.type:%d, msg.address:%s, is_leader:%d\n", msg->type, msg->server_address, global_fixture.is_leader);
	return 0;
}

struct result
{
    struct raft_message *message;
    bool done;
};

/* TODO use LOOP_RUN_UNTIL */
TEST(snapshot, both, raft_set_up, pool_tear_down, 0, NULL) {
	struct follower_ops follower_ops = {
		.ht_create = pool_ht_create_op,
		.work_queue = pool_work_queue_op,
		.sender_send = raft_sender_send_op,
		.read_sig = pool_read_sig_op,
		.write_chunk = pool_write_chunk_op,
		.fill_ht = pool_fill_ht_op,
		.is_main_thread = pool_is_main_thread_op,
	};

	struct follower *follower = &global_fixture.follower;

	*follower = (struct follower) {
		.ops = &follower_ops,
	};

	sm_init(&follower->sm, follower_sm_invariant,
		NULL, follower_sm_conf, "follower", FS_NORMAL);

	struct leader_ops leader_ops = {
		.to_init = pool_to_init_op,
		.to_stop = pool_to_stop_op,
		.to_start = pool_to_start_op,
		.ht_create = pool_ht_create_op,
		.work_queue = pool_work_queue_op,
		.sender_send = raft_sender_send_op,
		.is_main_thread = pool_is_main_thread_op,
	};

	struct leader *leader = &global_fixture.leader;
	*leader = (struct leader) {
		.ops = &leader_ops,

		.sigs_more = false,
		.pages_more = false,
		.sigs_calculated = false,
	};

	sm_init(&leader->sm, leader_sm_invariant,
		NULL, leader_sm_conf, "leader", LS_F_ONLINE);

	// TODO sm_relate(&leader->sm, &follower->sm);

	global_fixture.is_leader = true;
	ut_leader_message_received(leader, append_entries_result());
	wait_work();
	wait_msg_sent();

	struct raft_message msg;

	global_fixture.is_leader = false;
	wait_msg_received();
	msg = uv_get_msg_sent();
	/* Address of the receving end, not the sender. */
	munit_assert_int(msg.server_id, ==, 1);
	munit_assert_string_equal(msg.server_address, "127.0.0.1:9001");
	ut_follower_message_received(follower, &msg);
	wait_work();
	wait_work();
	fprintf(stderr, "BEFORE ERR\n");
	wait_msg_sent();

	global_fixture.is_leader = true;
	wait_msg_received();
	msg = uv_get_msg_sent();
	munit_assert_int(msg.server_id, ==, 2);
	munit_assert_string_equal(msg.server_address, "127.0.0.1:9002");
	ut_leader_message_received(leader, &msg);
	wait_work();
	wait_work();
	wait_msg_sent();

	// TODO pass messages to one or the other depending on address? It will be
	// more dynamic.
#define STEP \
	global_fixture.is_leader = false; \
	wait_msg_received(); \
	msg = uv_get_msg_sent(); \
	munit_assert_int(msg.server_id, ==, 1); \
	ut_follower_message_received(follower, &msg); \
	wait_work(); \
	wait_work(); \
	wait_msg_sent(); \
\
	global_fixture.is_leader = true; \
	wait_msg_received(); \
	msg = uv_get_msg_sent(); \
	munit_assert_int(msg.server_id, ==, 2); \
	ut_leader_message_received(leader, &msg); \
	wait_work(); \
	wait_work(); \
	wait_msg_sent(); \

	// uv_print_active_handles(global_fixture.loop, stderr);
	// fprintf(stderr, "------\n");

	STEP;
	follower->sigs_calculated = true;
	leader->sigs_calculated = true;
	for (unsigned i = 0; i < 10; i++) {
		STEP;
	}

	return MUNIT_OK;
}
