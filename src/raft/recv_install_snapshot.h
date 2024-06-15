/* InstallSnapshot RPC handlers. */

#ifndef RECV_INSTALL_SNAPSHOT_H_
#define RECV_INSTALL_SNAPSHOT_H_

#include <sqlite3.h>
#include <stdint.h>

#include "../raft.h"

struct timeout {

};

typedef void (*leader_sm_cb)(void);
struct rpc {
	struct raft_message msg;
	struct sm sm;
	leader_sm_cb cb;
	/* maybe timeout */
};

struct snapshot_io {
	bool (*log_index_found)(raft_index index);
	int (*async_work)(struct raft_io_async_work *req,
			  raft_io_async_work_cb cb);
	void (*async_send_message)(struct raft_io_send *req,
			struct raft_message *msg,
			raft_io_send_cb cb);
	void (*to__start)(struct timeout *timeout, unsigned ms, leader_sm_cb cb);
	void (*to__stop)(struct timeout *timeout);
	void (*async_timeout)(unsigned duration,
			raft_io_send_cb cb);
	void (*tiemout_cancel)(unsigned duration,
			raft_io_send_cb cb);
	int (*read_chunk)(uint64_t offset,
			uint64_t chunk_size,
			uint8_t *chunk);
};

struct snp__leader_state {
	struct snapshot_io *io;
	struct sm sm;
	struct rpc rpc;
	raft_id follower_id;
	sqlite3 *ht;
	sqlite3_stmt *ht_stmt;
	pageno_t last_page;
	pageno_t last_page_acked;
};

struct snapshot_follower_state {
	struct snapshot_io *io;
	struct sm sm;
	pageno_t last_page;
	sqlite3 *ht;
	sqlite3_stmt *ht_select_stmt;

	/* TODO: temporary solution until the snapshot state is embedded in the
	 * raft struct. */
	raft_id id;
};

// TODO: Page reading from db on disk. Iterate in chunks and barriers in
// the threadpool.

void leader_tick(struct sm *leader, const struct raft_message *msg);

void follower_tick(struct sm *follower, const struct raft_message *msg);

void snapshot_leader_state_init(struct snp__leader_state *state,
		struct snapshot_io *io,
		raft_id follower_id);

void snapshot_follower_state_init(struct snapshot_follower_state *state,
		struct snapshot_io *io,
		raft_id id);

/* Get the result field from any message type in the snapshot installation procedure. */
int get_message_result(const struct raft_message *msg);

/* Process an InstallSnapshot RPC from the given server. */
int recvInstallSnapshot(struct raft *r,
			raft_id id,
			const char *address,
			struct raft_install_snapshot *args);

#endif /* RECV_INSTALL_SNAPSHOT_H_ */
