/* InstallSnapshot RPC handlers. */

#ifndef RECV_INSTALL_SNAPSHOT_H_
#define RECV_INSTALL_SNAPSHOT_H_

#include <sqlite3.h>

#include "../raft.h"

// Forward declaration.
struct snapshot_leader_state;

struct snapshot_leader_io {
	bool (*log_index_found)(raft_index index);
	int (*async_work)(struct raft_io_async_work *req,
			  raft_io_async_work_cb cb);
	void (*async_send_message)(struct raft_io_send *req,
			struct raft_message *msg,
			raft_io_send_cb cb);
	int (*read_page)(uint64_t offset,
			int64_t page_size,
			char *page);
	// read_chunk
};

struct snapshot_leader_state {
	struct sm sm;
	raft_id follower_id;
	sqlite3 *ht;
	sqlite3_stmt *ht_stmt;
	struct snapshot_leader_io *io;
	unsigned long last_page;
	unsigned long last_page_acked;
};

// TODO: Page reading from db on disk. Iterate in chunks and barriers in
// the threadpool.

void leader_tick(struct sm *leader, const struct raft_message *msg);

void follower_tick(struct sm *follower, const struct raft_message *msg);

void snapshot_leader_state_init(struct snapshot_leader_state *state, 
		struct snapshot_leader_io *io,
		raft_id follower_id);

/* Process an InstallSnapshot RPC from the given server. */
int recvInstallSnapshot(struct raft *r,
			raft_id id,
			const char *address,
			struct raft_install_snapshot *args);

#endif /* RECV_INSTALL_SNAPSHOT_H_ */
