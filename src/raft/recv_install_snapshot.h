/* InstallSnapshot RPC handlers. */

#ifndef RECV_INSTALL_SNAPSHOT_H_
#define RECV_INSTALL_SNAPSHOT_H_

#include <sqlite3.h>

#include "../raft.h"

struct snapshot_state {
	struct sm sm;
	raft_id follower_id;
	char *db;
	sqlite3 *ht;
	sqlite3_stmt *ht_stmt;
	struct raft *r;
};

void leader_tick(struct sm *leader, const struct raft_message *msg);

void follower_tick(struct sm *follower, const struct raft_message *msg);

void snapshot_state_init(struct snapshot_state *state, struct raft *r);

/* Process an InstallSnapshot RPC from the given server. */
int recvInstallSnapshot(struct raft *r,
			raft_id id,
			const char *address,
			struct raft_install_snapshot *args);

#endif /* RECV_INSTALL_SNAPSHOT_H_ */
