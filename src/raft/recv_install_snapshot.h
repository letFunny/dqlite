/* InstallSnapshot RPC handlers. */

#ifndef RECV_INSTALL_SNAPSHOT_H_
#define RECV_INSTALL_SNAPSHOT_H_

#include <sqlite3.h>

#include "../raft.h"

struct snapshot_state {
	struct sm sm;
	raft_id follower_id;
	sqlite3 *db;
	sqlite3_stmt *stmt;
	struct raft *r;
};

void leader_tick(struct sm *leader, const struct raft_message *msg);

void follower_tick(struct sm *follower, const struct raft_message *msg);

/* Process an InstallSnapshot RPC from the given server. */
int recvInstallSnapshot(struct raft *r,
			raft_id id,
			const char *address,
			struct raft_install_snapshot *args);

#endif /* RECV_INSTALL_SNAPSHOT_H_ */
