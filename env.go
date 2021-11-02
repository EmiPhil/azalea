package azalea

const (
	// RaftForceMoreReelection flags the system to increase the likelihood of
	// multiple candidates running when a leader fails. DON'T USE IN PRODUCTION.
	RaftForceMoreReelection string = "RAFT_FORCE_MORE_REELECTION"

	// RaftUnreliableRPC simulates a bad rpc connections. DON'T USE IN PRODUCTION.
	RaftUnreliableRPC = "RAFT_UNRELIABLE_RPC"

	// Log level options:
	// 0 log everything
	// 1 log essentials

	// ConsensusModuleLogLevel sets the log level for the consensus module
	ConsensusModuleLogLevel = "CM_LOG_LEVEL"
)
