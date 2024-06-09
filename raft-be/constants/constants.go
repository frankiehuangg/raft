package constants

type RaftState int

const (
	Follower RaftState = iota
	Candidate
	Leader
)

func (rs *RaftState) ToRune() rune {
	if *rs == Follower {
		return 'F'
	}

	if *rs == Candidate {
		return 'C'
	}

	return 'L'
}

func (rs *RaftState) ToString() string {
	if *rs == Follower {
		return "Follower"
	}

	if *rs == Candidate {
		return "Candidate"
	}

	return "Leader"
}

const (
	RPC_TIMEOUT            int = 500
	HEARTBEAT_INTERVAL     int = 2000
	ELECTION_TIMEOUT_MIN   int = 4000
	ELECTION_TIMEOUT_MAX   int = 6000
	CONNECT_SERVER_TIMEOUT int = 10000
	REQUEST_TIMEOUT        int = 15000
)

type LoggingLevel int

const (
	Success LoggingLevel = iota
	Failed
	Debug
	Error
	Fatal
)
