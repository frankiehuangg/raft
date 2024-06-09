package node

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"raft/constants"
	"raft/pb"
	"raft/utils"
	"slices"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
)

type raftNode struct {
	pb.UnimplementedRaftServer
	mu sync.Mutex

	// Persistent state on all nodes
	currentTerm int64
	votedFor    utils.Address
	log         []utils.Entry
	address     utils.Address
	state       constants.RaftState

	// Volatile state on all nodes
	commitIndex   int64
	lastApplied   int64
	leaderAddress utils.Address
	addressList   []utils.Address

	// Volatile state on leaders
	nextIndex  []int64
	matchIndex []int64

	timeoutChannel chan bool

	app Application
}

type RaftNode interface {
	Run()

	initializeAsLeader()
	applyMembership(contactAddress utils.Address)

	sendPeriodicHeartbeats()
	sendHeartbeat(address *utils.Address)

	startElection(buffer chan<- bool)
	sendRequestVote(address *utils.Address, buffer chan<- bool)

	replicateEntries(buffer chan<- bool)
	sendAppendEntries(address *utils.Address, wg *sync.WaitGroup, buffer chan<- bool, idx int)

	distributeNewNode(address *utils.Address, newAddress *utils.Address)

	checkTerm(term int64, leaderAddress *utils.Address) bool

	printLog(level constants.LoggingLevel, message string)
}

func NewRaftNode(address *utils.Address, contactAddress *utils.Address) RaftNode {
	node := &raftNode{
		currentTerm: 0,
		votedFor:    utils.Address{},
		log:         []utils.Entry{},
		address:     *address,
		state:       constants.Follower,

		commitIndex: -1,
		lastApplied: -1,

		nextIndex:  []int64{},
		matchIndex: []int64{},

		timeoutChannel: make(chan bool),

		app: NewApplication(),
	}

	for range len(node.addressList) + 1 {
		node.nextIndex = append(node.nextIndex, int64(len(node.log)))
		node.matchIndex = append(node.matchIndex, -1)
	}

	node.printLog(constants.Debug, fmt.Sprintf("Starting a Raft Node at %s", address.ToString()))

	if contactAddress != nil {
		node.applyMembership(*contactAddress)
	} else {
		node.addressList = append(node.addressList, *address)
		node.initializeAsLeader()
	}

	return node
}

func (r *raftNode) Run() {
	go func() {
		for {
			r.printLog(constants.Debug, fmt.Sprintf("Current LastApplied (%d), current CommitIndex (%d)\n", r.lastApplied, r.commitIndex))
			//log.Println(r.lastApplied, r.commitIndex)

			go func() {
				for r.commitIndex > r.lastApplied {
					r.printLog(constants.Debug, fmt.Sprintf("Increasing lastApplied to %d\n", r.lastApplied+1))

					r.lastApplied++

					if r.state != constants.Leader {
						r.printLog(constants.Debug, fmt.Sprintf("Running command %s\n", r.log[r.lastApplied].Command))
						r.app.Execute(r.log[r.lastApplied].Command)
					}
				}
			}()

			if r.state == constants.Leader {
				r.sendPeriodicHeartbeats()
			} else if r.state == constants.Candidate {
				buffer := make(chan bool, len(r.addressList))

				go r.startElection(buffer)

				duration := time.Duration(rand.Intn(constants.ELECTION_TIMEOUT_MAX-constants.ELECTION_TIMEOUT_MIN)+constants.ELECTION_TIMEOUT_MIN) * time.Millisecond

				select {
				case <-r.timeoutChannel:
					count := 1

					for i := 0; i < len(r.addressList)/2; i++ {
						voteGranted := <-buffer
						if voteGranted {
							count++
						}
					}

					log.Printf("Accepted %d of total %d\n", count, len(r.addressList))

					if count > len(r.addressList)/2 {
						r.state = constants.Leader
						r.printLog(constants.Debug, "Node has won the election, changing to leader")

						r.nextIndex = []int64{}
						r.matchIndex = []int64{}

						for range len(r.addressList) {
							r.nextIndex = append(r.nextIndex, int64(len(r.log)))
							r.matchIndex = append(r.matchIndex, -1)
						}
					}
				case <-time.After(duration):
					r.printLog(constants.Debug, "Restarting election")
				}
			} else {
				duration := time.Duration(rand.Intn(constants.ELECTION_TIMEOUT_MAX-constants.ELECTION_TIMEOUT_MIN)+constants.ELECTION_TIMEOUT_MIN) * time.Millisecond

				select {
				case <-r.timeoutChannel:
					r.printLog(constants.Debug, "Resetting election timeout")
				case <-time.After(duration):
					r.printLog(constants.Debug, "Election timed out")
					r.state = constants.Candidate
				}
			}
		}
	}()

	listen, err := net.Listen("tcp", r.address.ToString())
	if err != nil {
		r.printLog(constants.Fatal, "Failed to create listener")
	}

	s := grpc.NewServer()
	reflection.Register(s)

	pb.RegisterRaftServer(s, r)

	if err := s.Serve(listen); err != nil {
		r.printLog(constants.Fatal, fmt.Sprintln("Failed to serve:", err))
	}
}

func (r *raftNode) initializeAsLeader() {
	r.printLog(constants.Debug, "Trying to initialize as leader")

	r.leaderAddress = r.address
	r.state = constants.Leader

	r.printLog(constants.Success, "Node has been initialized as leader")
}

func (r *raftNode) applyMembership(contactAddress utils.Address) {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(constants.CONNECT_SERVER_TIMEOUT)*time.Millisecond)
	defer cancel()

	res := &pb.JoinResults{
		Success: false,
		Address: &pb.Address{
			IP:   contactAddress.IP,
			Port: contactAddress.Port,
		},
	}

	leaderAddress := utils.Address{
		IP:   contactAddress.IP,
		Port: contactAddress.Port,
	}

	for !res.Success {
		r.printLog(constants.Debug, fmt.Sprintf("Trying to connect to %s\n", leaderAddress.ToString()))

		conn, err := grpc.NewClient(leaderAddress.ToString(), opts...)
		if err != nil {
			r.printLog(constants.Fatal, fmt.Sprintf("Fail to dial:%s\n", err))
		}

		client := pb.NewRaftClient(conn)

		res, err = client.Join(ctx, &pb.JoinArguments{
			Address: &pb.Address{
				IP:   r.address.IP,
				Port: r.address.Port,
			},
			Internal: false,
		})

		if err != nil {
			r.printLog(constants.Fatal, fmt.Sprintf("Unable to connect with contact address:%s\n", err))
		} else if !res.Success {
			leaderAddress = utils.Address{
				IP:   res.Address.IP,
				Port: res.Address.Port,
			}

			r.printLog(constants.Failed, fmt.Sprintf("Redirect request to %s\n", leaderAddress.ToString()))
		} else {
			r.leaderAddress = utils.Address{
				IP:   res.Address.IP,
				Port: res.Address.Port,
			}

			r.commitIndex = res.LastApplied

			var entries []utils.Entry
			for _, entry := range res.Entries {
				entries = append(entries, utils.Entry{
					Command: entry.Command,
					Term:    entry.Term,
				})
			}

			r.log = entries

			var addresses []utils.Address
			for _, address := range res.AddressList {
				addresses = append(addresses, utils.Address{
					IP:   address.IP,
					Port: address.Port,
				})
			}

			r.printLog(constants.Debug, fmt.Sprintf("Current entries: %v\n", r.log))
		}
	}

	r.leaderAddress = utils.Address{
		IP:   res.Address.IP,
		Port: res.Address.Port,
	}

	var entries []utils.Entry
	for _, entry := range res.Entries {
		entries = append(entries, utils.Entry{
			Command: entry.Command,
			Term:    entry.Term,
		})
	}
	r.log = entries

	var addresses []utils.Address
	for _, address := range res.AddressList {
		addresses = append(addresses, utils.Address{
			IP:   address.IP,
			Port: address.Port,
		})
	}
	r.addressList = addresses

	r.printLog(constants.Success, fmt.Sprintf("Successfully connected to %s\n", r.leaderAddress.ToString()))
}

func (r *raftNode) sendPeriodicHeartbeats() {
	for i := range r.addressList {
		if r.addressList[i] != r.address {
			r.printLog(constants.Debug, fmt.Sprintf("Sending heartbeat to %s\n", r.addressList[i].ToString()))
			go r.sendHeartbeat(&r.addressList[i])
		}
	}

	time.Sleep(time.Duration(constants.HEARTBEAT_INTERVAL) * time.Millisecond)
}

func (r *raftNode) sendHeartbeat(address *utils.Address) {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	ctx := context.WithoutCancel(context.Background())

	conn, err := grpc.NewClient(address.ToString(), opts...)
	if err != nil {
		r.printLog(constants.Fatal, fmt.Sprintf("Fail to dial:%s\n", err))
	}
	defer conn.Close()

	client := pb.NewRaftClient(conn)

	res, err := client.AppendEntries(ctx, &pb.AppendEntriesArguments{
		Term: r.currentTerm,
		LeaderAddress: &pb.Address{
			IP:   r.address.IP,
			Port: r.address.Port,
		},
		Entries:      []*pb.Entry{},
		LeaderCommit: r.commitIndex,
	})

	if err != nil {
		r.printLog(constants.Error, fmt.Sprintf("Unable to send heartbeat to %s\n", address.ToString()))
	} else {
		r.printLog(constants.Success, fmt.Sprintf("Heartbeat received successfully by %s\n", address.ToString()))

		if res.Success {
			r.checkTerm(res.Term, address)
		}
	}
}

func (r *raftNode) startElection(buffer chan<- bool) {
	r.printLog(constants.Debug, fmt.Sprintf("Starting an election"))
	r.currentTerm++
	r.votedFor = r.address

	for i := range r.addressList {
		if r.addressList[i] != r.address {
			r.printLog(constants.Debug, fmt.Sprintf("Sending vote request to %s\n", r.addressList[i].ToString()))

			go r.sendRequestVote(&r.addressList[i], buffer)
		}
	}

	r.timeoutChannel <- true

	if r.state == constants.Follower {
		return
	}
}

func (r *raftNode) sendRequestVote(address *utils.Address, buffer chan<- bool) {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	duration := time.Duration(constants.RPC_TIMEOUT) * time.Millisecond

	ctx := context.WithoutCancel(context.Background())

	var lastLogIndex int64 = 0
	var lastLogTerm int64 = 0
	if len(r.log) != 0 {
		lastLogIndex = int64(len(r.log)) - 1
		lastLogTerm = r.log[len(r.log)-1].Term
	}

	isSent := false

	for !isSent {
		conn, err := grpc.NewClient(address.ToString(), opts...)
		if err != nil {
			r.printLog(constants.Fatal, fmt.Sprintf("Fail to dial:%s\n", err))
		}
		defer conn.Close()

		client := pb.NewRaftClient(conn)

		res, err := client.RequestVote(ctx, &pb.RequestVoteArguments{
			Term: r.currentTerm,
			CandidateAddress: &pb.Address{
				IP:   r.address.IP,
				Port: r.address.Port,
			},
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		})

		if r.state != constants.Candidate {
			r.printLog(constants.Debug, fmt.Sprintf("State changed to follower, quitting election"))

			buffer <- false

			return
		}

		if err != nil {
			r.printLog(constants.Error, fmt.Sprintf("Unable to send vote request to %s\n", address.ToString()))

			time.Sleep(duration)
		} else {
			isSent = true

			if res.VoteGranted {
				r.printLog(constants.Success, fmt.Sprintf("%s has accepted your vote\n", address.ToString()))
			} else {
				r.printLog(constants.Failed, fmt.Sprintf("%s has rejected your vote\n", address.ToString()))
			}

			buffer <- res.VoteGranted

			r.checkTerm(res.Term, address)
		}
	}
}

func (r *raftNode) replicateEntries(buffer chan<- bool) {
	r.printLog(constants.Debug, fmt.Sprintf("Replicating entries to other nodes"))

	var wg sync.WaitGroup

	for i := 0; i < len(r.addressList); i++ {
		if r.addressList[i] != r.address {
			wg.Add(1)
			go r.sendAppendEntries(&r.addressList[i], &wg, buffer, i)
		}
	}
	wg.Wait()

	r.printLog(constants.Debug, fmt.Sprintf("Current nextIndex: %v\n", r.nextIndex))

	r.printLog(constants.Debug, fmt.Sprintf("Current matchIndex: %v\n", r.matchIndex))

	r.printLog(constants.Debug, fmt.Sprintf("Updating current commitIndex"))
	for i := int64(len(r.log)) - 1; i > r.commitIndex; i-- {
		if r.log[i].Term != r.currentTerm {
			continue
		}

		count := 0
		for j := 0; j < len(r.matchIndex); j++ {
			if r.matchIndex[int64(j)] >= i {
				count++
			}
		}

		if count > len(r.matchIndex)/2 {
			r.commitIndex = i
			r.printLog(constants.Success, fmt.Sprintf("Successfully increased commitIndex to %d\n", r.commitIndex))
			break
		}
	}
}

func (r *raftNode) sendAppendEntries(address *utils.Address, wg *sync.WaitGroup, buffer chan<- bool, idx int) {
	defer wg.Done()

	if r.state != constants.Leader {
		return
	}

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	ctx := context.WithoutCancel(context.Background())

	if int64(len(r.log))-1 >= r.nextIndex[idx] {
		isSent := false

		for !isSent {
			var entries []*pb.Entry

			startIndex := max(0, r.nextIndex[idx])

			for i := startIndex; i < int64(len(r.log)); i++ {
				entries = append(entries, &pb.Entry{
					Command: r.log[i].Command,
					Term:    r.log[i].Term,
				})
			}
			r.printLog(constants.Debug, fmt.Sprintf("Sending AppendEntries broadcast from index %d to %s\n", startIndex, address.ToString()))

			conn, err := grpc.NewClient(address.ToString(), opts...)
			if err != nil {
				r.printLog(constants.Fatal, fmt.Sprintf("Fail to dial:%s\n", err))
			}

			client := pb.NewRaftClient(conn)

			res, err := client.AppendEntries(ctx, &pb.AppendEntriesArguments{
				Term: r.currentTerm,
				LeaderAddress: &pb.Address{
					IP:   r.address.IP,
					Port: r.address.Port,
				},
				PrevLogIndex: startIndex,
				PrevLogTerm:  r.log[startIndex].Term,
				Entries:      entries,
				LeaderCommit: r.commitIndex,
			})

			if err != nil {
				r.printLog(constants.Error, fmt.Sprintf("Unable to send AppendEntries to %s\n", address.ToString()))
			} else {
				if res.Success {
					buffer <- res.Success

					isSent = true

					r.printLog(constants.Success, fmt.Sprintf("Successfully sent AppendEntries to %s\n", address.ToString()))

					r.nextIndex[idx] = int64(len(r.log))

					r.matchIndex[idx] = int64(len(r.log)) - 1
				} else {
					r.printLog(constants.Failed, fmt.Sprintf("Failed to send AppendEntries to %s, retrying with lower idx (%d)\n", address.ToString(), r.nextIndex[idx]-1))
					r.nextIndex[idx]--
				}
			}
		}
	}
}

func (r *raftNode) distributeNewNode(address *utils.Address, newAddress *utils.Address) {
	r.printLog(constants.Debug, fmt.Sprintf("Sending new node address to %s\n", address.ToString()))

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	ctx := context.WithoutCancel(context.Background())

	isSent := false

	for !isSent {
		r.printLog(constants.Debug, fmt.Sprintf("Trying to connect to %s\n", address.ToString()))

		conn, err := grpc.NewClient(address.ToString(), opts...)
		if err != nil {
			r.printLog(constants.Fatal, fmt.Sprintf("Fail to dial:%s\n", err))
		}

		client := pb.NewRaftClient(conn)

		res, err := client.Join(ctx, &pb.JoinArguments{
			Address: &pb.Address{
				IP:   newAddress.IP,
				Port: newAddress.Port,
			},
			Internal: true,
		})

		if err != nil {
			r.printLog(constants.Failed, fmt.Sprintf("Unable to connect with address: %s\n", err))
		} else {
			if !res.Success {
				r.printLog(constants.Failed, fmt.Sprintf("Resending join request to %s\n", address.ToString()))
			} else {
				isSent = true
			}
		}
	}

	r.printLog(constants.Success, fmt.Sprintf("Successfully adding node %s to node %s\n", newAddress.ToString(), address.ToString()))
}

func (r *raftNode) checkTerm(term int64, leaderAddress *utils.Address) bool {
	if term > r.currentTerm {
		r.printLog(constants.Debug, fmt.Sprintf("Other term (%d) is larger than current term (%d), switching to follower", term, r.currentTerm))

		r.currentTerm = term
		r.votedFor = utils.Address{}
		r.leaderAddress = *leaderAddress
		r.state = constants.Follower

		return true
	}
	return false
}

func (r *raftNode) printLog(level constants.LoggingLevel, message string) {
	if level == constants.Success {
		log.Printf("[+] [%c] [%d] %s", r.state.ToRune(), r.currentTerm, message)
	} else if level == constants.Failed {
		log.Printf("[-] [%c] [%d] %s", r.state.ToRune(), r.currentTerm, message)
	} else if level == constants.Debug {
		log.Printf("[*] [%c] [%d] %s", r.state.ToRune(), r.currentTerm, message)
	} else if level == constants.Error {
		log.Printf("[x] [%c] [%d] %s", r.state.ToRune(), r.currentTerm, message)
	} else {
		log.Fatalf("[!] [%c] [%d] %s", r.state.ToRune(), r.currentTerm, message)
	}
}

func (r *raftNode) AppendEntries(ctx context.Context, in *pb.AppendEntriesArguments) (*pb.AppendEntriesResults, error) {
	address := utils.Address{
		IP:   in.LeaderAddress.IP,
		Port: in.LeaderAddress.Port,
	}

	r.timeoutChannel <- true

	if r.checkTerm(in.Term, &address) {
		return &pb.AppendEntriesResults{
			Term:    r.currentTerm,
			Success: true,
		}, nil
	}

	if in.Term < r.currentTerm {
		r.printLog(constants.Failed, fmt.Sprintf("Reject append entries from %s, term (%d) < currentTerm (%d)\n", address.ToString(), in.Term, r.currentTerm))

		return &pb.AppendEntriesResults{
			Term:    r.currentTerm,
			Success: false,
		}, nil
	}

	// Follower will receive periodic heartbeat from leader
	if len(in.Entries) == 0 {
		r.printLog(constants.Debug, fmt.Sprintf("Received heartbeat from %s\n", address.ToString()))

		if r.state == constants.Candidate {
			r.printLog(constants.Debug, fmt.Sprintf("Candidate received heartbeat from %s, changing to follower", address.ToString()))
			r.state = constants.Follower
		} else if r.state == constants.Leader && in.Term >= r.currentTerm {
			r.printLog(constants.Debug, fmt.Sprintf("Found another node (%s) with larger or equal term, changing to follower\n", address.ToString()))
			r.state = constants.Follower
		}

		if in.LeaderCommit > r.commitIndex {
			r.printLog(constants.Debug, fmt.Sprintf("LeaderCommit (%d) > CommitIndex (%d), changing to %d\n", in.LeaderCommit, r.commitIndex, min(in.LeaderCommit, int64(len(r.log))-1)))

			r.commitIndex = min(in.LeaderCommit, int64(len(r.log))-1)
		} else {
			r.printLog(constants.Debug, fmt.Sprintf("LeaderCommit (%d) < CommitIndex (%d)\n", in.LeaderCommit, r.commitIndex))
		}

		return &pb.AppendEntriesResults{
			Term:    r.currentTerm,
			Success: true,
		}, nil
	}

	if in.PrevLogIndex < int64(len(r.log)) && r.log[in.PrevLogIndex].Term != in.PrevLogTerm {
		r.printLog(constants.Failed, fmt.Sprintf("Reject append entries from %s, PrevLogIndex term(%d) != PrevLogTerm (%d)\n", address.ToString(), r.log[in.PrevLogIndex].Term, in.PrevLogTerm))

		return &pb.AppendEntriesResults{
			Term:    r.currentTerm,
			Success: false,
		}, nil
	}

	for i := 0; i < len(in.Entries); i++ {
		if int64(i)+in.PrevLogIndex >= int64(len(r.log)) || r.log[in.PrevLogIndex+int64(i)].Term != in.PrevLogTerm {
			if int64(i)+in.PrevLogIndex >= int64(len(r.log)) {
				r.log = r.log[:int64(i)+in.PrevLogIndex]
			}

			for j := i; j < len(in.Entries); j++ {
				r.log = append(r.log, utils.Entry{
					Command: in.Entries[j].Command,
					Term:    in.Entries[j].Term,
				})
			}

			break
		}
	}

	if in.LeaderCommit > r.commitIndex {
		r.printLog(constants.Debug, fmt.Sprintf("LeaderCommit (%d) > CommitIndex (%d), changing to %d\n", in.LeaderCommit, r.commitIndex, min(in.LeaderCommit, int64(len(r.log))-1)))

		r.commitIndex = min(in.LeaderCommit, int64(len(r.log))-1)
	} else {
		r.printLog(constants.Debug, fmt.Sprintf("LeaderCommit (%d) < CommitIndex (%d)\n", in.LeaderCommit, len(r.log)-1))
	}

	r.printLog(constants.Success, fmt.Sprintf("AppendEntries from %s has been commited\n", address.ToString()))

	r.printLog(constants.Debug, fmt.Sprintf("Current entries: %v", r.log))

	return &pb.AppendEntriesResults{
		Term:    r.currentTerm,
		Success: true,
	}, nil
}

func (r *raftNode) RequestVote(ctx context.Context, in *pb.RequestVoteArguments) (*pb.RequestVoteResults, error) {
	address := utils.Address{
		IP:   in.CandidateAddress.IP,
		Port: in.CandidateAddress.Port,
	}

	if r.checkTerm(in.Term, &address) {
		return &pb.RequestVoteResults{
			Term:        r.currentTerm,
			VoteGranted: true,
		}, nil
	}

	r.printLog(constants.Debug, fmt.Sprintf("Received vote request from %s\n", address.ToString()))

	// As defined in RequestVoteRPC implementation no. 1
	if in.Term < r.currentTerm {
		r.printLog(constants.Failed, fmt.Sprintf("Reject vote request from %s, term (%d) < currentTerm (%d)\n", address.ToString(), in.Term, r.currentTerm))

		return &pb.RequestVoteResults{
			Term:        r.currentTerm,
			VoteGranted: false,
		}, nil
	}

	// As defined in RequestVoteRPC implementation no. 2
	if !r.votedFor.Equal(&utils.Address{}) && !r.votedFor.Equal(&address) {
		r.printLog(constants.Failed, fmt.Sprintf("Reject vote request from %s, votedFor is not nil and not %s  \n", address.ToString(), address.ToString()))

		return &pb.RequestVoteResults{
			Term:        r.currentTerm,
			VoteGranted: false,
		}, nil
	}

	// As defined in Raft paper 5.4.1 last paragraph
	if len(r.log) > 0 && r.log[len(r.log)-1].Term > in.LastLogTerm {
		r.printLog(constants.Failed, fmt.Sprintf("Reject vote request from %s, last term (%d) > LastLogTerm (%d)\n", address.ToString(), r.log[len(r.log)-1].Term, in.LastLogTerm))

		return &pb.RequestVoteResults{
			Term:        r.currentTerm,
			VoteGranted: false,
		}, nil
	}

	// As defined in Raft paper 5.4.1 last paragraph
	if len(r.log) > 0 && in.LastLogIndex < int64(len(r.log)) {
		r.printLog(constants.Failed, fmt.Sprintf("Reject vote request from %s, \n", address.ToString()))

		return &pb.RequestVoteResults{
			Term:        r.currentTerm,
			VoteGranted: false,
		}, nil
	}

	r.printLog(constants.Success, fmt.Sprintf("Voting for %s\n", address.ToString()))

	return &pb.RequestVoteResults{
		Term:        r.currentTerm,
		VoteGranted: true,
	}, nil
}

func (r *raftNode) Join(ctx context.Context, in *pb.JoinArguments) (*pb.JoinResults, error) {
	address := utils.Address{
		IP:   in.Address.IP,
		Port: in.Address.Port,
	}

	r.printLog(constants.Debug, fmt.Sprintf("New node %s attempting to join the cluster\n", address.ToString()))

	if slices.Contains(r.addressList, address) {
		var entries []*pb.Entry

		for _, entry := range r.log {
			entries = append(entries, &pb.Entry{
				Command: entry.Command,
				Term:    entry.Term,
			})
		}

		var addresses []*pb.Address

		for _, address := range r.addressList {
			addresses = append(addresses, &pb.Address{
				IP:   address.IP,
				Port: address.Port,
			})
		}

		return &pb.JoinResults{
			Success: true,
			Address: &pb.Address{
				IP:   r.address.IP,
				Port: r.address.Port,
			},
			Entries:     entries,
			AddressList: addresses,
		}, nil
	}

	if r.state == constants.Follower {
		if !in.Internal {
			r.printLog(constants.Failed, fmt.Sprintf("Current node is not a leader, redirecting to %s\n", r.leaderAddress.ToString()))

			return &pb.JoinResults{
				Success: false,
				Address: &pb.Address{
					IP:   r.leaderAddress.IP,
					Port: r.leaderAddress.Port,
				},
			}, nil
		}

		r.printLog(constants.Debug, fmt.Sprintf("Adding address %s to node %s\n", address.ToString(), r.address.ToString()))

		r.addressList = append(r.addressList, address)

		r.printLog(constants.Debug, fmt.Sprintf("Current node: %v\n", r.addressList))

		return &pb.JoinResults{
			Success: true,
		}, nil
	}

	r.nextIndex = append(r.nextIndex, int64(len(r.log)))
	r.matchIndex = append(r.matchIndex, -1)

	r.addressList = append(r.addressList, address)

	for _, addr := range r.addressList {
		if !addr.Equal(&address) && !addr.Equal(&r.address) {
			go r.distributeNewNode(&addr, &address)
		}
	}

	r.printLog(constants.Success, fmt.Sprintf("Node %s has joined the cluster\n", address.ToString()))

	var entries []*pb.Entry

	for _, entry := range r.log {
		entries = append(entries, &pb.Entry{
			Command: entry.Command,
			Term:    entry.Term,
		})
	}

	var addresses []*pb.Address

	for _, address := range r.addressList {
		addresses = append(addresses, &pb.Address{
			IP:   address.IP,
			Port: address.Port,
		})
	}

	return &pb.JoinResults{
		Success: true,
		Address: &pb.Address{
			IP:   r.address.IP,
			Port: r.address.Port,
		},
		LastApplied: r.lastApplied,
		Entries:     entries,
		AddressList: addresses,
	}, nil
}

func (r *raftNode) ExecuteCommand(ctx context.Context, in *pb.ExecuteCommandArguments) (*pb.ExecuteCommandResults, error) {
	// If sent to follower, forward to leader
	if r.state != constants.Leader {
		return &pb.ExecuteCommandResults{
			Success: false,
			Address: &pb.Address{
				IP:   r.leaderAddress.IP,
				Port: r.leaderAddress.Port,
			},
			Message: fmt.Sprintf("Leader node is in %s\n", r.leaderAddress.ToString()),
		}, nil
	}

	r.printLog(constants.Debug, fmt.Sprintf("Received command \"%s\" from client\n", in.Command))

	index := len(r.log)

	r.log = append(r.log, utils.Entry{
		Command: in.Command,
		Term:    r.currentTerm,
	})

	r.printLog(constants.Debug, fmt.Sprintf("Current entries: %v\n", r.log))

	buffer := make(chan bool, len(r.addressList))

	go r.replicateEntries(buffer)

	count := 1
	for i := 0; i < len(r.addressList)/2; i++ {
		voteGranted := <-buffer
		r.printLog(constants.Debug, fmt.Sprintf("Received vote: %t\n", voteGranted))
		if voteGranted {
			count++
		}
	}

	for count <= len(r.addressList)/2 && r.lastApplied < int64(index) {
		if r.state != constants.Leader {
			return &pb.ExecuteCommandResults{
				Success: false,
				Address: &pb.Address{
					IP:   r.leaderAddress.IP,
					Port: r.leaderAddress.Port,
				},
				Message: fmt.Sprintf("Leader has changed to %s, please retry", r.leaderAddress.ToString()),
			}, nil
		}
	}

	r.printLog(constants.Debug, fmt.Sprintf("Accepted %d of total %d\n", count, len(r.addressList)))

	r.printLog(constants.Debug, fmt.Sprintf("Current entries: %v\n", r.log))

	r.printLog(constants.Debug, fmt.Sprintf("Running command %s\n", r.log[len(r.log)-1].Command))

	res, err := r.app.Execute(r.log[len(r.log)-1].Command)
	if err != nil {
		r.printLog(constants.Failed, fmt.Sprintf("Error executing command: %s\n", err))

		return &pb.ExecuteCommandResults{
			Success: true,
			Address: &pb.Address{
				IP:   r.leaderAddress.IP,
				Port: r.leaderAddress.Port,
			},
			Message: fmt.Sprintf("Error executing command: %s\n", err),
		}, nil
	}

	return &pb.ExecuteCommandResults{
		Success: true,
		Message: res,
	}, nil
}

func (r *raftNode) RequestLog(ctx context.Context, in *pb.Empty) (*pb.RequestLogResult, error) {
	var entries []*pb.Entry

	for _, entry := range r.log {
		entries = append(entries, &pb.Entry{
			Command: entry.Command,
			Term:    entry.Term,
		})
	}

	return &pb.RequestLogResult{
		Entries: entries,
	}, nil
}
