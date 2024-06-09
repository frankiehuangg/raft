package client

import (
	"context"
	"encoding/json"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log"
	"raft/constants"
	"raft/pb"
	"raft/utils"
	"time"
)

type serverData struct {
	currentTerm int64
	logEntries  []utils.Entry
	address     utils.Address
	state       constants.RaftState
}

type ambassador struct {
	leaderAddress utils.Address
}

type Ambassador interface {
	ExecuteCommand(command string) string
	RequestLog() string
}

func NewAmbassador(address *utils.Address) Ambassador {
	return &ambassador{
		leaderAddress: *address,
	}
}

func (a *ambassador) ExecuteCommand(command string) string {
	isSent := false

	var message string

	for !isSent {
		log.Printf("Trying to connect to %s\n", a.leaderAddress.ToString())

		opts := []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		}

		//ctx, cancel := context.WithTimeout(context.Background(), time.Duration(constants.REQUEST_TIMEOUT)*time.Millisecond)
		//defer cancel()
		ctx := context.WithoutCancel(context.Background())

		conn, err := grpc.NewClient(a.leaderAddress.ToString(), opts...)
		if err != nil {
			log.Printf("Fail to dial:%s\n", err)
		}
		defer conn.Close()

		client := pb.NewRaftClient(conn)

		res, err := client.ExecuteCommand(ctx, &pb.ExecuteCommandArguments{
			Command: command,
		})

		if err != nil {
			log.Printf("Unable to connect with address: %s\n", err)
		} else {
			if !res.Success {
				a.leaderAddress = utils.Address{
					IP:   res.Address.IP,
					Port: res.Address.Port,
				}

				log.Printf("Changing leader address to %s\n", a.leaderAddress.ToString())
			} else {
				isSent = true

				message = res.Message
			}
		}
	}

	return message
}

func (a *ambassador) RequestLog() string {
	isSent := false

	var message string

	for !isSent {
		log.Printf("Trying to connect to %s\n", a.leaderAddress.ToString())

		opts := []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(constants.REQUEST_TIMEOUT)*time.Millisecond)
		defer cancel()
		//ctx := context.WithoutCancel(context.Background())

		conn, err := grpc.NewClient(a.leaderAddress.ToString(), opts...)
		if err != nil {
			log.Printf("Fail to dial:%s\n", err)
		}
		defer conn.Close()

		client := pb.NewRaftClient(conn)

		res, err := client.RequestLog(ctx, &pb.Empty{})

		if err != nil {
			log.Printf("Unable to connect with address: %s\n", err)
		} else {
			messageByte, _ := json.Marshal(res.Entries)

			message = string(messageByte[:])

			isSent = true
		}
	}

	return message
}
