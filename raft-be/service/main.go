package main

import (
	"log"
	"os"
	"raft/service/node"
	"raft/utils"
	"strconv"
)

func main() {
	if len(os.Args) < 3 {
		log.Fatalln("Usage: go run main.go ip port [contact_ip] [contact_port]")
	}

	nodeIP := os.Args[1]
	nodePort, err := strconv.Atoi(os.Args[2])
	if err != nil {
		log.Fatalln("Usage: go run main.go ip port [contact_ip] [contact_port]")
	}

	address := &utils.Address{
		IP:   nodeIP,
		Port: int64(nodePort),
	}

	var rn node.RaftNode

	if len(os.Args) >= 5 {
		contactIP := os.Args[3]
		contactPort, err := strconv.Atoi(os.Args[4])
		if err != nil {
			log.Fatalln("Usage: go run main.go ip port [contact_ip] [contact_port]")
		}

		contactAddress := &utils.Address{
			IP:   contactIP,
			Port: int64(contactPort),
		}

		rn = node.NewRaftNode(address, contactAddress)
	} else {
		rn = node.NewRaftNode(address, nil)
	}

	rn.Run()
}
