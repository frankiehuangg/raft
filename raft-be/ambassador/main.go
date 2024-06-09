package main

import (
	"log"
	"net/http"
	"os"
	"raft/ambassador/client"
	"raft/utils"
	"strconv"
)

func main() {
	if len(os.Args) < 5 {
		log.Fatalln("Usage: go run main.go ip port contact_ip contact_port")
	}

	port, err := strconv.Atoi(os.Args[2])
	if err != nil {
		log.Fatalln("Usage: go run main.go ip port contact_ip contact_port")
	}

	address := utils.Address{
		IP:   os.Args[1],
		Port: int64(port),
	}

	contactIP := os.Args[3]
	contactPort, err := strconv.Atoi(os.Args[4])
	if err != nil {
		log.Fatalln("Usage: go run main.go ip port contact_ip contact_port")
	}

	contactAddress := utils.Address{
		IP:   contactIP,
		Port: int64(contactPort),
	}

	ambassador := client.NewAmbassador(&contactAddress)

	router := http.NewServeMux()

	router.HandleFunc("POST /command", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")

		if err := r.ParseForm(); err != nil {
			log.Println("error parsing form body")
			return
		}

		command := r.FormValue("command")

		if command == "request log" {
			res := ambassador.RequestLog()

			w.Write([]byte(res))
		} else {
			res := ambassador.ExecuteCommand(command)

			log.Printf("Received response: %s\n", res)

			w.Write([]byte(res))
		}
	})

	server := http.Server{
		Addr:    address.ToString(),
		Handler: router,
	}

	log.Printf("Server listening on port %s\n", address.ToString())

	server.ListenAndServe()
}
