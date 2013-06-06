package main

import (
	"fmt"
	"net"
	"log"
)

const (
	ERROR = "ERROR\r\n"
)

func main() {
	local, err := net.Listen("tcp", ":21211")
	if err != nil {
		log.Fatalf("Listen failed: %v", err)
	}
	defer local.Close()

	for {
		conn, err := local.Accept()
		if err != nil {
			log.Printf("Accept failed: %v", err)
			continue
		}
		go handle(conn)
	}
}

func handle(local net.Conn) {
	log.Print("accept")
	remote, err := net.Dial("tcp", ":11211")
	if err != nil {
		fmt.Fprint(local, ERROR)
		return
	}
	recv := make([]byte, 10000)
	go func() {
		defer local.Close()
		for {
			// Read in the request
			n, err := local.Read(recv)
			if err != nil {
				fmt.Fprint(local, ERROR)
				log.Print("derp")
				return
			}
			log.Printf("> %s", recv[0:n])
			if recv[0] == 's' {  // Check if the first byte is 's' for 'set'
				log.Print("setting")
			}

			// Write the request bytes to the remote server
			fmt.Fprintf(remote, "%s", recv[0:n])

			// Read back response from remote
			n, _ = remote.Read(recv)
			log.Printf("< %s", recv[0:n])

			// Write response to local
			fmt.Fprintf(local, "%s", recv[0:n])
		}
	}()
}
