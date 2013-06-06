package main

import (
	"fmt"
	"net"
	"log"
	"bytes"
	"compress/flate"
	"io/ioutil"
	"io"
	"runtime"
)

const (
	ERROR = "ERROR\r\n"
	SET_CMD = "set %s %s %s %d\r\n%s\r\n"
	RESULT = "VALUE %s %s %d\r\n%s\r\nEND\r\n"
	END = "END\r\n"
)

var (
	NEWLINE = []byte{'\n'}
	SPACE = []byte{' '}
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	local, err := net.Listen("tcp", ":21211")
	if err != nil {
		log.Fatalf("Listen failed: %v", err)
	}
	defer local.Close()

	log.Print("Listening on :21211...")

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
	remote, err := net.Dial("tcp", ":11211")
	if err != nil {
		fmt.Fprint(local, ERROR)
		log.Print("Error connecting to remote")
		return
	}
	defer remote.Close()

	recv := make([]byte, 1048576)  // 1MB recv buffer
	for {
		// Read in the request
		n, err := local.Read(recv)
		if err != nil {
			if err != io.EOF {
				fmt.Fprint(local, ERROR)
			}
			// log.Printf("Error reading from local: %v", err)
			return
		}

		var buf string

		// log.Printf("> %s", recv[0:n])
		if recv[0] == 's' {  // 's' for 'set' command
			pieces := bytes.SplitN(recv[4:n], NEWLINE, 2)
			cmdPieces := bytes.Split(pieces[0], SPACE)
			value := pieces[1]
			value = value[:len(value)-2]  // strip off the \r\n
			// log.Printf("%s", cmd)
			compressedValue := new(bytes.Buffer)
			compressor, _ := flate.NewWriter(compressedValue, flate.BestSpeed)
			compressor.Write(value)
			compressor.Close()
			length := int64(compressedValue.Len())
			buf = fmt.Sprintf(SET_CMD, cmdPieces[0], cmdPieces[1], cmdPieces[2], length, compressedValue)
		} else {
			buf = fmt.Sprintf("%s", recv[0:n])
		}

		// Write the request bytes to the remote server
		fmt.Fprint(remote, buf)

		// Read back response from remote
		n, err = remote.Read(recv)
		if err != nil {
			if err != io.EOF {
				fmt.Fprint(local, ERROR)
			}
			// log.Printf("Error reading from remote: %v", err)
			return
		}

		if recv[0] == 'V' {  // 'V' for 'VALUE'
			pieces := bytes.SplitN(recv[6:n], NEWLINE, 2)
			valuePieces := bytes.Split(pieces[0], SPACE)
			value := pieces[1]
			decompressor := flate.NewReader(bytes.NewBuffer(value))
			decompressedValue, _ := ioutil.ReadAll(decompressor)
			buf = fmt.Sprintf(RESULT, valuePieces[0], valuePieces[1], len(decompressedValue), decompressedValue)
		} else {
			buf = fmt.Sprintf("%s", recv[0:n])
		}
		// log.Printf("< %s", buf)

		// Write response to local
		fmt.Fprint(local, buf)
	}
}
