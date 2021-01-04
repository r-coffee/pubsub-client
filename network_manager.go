package psclient

import (
	"encoding/binary"
	"log"

	"google.golang.org/protobuf/proto"
)

type networkManager struct {
	queue  chan byte
	output chan ClientMessage
}

func createNetworkManager() *networkManager {
	var n networkManager
	n.queue = make(chan byte, 500000)
	n.output = make(chan ClientMessage, 100)

	// start the reader
	go n.reader()

	return &n
}

func (n *networkManager) write(raw []byte) {
	for _, x := range raw {
		n.queue <- x
	}
}

func (n *networkManager) reader() {
	for {
		// get the byte count
		var cnt [4]byte
		cnt[0] = <-n.queue
		cnt[1] = <-n.queue
		cnt[2] = <-n.queue
		cnt[3] = <-n.queue
		x := int(binary.LittleEndian.Uint32(cnt[:]))
		var buffer []byte
		for i := 0; i < x; i++ {
			b := <-n.queue
			buffer = append(buffer, b)
		}
		var msg ClientMessage
		err := proto.Unmarshal(buffer, &msg)
		if err != nil {
			log.Printf("invalid message: %v", err)
		} else {
			n.output <- msg
		}
	}
}
