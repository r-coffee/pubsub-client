package psclient

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"
	sync "sync"
	"time"

	"google.golang.org/protobuf/proto"
)

// Client pubsub client
type Client struct {
	endpoint string
	con      net.Conn
	nm       *networkManager
}

// CreateClient creates a new client
func CreateClient(endpoint string) (*Client, error) {
	var c Client
	c.endpoint = endpoint
	con, err := net.Dial("tcp", endpoint)
	if err != nil {
		return nil, err
	}
	c.con = con
	c.nm = createNetworkManager()
	return &c, nil
}

// ReConnect re-establish connection
func (c *Client) ReConnect() error {
	c.con.Close()
	con, err := net.Dial("tcp", c.endpoint)
	if err != nil {
		return err
	}
	c.con = con
	return nil
}

// Publish a message to the topic
func (c *Client) Publish(topic string, dat []byte) error {
	var msg ClientMessage
	msg.Kind = "Publish"
	msg.Topic = topic
	msg.Payload = dat
	raw, err := proto.Marshal(&msg)
	if err != nil {
		return err
	}

	_, err = c.con.Write(send(raw))
	if err != nil {
		return err
	}

	// get response
	message := make([]byte, 4096)
	length, err := c.con.Read(message)
	if err != nil {
		return err
	}

	c.nm.write(message[0:length])

	return nil
}

func (c *Client) publishHandler() error {
	for {
		msg := <-c.nm.output
		if msg.Kind == "NACK" {
			log.Println("could not publish message")
		}
	}
}

// Payload the message format
type Payload struct {
	Offset uint64
	Data   []byte
}

// Callback a function that receives a payload
type Callback func(p Payload) error

// Subscribe to a topic via a subscription
func (c *Client) Subscribe(topic string, initialOffset uint64, fn Callback) error {
	// start the handler
	go c.subscribeHandler(fn)

	var msg ClientMessage
	msg.Kind = "Subscribe"
	msg.Topic = topic
	msg.Offset = initialOffset
	raw, err := proto.Marshal(&msg)
	if err != nil {
		return err
	}

	_, err = c.con.Write(send(raw))
	if err != nil {
		return err
	}

	for {
		message := make([]byte, 4096)
		length, err := c.con.Read(message)

		if err != nil {
			log.Printf("can't read message %v", err)
			return err
		}

		c.nm.write(message[0:length])
	}
}

func (c *Client) subscribeHandler(fn Callback) {
	for {
		msg := <-c.nm.output
		if msg.Kind == "Data" {
			var p Payload
			p.Offset = msg.Offset
			p.Data = msg.Payload
			err := fn(p)
			if err != nil {
				log.Println(err)
			}
		} else {
			log.Printf("unknown kind: %s", msg.Kind)
		}
	}
}

func send(raw []byte) []byte {
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, uint32(len(raw)))
	return append(bs, raw...)
}

func producer() {
	topic := "test"

	c, err := CreateClient("localhost:7777")
	if err != nil {
		log.Fatal(err)
	}
	go c.publishHandler()

	idx := 1
	for {
		msg := fmt.Sprintf("message%d", idx)
		log.Println(msg)
		err = c.Publish(topic, []byte(msg))
		if err != nil {
			log.Fatal(err)
		}
		idx++
		time.Sleep(time.Second)
	}
}

func consumer() {
	topic := "test"
	c, err := CreateClient("localhost:7777")
	if err != nil {
		log.Fatal(err)
	}

	// start a subscriber loop (subscribe to bar subscription of foo topic)
	var mux sync.Mutex
	err = c.Subscribe(topic, 0, func(p Payload) error {
		mux.Lock()
		defer mux.Unlock()

		log.Println(string(p.Data))
		//time.Sleep(100 * time.Millisecond)

		return nil
	})
	if err != nil {
		log.Printf("subscription broken: %v", err)
	}
}

func benchmark() {
	topic := "test"
	c, _ := CreateClient("localhost:7777")
	go c.publishHandler()
	for i := 0; i < 100000; i++ {
		c.Publish(topic, []byte(fmt.Sprintf("this is benchmark: %d", i+1)))
	}
}

func main() {
	//producer()
	consumer()

	//benchmark()
}
