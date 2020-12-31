package main

import (
	"errors"
	"log"
	"net"
	"sync"

	"google.golang.org/protobuf/proto"
)

// Client pubsub client
type Client struct {
	endpoint string
	con      net.Conn
}

func createClient(endpoint string) (*Client, error) {
	var c Client
	c.endpoint = endpoint
	con, err := net.Dial("tcp", endpoint)
	if err != nil {
		return nil, err
	}
	c.con = con
	return &c, nil
}

func (c *Client) publish(topic string, dat []byte) error {
	var msg ClientMessage
	msg.Kind = "Publish"
	msg.Topic = topic
	msg.Payload = dat
	raw, err := proto.Marshal(&msg)
	if err != nil {
		return err
	}

	_, err = c.con.Write(raw)
	if err != nil {
		return err
	}

	// get response
	message := make([]byte, 1)
	_, err = c.con.Read(message)
	if err != nil {
		return err
	}

	// 0 bad, 1 good
	if string(message) == "0" {
		return errors.New("could not publish message")
	}

	return nil
}

func (c *Client) ack(topic, sub, id string) error {
	var msg ClientMessage
	msg.Kind = "Ack"
	msg.Topic = topic
	msg.Subscription = sub
	msg.ID = id
	raw, err := proto.Marshal(&msg)
	if err != nil {
		return err
	}

	_, err = c.con.Write(raw)
	if err != nil {
		return err
	}

	// get response
	message := make([]byte, 1)
	_, err = c.con.Read(message)
	if err != nil {
		return err
	}

	// 0 bad, 1 good
	if string(message) == "0" {
		return errors.New("could not publish message")
	}

	return nil
}

type payload struct {
	ID   string
	Data []byte
}

type callback func(p payload) error

func (c *Client) subscribe(topic, subscription string, fn callback) error {
	con, err := net.Dial("tcp", c.endpoint)
	if err != nil {
		return err
	}

	var msg ClientMessage
	msg.Kind = "Subscribe"
	msg.Topic = topic
	msg.Subscription = subscription
	raw, err := proto.Marshal(&msg)
	if err != nil {
		return err
	}

	_, err = con.Write(raw)
	if err != nil {
		return err
	}

	for {
		message := make([]byte, 4096)
		length, err := con.Read(message)

		if err != nil {
			con.Close()
			return err
		}

		var msg ClientMessage
		err = proto.Unmarshal(message[0:length], &msg)
		if err != nil {
			con.Close()
			return err
		}

		if msg.Kind == "Data" {
			var p payload
			p.ID = msg.ID
			p.Data = msg.Payload
			err = fn(p)
			if err != nil {
				log.Println(err)
			}
		}
	}
}

func main() {
	c, err := createClient("localhost:7777")
	if err != nil {
		log.Fatal(err)
	}

	// publish message to foo topic
	err = c.publish("foo", []byte("testing"))
	if err != nil {
		log.Fatal(err)
	}

	err = c.publish("foo", []byte("testing 123"))
	if err != nil {
		log.Fatal(err)
	}

	// start a subscriber loop (subscribe to bar subscription of foo topic)
	var mux sync.Mutex
	err = c.subscribe("foo", "bar", func(p payload) error {
		mux.Lock()
		defer mux.Unlock()

		log.Println(string(p.Data))

		return c.ack("foo", "bar", p.ID)
	})

	/*var msg ClientMessage
	msg.Kind = "Foo"
	msg.Topic = "Bar"
	raw, err := proto.Marshal(&msg)
	if err != nil {
		log.Fatal(err)
	}

	con, err := net.Dial("tcp", "localhost:7777")
	if err != nil {
		log.Fatal(err)
	}

	n, err := con.Write(raw)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("wrote %d bytes", n)

	err = con.Close()
	if err != nil {
		log.Fatal(err)
	}*/
}
