package psclient

import (
	"errors"
	"log"
	"net"

	"google.golang.org/protobuf/proto"
)

// Client pubsub client
type Client struct {
	endpoint string
	con      net.Conn
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
	return &c, nil
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

// Ack acknowledge the message
func (c *Client) Ack(topic, sub, id string) error {
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

// Payload the message format
type Payload struct {
	ID   string
	Data []byte
}

// Callback a function that receives a payload
type Callback func(p Payload) error

// Subscribe to a topic via a subscription
func (c *Client) Subscribe(topic, subscription string, fn Callback) error {
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
			var p Payload
			p.ID = msg.ID
			p.Data = msg.Payload
			err = fn(p)
			if err != nil {
				log.Println(err)
			}
		}
	}
}
