# Pubsub Client

### Sample Usage
```golang
c, err := CreateClient("localhost:7777")
if err != nil {
    log.Fatal(err)
}

// publish message to foo topic
err = c.Publish("foo", []byte("testing"))
if err != nil {
    log.Fatal(err)
}

// start a subscriber loop (subscribe to bar subscription of foo topic)
var mux sync.Mutex
err = c.Subscribe("foo", "bar", func(p Payload) error {
    mux.Lock()
    defer mux.Unlock()

    log.Println(string(p.Data))

    return c.Ack("foo", "bar", p.ID)
})
```