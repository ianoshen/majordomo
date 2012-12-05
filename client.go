package majordomo

import (
    "time"
    zmq "github.com/alecthomas/gozmq"
)

type Client interface {
    Close()
    Send([]byte, [][]byte) [][]byte
}

type mdClient struct {
    broker string
    client zmq.Socket
    context zmq.Context
    retries int
    timeout time.Duration
    verbose bool
}

func NewClient(broker string, verbose bool) (Client, error) {
    context, err := zmq.NewContext()
    if err != nil { return nil, err}
    self := &mdClient{
        broker: broker,
        context: context,
        retries: 3,
        timeout: CLIENT_TIMEOUT,
        verbose: verbose,
    }
    self.reconnect()
    return self, nil
}

func (self *mdClient) reconnect() {
    if self.client != nil {
        self.client.Close()
    }

    self.client, _ = self.context.NewSocket(zmq.REQ)
    self.client.SetSockOptInt(zmq.LINGER, 0)
    self.client.Connect(self.broker)
    if self.verbose {
        StdLogger.Printf("Connecting to broker at %s...\n", self.broker)
    }
}

func (self *mdClient) Close() {
    if self.client != nil {
        self.client.Close()
    }
    self.context.Close()
}

func (self *mdClient) Send(service []byte, request [][]byte) (reply [][]byte){
    frame := append([][]byte{[]byte(MDPC_CLIENT), service}, request...)
    if self.verbose {
        StdLogger.Printf("Send request to '%s' service:\n%s", service, dump(frame))
    }

    for retries := self.retries; retries > 0;{
        self.client.SendMultipart(frame, 0)
        items := zmq.PollItems{
            zmq.PollItem{Socket: self.client, Events: zmq.POLLIN},
        }

        _, err := zmq.Poll(items, self.timeout.Nanoseconds()/1e3)
        if err != nil {
            ErrLogger.Println("ZMQ poll error:", err)
            continue
        }

        if item := items[0]; item.REvents&zmq.POLLIN != 0 {
            msg, err := self.client.RecvMultipart(0)
            if err != nil {
                ErrLogger.Println("Socket receive fail:", err)
                continue
            }
            if self.verbose {
                StdLogger.Print("Received reply:\n", dump(msg))
            }

            if len(msg) < 3 {
                ErrLogger.Printf("Invalid msg length %d:\n%s", len(msg), dump(msg))
                continue
            }

            header := msg[0]
            if string(header) != MDPC_CLIENT {
                ErrLogger.Printf("Incorrect header: %s, expected: %s\n", header, MDPC_CLIENT)
                continue
            }

            replyService := msg[1]
            if string(service) != string(replyService) {
                ErrLogger.Println("Incorrect reply service: %s, expected: %s\n", service, replyService)
                continue
            }

            reply = msg[2:]
            break
        } else if retries -= 1; retries > 0{
            ErrLogger.Printf("No reply from %s, reconnecting...\n", self.broker)
            self.reconnect()
        } else {
            ErrLogger.Printf("Unable to connect %s, abandoning\n", self.broker)
            break
        }
    }
    return
}
