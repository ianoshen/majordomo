package majordomo

import (
    "fmt"
    "time"
    zmq "github.com/alecthomas/gozmq"
)

type Client interface {
    Close() error
    Send(string, [][]byte) ([][]byte, error)
}

type mdClient struct {
    broker string
    client zmq.Socket
    context zmq.Context
    retries int
    timeout time.Duration
}

func newClient(broker string, retries int, timeout time.Duration) (Client, error) {
    context, err := zmq.NewContext()
    if err != nil {return nil, err}
    client := &mdClient{
        broker: broker,
        context: context,
        retries: retries,
        timeout: timeout,
    }
    err = client.connectToBroker()
    return client, err
}

func (self *mdClient) connectToBroker() (err error) {
    if self.client != nil {
        err = self.client.Close()
        if err != nil {return}
    }

    self.client, err = self.context.NewSocket(zmq.REQ)
    if err != nil {return}
    err = self.client.SetSockOptInt(zmq.LINGER, 0)
    if err != nil {return}
    err = self.client.Connect(self.broker)
    return
}

func (self *mdClient) Close() (err error) {
    if self.client != nil {
        err = self.client.Close()
        if err != nil {return err}
    }
    self.context.Close()
    return
}

func (self *mdClient) Send(service string, request [][]byte) (reply [][]byte, err error) {
    frame := append([][]byte{[]byte(MDPC_CLIENT), []byte(service)}, request...)

    for retries := self.retries; retries > 0; retries -- {
        err = self.client.SendMultipart(frame, 0)
        items := zmq.PollItems{
            zmq.PollItem{Socket: self.client, Events: zmq.POLLIN},
        }

        _, err = zmq.Poll(items, self.timeout.Nanoseconds()/1e3)
        if err != nil {continue}

        if item := items[0]; item.REvents&zmq.POLLIN != 0 {
            msg, e := self.client.RecvMultipart(0)
            if e != nil {err = e; continue}

            if len(msg) < 3 {
                err = fmt.Errorf("Invalid msg length %d", len(msg))
                continue
            }

            header := msg[0]
            if string(header) != MDPC_CLIENT {
                err = fmt.Errorf("Incorrect header: %s, expected: %s", header, MDPC_CLIENT)
                continue
            }

            replyService := msg[1]
            if string(service) != string(replyService) {
                err = fmt.Errorf("Incorrect reply service: %s, expected: %s", service, replyService)
                continue
            }

            reply = msg[2:]
            err = nil
            return
        } else {
            err = self.connectToBroker()
        }
    }
    return
}
