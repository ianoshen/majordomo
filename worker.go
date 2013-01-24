package majordomo

import (
    "fmt"
    "time"
    zmq "github.com/alecthomas/gozmq"
)

type Worker interface {
    Close() error
    Recv([][]byte) ([][]byte, error)
}

type mdWorker struct {
    broker string
    context zmq.Context
    service string
    worker zmq.Socket

    heartbeatAt time.Time
    heartbeatIntv time.Duration
    reconnectIntv time.Duration
    retries int

    expectReply bool
    replyTo []byte
}

func newWorker(broker, service string, heartbeatIntv, reconnectIntv time.Duration, retries int) (Worker, error) {
    context, err := zmq.NewContext()
    if err != nil {return nil, err}
    worker := &mdWorker{
        broker: broker,
        context: context,
        service: service,
        heartbeatIntv: heartbeatIntv,
        reconnectIntv: reconnectIntv,
        retries: retries,
    }
    err = worker.connectToBroker()
    return worker, err
}

func (self *mdWorker) connectToBroker() (err error) {
    if self.worker != nil {
        err = self.worker.Close()
        if err != nil {return}
    }
    self.worker, err = self.context.NewSocket(zmq.DEALER)
    if err != nil {return}
    err = self.worker.SetSockOptInt(zmq.LINGER, 0)
    if err != nil {return}
    err = self.worker.Connect(self.broker)
    if err != nil {return}
    err = self.sendToBroker(MDPW_READY, []byte(self.service), nil)
    if err != nil {return}
    self.heartbeatAt = time.Now().Add(self.heartbeatIntv)
    return
}

func (self *mdWorker) sendToBroker(command string, option []byte, msg [][]byte) error {
    if len(option) > 0 {
        msg = append([][]byte{option}, msg...)
    }

    msg = append([][]byte{nil, []byte(MDPW_WORKER), []byte(command)}, msg...)
    return self.worker.SendMultipart(msg, 0)
}

func (self *mdWorker) Close() (err error) {
    if self.worker != nil {
        err = self.worker.Close()
        if err != nil {return err}
    }
    self.context.Close()
    return
}

func (self *mdWorker) Recv(reply [][]byte) (msg [][]byte, err error) {
    if len(reply) == 0 && self.expectReply {
        err = fmt.Errorf("Empty reply")
        return
    }

    if len(reply) > 0 {
        if len(self.replyTo) == 0{
            err = fmt.Errorf("Empty replyTo in worker")
            return
        }
        reply = append([][]byte{self.replyTo, nil}, reply...)
        err = self.sendToBroker(MDPW_REPLY, nil, reply)
        if err != nil {return}
    }

    self.expectReply = true

    for retries := self.retries;; retries -- {
        items := zmq.PollItems{
            zmq.PollItem{Socket: self.worker, Events: zmq.POLLIN},
        }

        _, err = zmq.Poll(items, self.heartbeatIntv.Nanoseconds()/1e3)
        if err != nil {continue}

        if item := items[0]; item.REvents&zmq.POLLIN != 0 {
            retries = self.retries

            msg, err = self.worker.RecvMultipart(0)
            if err != nil {continue}
            if len(msg) < 3 {
                err = fmt.Errorf("Invalid msg length %d", len(msg))
                continue
            }

            if header := string(msg[1]); header != MDPW_WORKER {
                err = fmt.Errorf("Incorrect header: %s, expected: %s", header, MDPW_WORKER)
                continue
            }

            switch command := string(msg[2]); command {
            case MDPW_REQUEST:
                self.replyTo = msg[3]
                msg = msg[5:]
                err = nil
                return
            case MDPW_HEARTBEAT:
                // do nothing
            case MDPW_DISCONNECT:
                err = self.connectToBroker()
                if err == nil {
                    retries = self.retries
                }
            default:
                err = fmt.Errorf("Unexpected command: %X", command)
            }
        } else if retries <= 0 {
            time.Sleep(self.reconnectIntv)
            err = self.connectToBroker()
            if err == nil {
                retries = self.retries
            }
        }

        if self.heartbeatAt.Before(time.Now()) {
            err = self.sendToBroker(MDPW_HEARTBEAT, nil, nil)
            if err == nil {
                self.heartbeatAt = time.Now().Add(self.heartbeatIntv)
            }
        }
    }

    return
}

