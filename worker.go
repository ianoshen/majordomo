package majordomo

import (
    "time"
    zmq "github.com/alecthomas/gozmq"
)

type Worker interface {
    Close()
    Recv([][]byte) [][]byte
}

type mdWorker struct {
    broker string
    context zmq.Context
    service string
    verbose bool
    worker zmq.Socket

    heartbeatIntv time.Duration
    heartbeatAt time.Time
    liveness int
    reconnect time.Duration

    expectReply bool
    replyTo []byte
}

func NewWorker(broker, service string, verbose bool) (Worker, error) {
    context, err := zmq.NewContext()
    if err != nil {return nil, err}
    self := &mdWorker{
        broker: broker,
        context: context,
        service: service,
        verbose: verbose,
        heartbeat: HEARTBEAT_INTERVAL,
        liveness: 0,
        reconnect: WORKER_RECONNECT_INTERVAL,
    }
    self.connectToBroker()
    return self, nil
}

func (self *mdWorker) connectToBroker() {
    if self.worker != nil {
        self.worker.Close()
    }
    self.worker, _ = self.context.NewSocket(zmq.DEALER)
    self.worker.SetSockOptInt(zmq.LINGER, 0)
    self.worker.Connect(self.broker)
    if self.verbose {
        StdLogger.Printf("Connecting to broker at %s...\n", self.broker)
    }
    self.sendToBroker(MDPW_READY, []byte(self.service), nil)
    self.liveness = HEARTBEAT_LIVENESS
    self.heartbeatAt = time.Now().Add(self.heartbeatIntv)
}

func (self *mdWorker) sendToBroker(command string, option []byte, msg [][]byte) {
    if len(option) > 0 {
        msg = append([][]byte{option}, msg...)
    }

    msg = append([][]byte{nil, []byte(MDPW_WORKER), []byte(command)}, msg...)
    if self.verbose {
        StdLogger.Printf("Sending %X to broker:\n%s", command, dump(msg))
    }
    self.worker.SendMultipart(msg, 0)
}

func (self *mdWorker) Close() {
    if self.worker != nil {
        self.worker.Close()
    }
    self.context.Close()
}

func (self *mdWorker) Recv(reply [][]byte) (msg [][]byte) {
    if len(reply) == 0 && self.expectReply {
        ErrLogger.Println("Empty reply")
        return
    }

    if len(reply) > 0 {
        if len(self.replyTo) == 0{
            ErrLogger.Println("Empty replyTo")
            return
        }
        reply = append([][]byte{self.replyTo, nil}, reply...)
        self.sendToBroker(MDPW_REPLY, nil, reply)
    }

    self.expectReply = true

    for {
        items := zmq.PollItems{
            zmq.PollItem{Socket: self.worker, Events: zmq.POLLIN},
        }

        _, err := zmq.Poll(items, self.heartbeatIntv.Nanoseconds()/1e3)
        if err != nil {
            ErrLogger.Println("ZMQ poll error:", err)
            continue
        }

        if item := items[0]; item.REvents&zmq.POLLIN != 0 {
            msg, _ = self.worker.RecvMultipart(0)
            if self.verbose {
                StdLogger.Print("Received message from broker:\n", dump(msg))
            }
            self.liveness = HEARTBEAT_LIVENESS
            if len(msg) < 3 {
                ErrLogger.Printf("Invalid msg length %d:\n%s", len(msg), dump(msg))
                continue
            }

            if header := string(msg[1]); header != MDPW_WORKER {
                ErrLogger.Printf("Incorrect header: %s, expected: %s\n", header, MDPW_WORKER)
                continue
            }

            switch command := string(msg[2]); command {
            case MDPW_REQUEST:
                self.replyTo = msg[3]
                msg = msg[5:]
                return
            case MDPW_HEARTBEAT:
                // do nothin
            case MDPW_DISCONNECT:
                self.connectToBroker()
            default:
                ErrLogger.Print("Invalid input message:\n", dump(msg))
            }
        } else if self.liveness --; self.liveness == 0{
            ErrLogger.Println("Disconnected from broker - retrying...")
            time.Sleep(self.reconnect)
            self.connectToBroker()
        }

        if self.heartbeatAt.Before(time.Now()) {
            self.sendToBroker(MDPW_HEARTBEAT, nil, nil)
            self.heartbeatAt = time.Now().Add(self.heartbeatIntv)
        }
    }

    return
}

