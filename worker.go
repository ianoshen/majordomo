package majordomo

import (
    "time"
    zmq "github.com/alecthomas/gozmq"
)

const(
    W_HEARTBEAT_LIVENESS = 3
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

    heartbeat time.Duration
    heartbeatAt time.Time
    liveness int
    reconnect time.Duration

    expectReply bool
    replyTo []byte
}

func NewWorker(broker, service string, verbose bool) Worker {
    context, _ := zmq.NewContext()
    self := &mdWorker{
        broker: broker,
        context: context,
        service: service,
        verbose: verbose,
        heartbeat: 2500 * time.Millisecond,
        liveness: 0,
        reconnect: 2500 * time.Millisecond,
    }
    self.reconnectToBroker()
    return self
}

func (self *mdWorker) reconnectToBroker() {
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
    self.liveness = W_HEARTBEAT_LIVENESS
    self.heartbeatAt = time.Now().Add(self.heartbeat)
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
    if len(reply) == 0 && self.expectReply { panic("Error reply") }

    if len(reply) > 0 {
        if len(self.replyTo) == 0{ panic("Error replyTo") }
        reply = append([][]byte{self.replyTo, nil}, reply...)
        self.sendToBroker(MDPW_REPLY, nil, reply)
    }

    self.expectReply = true

    for {
        items := zmq.PollItems{
            zmq.PollItem{Socket: self.worker, Events: zmq.POLLIN},
        }

        _, err := zmq.Poll(items, self.heartbeat.Nanoseconds()/1e3)
        if err != nil {
            panic(err)
        }

        if item := items[0]; item.REvents&zmq.POLLIN != 0 {
            msg, _ = self.worker.RecvMultipart(0)
            if self.verbose {
                StdLogger.Println("Received message from broker:\n", dump(msg))
            }
            self.liveness = W_HEARTBEAT_LIVENESS
            if len(msg) < 3 { panic("Invalid msg") }

            header := msg[1]
            if string(header) != MDPW_WORKER { panic("Invalid header") }

            command := msg[2]
            switch string(command) {
            case MDPW_REQUEST:
                self.replyTo = msg[3]
                msg = msg[5:]
                return
            case MDPW_HEARTBEAT:
                // do nothin
            case MDPW_DISCONNECT:
                self.reconnectToBroker()
            default:
                ErrLogger.Println("Invalid input message:\n", dump(msg))
            }
        } else if self.liveness -= 1; self.liveness == 0{
            ErrLogger.Println("Disconnected from broker - retrying...")
            time.Sleep(self.reconnect)
            self.reconnectToBroker()
        }

        if self.heartbeatAt.Before(time.Now()) {
            self.sendToBroker(MDPW_HEARTBEAT, nil, nil)
            self.heartbeatAt = time.Now().Add(self.heartbeat)
        }
    }

    return
}

