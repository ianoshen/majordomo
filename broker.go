package majordomo

import (
    "encoding/hex"
    "fmt"
    "time"
    zmq "github.com/alecthomas/gozmq"
)

type Broker interface {
    Close() error
    Errors() chan error
    Run()
}

type refWorker struct {
    identity string
    address []byte
    expiry time.Time
    service *mdService
}

type mdService struct {
    broker Broker
    name string
    requests [][][]byte
    waiting *ZList
}

type mdBroker struct {
    context *zmq.Context
    services map[string]*mdService
    socket *zmq.Socket
    waiting *ZList
    workers map[string]*refWorker

    heartbeatAt time.Time
    heartbeatIntv time.Duration
    workerExpiry time.Duration

    errors chan error
}

func newBroker(endpoint string, heartbeatIntv, workerExpiry time.Duration) (broker Broker, err error) {
    context, err := zmq.NewContext()
    if err != nil {return}
    socket, err := context.NewSocket(zmq.ROUTER)
    if err != nil {return}
    err = socket.SetSockOptInt(zmq.LINGER, 0)
    if err != nil {return}
    err = socket.Bind(endpoint)
    if err != nil {return}
    broker = &mdBroker{
        context: context,
        services: make(map[string]*mdService),
        socket: socket,
        waiting: NewList(),
        workers: make(map[string]*refWorker),
        heartbeatAt: time.Now().Add(heartbeatIntv),
        heartbeatIntv: heartbeatIntv,
        workerExpiry: workerExpiry,
        errors: make(chan error),
    }
    return
}

func (self *mdBroker) deleteWorker(worker *refWorker, disconnect bool) (err error) {
    if worker == nil {
        err = fmt.Errorf("Nil worker")
        return
    }

    if disconnect {
        self.sendToWorker(worker, MDPW_DISCONNECT, nil, nil)
    }

    if worker.service != nil {
        worker.service.waiting.Delete(worker)
    }
    self.waiting.Delete(worker)
    delete(self.workers, worker.identity)
    return
}

func (self *mdBroker) dispatch(service *mdService, msg [][]byte) (err error) {
    if service == nil {
        err = fmt.Errorf("Nil service")
        return
    }
    if len(msg) != 0 {
        service.requests = append(service.requests, msg)
    }
    if err = self.purgeWorkers(); err != nil {return}
    for service.waiting.Len() > 0 && len(service.requests) > 0 {
        msg, service.requests = service.requests[0], service.requests[1:]
        elem := service.waiting.Pop()
        self.waiting.Remove(elem)
        worker, ok := elem.Value.(*refWorker)
        if !ok {
            err = fmt.Errorf("Worker type assertion fail")
            return
        }
        err = self.sendToWorker(worker, MDPW_REQUEST, nil, msg)
        if err != nil {return}
    }
    return
}

func (self *mdBroker) processClient(sender []byte, msg [][]byte) (err error) {
    if len(msg) < 2 {
        err = fmt.Errorf("Invalid msg length: %d", len(msg))
        return
    }
    service := msg[0]
    msg = append([][]byte{sender, nil}, msg[1:]...)
    if string(service[:4]) == INTERNAL_SERVICE_PREFIX {
        err = self.serviceInternal(service, msg)
    } else if service, e := self.requireService(string(service)); e == nil {
        err = self.dispatch(service, msg)
    }
    return
}

func (self *mdBroker) processWorker(sender []byte, msg [][]byte) (err error) {
    if len(msg) < 1 {
        err = fmt.Errorf("Invalid msg length: %d", len(msg))
        return
    }

    command, msg := msg[0], msg[1:]
    identity := hex.EncodeToString(sender)
    worker, workerReady := self.workers[identity]
    if !workerReady {
        worker = &refWorker{
            identity: identity,
            address: sender,
            expiry: time.Now().Add(self.workerExpiry),
        }
        self.workers[identity] = worker
    }

    switch string(command) {
    case MDPW_READY:
        if len(msg) < 1 {
            fmt.Errorf("Invalid msg length: %d\n", len(msg))
            return
        }
        service := msg[0]
        if workerReady || string(service[:4]) == INTERNAL_SERVICE_PREFIX {
            err = self.deleteWorker(worker, true)
        } else if service, e := self.requireService(string(service)); e == nil {
            worker.service = service
            err = self.workerWaiting(worker)
        }
    case MDPW_REPLY:
        if workerReady {
            client := msg[0]
            msg = append([][]byte{client, nil, []byte(MDPC_CLIENT), []byte(worker.service.name)}, msg[2:]...)
            err = self.socket.SendMultipart(msg, 0)
            if err != nil {return}
            err = self.workerWaiting(worker)
        } else {
            err = self.deleteWorker(worker, true)
        }
    case MDPW_HEARTBEAT:
        if workerReady {
            worker.expiry = time.Now().Add(self.workerExpiry)
        } else {
            err = self.deleteWorker(worker, true)
        }
    case MDPW_DISCONNECT:
        err = self.deleteWorker(worker, false)
    default:
        err = fmt.Errorf("Invalid command: %X", command)
    }
    return
}

func (self *mdBroker) purgeWorkers() (err error) {
    now := time.Now()
    for elem := self.waiting.Front(); elem != nil; elem = self.waiting.Front() {
        worker, ok := elem.Value.(*refWorker)
        if !ok {
            err = fmt.Errorf("Worker type assertion fail")
            return
        }
        if worker.expiry.After(now) {
            break
        }
        err = self.deleteWorker(worker, false)
        if err != nil {return}
    }
    return
}

func (self *mdBroker) pushError(err error) {
    select {
    case self.errors <- err:
    default:
    }
}

func (self *mdBroker) requireService(name string) (svc *mdService, err error) {
    if len(name) == 0 {
        err = fmt.Errorf("Empty service name")
        return
    }
    svc, ok := self.services[name]
    if !ok {
        svc = &mdService{
            name: name,
            waiting: NewList(),
        }
        self.services[name] = svc
    }
    return
}

func (self *mdBroker) sendToWorker(worker *refWorker, command string, option []byte, msg [][]byte) error {
    if len(option) > 0 {
        msg = append([][]byte{option}, msg...)
    }
    msg = append([][]byte{worker.address, nil, []byte(MDPW_WORKER), []byte(command)}, msg...)

    return self.socket.SendMultipart(msg, 0)
}

func (self *mdBroker) serviceInternal(service []byte, msg [][]byte) error {
    returncode := "501"
    if string(service) == "mmi.service" {
        name := string(msg[len(msg)-1])
        if _, ok := self.services[name]; ok {
            returncode = "200"
        } else {
            returncode = "404"
        }
    }
    msg[len(msg)-1] = []byte(returncode)
    msg = append(append(msg[2:], []byte(MDPC_CLIENT), service), msg[3:]...)
    return self.socket.SendMultipart(msg, 0)
}

func (self *mdBroker) workerWaiting(worker *refWorker) error {
    self.waiting.PushBack(worker)
    worker.service.waiting.PushBack(worker)
    worker.expiry = time.Now().Add(self.workerExpiry)
    return self.dispatch(worker.service, nil)
}

func (self *mdBroker) Close() (err error) {
    if self.socket != nil {
        err = self.socket.Close()
        if err != nil {return}
    }
    self.context.Close()
    return
}

func (self *mdBroker) Errors() chan error {
    return self.errors
}

func (self *mdBroker) Run() {
    var err error
    for {
        items := zmq.PollItems{
            zmq.PollItem{Socket: self.socket, Events: zmq.POLLIN},
        }

        _, err = zmq.Poll(items, self.heartbeatIntv)
        if err != nil {self.pushError(err); continue}

        if item := items[0]; item.REvents&zmq.POLLIN != 0 {
            msg, e := self.socket.RecvMultipart(0)
            if e != nil {self.pushError(e); continue}

            sender := msg[0]
            header := msg[2]
            msg = msg[3:]

            if string(header) == MDPC_CLIENT {
                err = self.processClient(sender, msg)
            } else if string(header) == MDPW_WORKER {
                err = self.processWorker(sender, msg)
            } else {
                err = fmt.Errorf("Invalid header: %s", header)
            }
            if err != nil {self.pushError(err); continue}
        }

        if self.heartbeatAt.Before(time.Now()) {
            err = self.purgeWorkers()
            if err != nil {self.pushError(err); continue}
            for elem := self.waiting.Front(); elem != nil; elem = elem.Next() {
                worker, ok := elem.Value.(*refWorker)
                if !ok {
                    self.pushError(fmt.Errorf("Worker type assertion fail"))
                    continue
                }
                err = self.sendToWorker(worker, MDPW_HEARTBEAT, nil, nil)
                if err != nil {self.pushError(err); continue}
            }
            if err == nil {
                self.heartbeatAt = time.Now().Add(self.heartbeatIntv)
            }
        }
    }
}
