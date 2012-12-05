#Majordomo#

Based on [Majordomo Protocol 0.1](http://rfc.zeromq.org/spec:7)

Reqirement: [gozmq](http://github.com/alecthomas/gozmq)

##TODO##

* broker has not been tested yet
* add unit-test

##Examples##

###Client Example###

```go
package main

import (
    "os"
    md "github.com/ianoshen/majordomo"
)

func main() {
    verbose := len(os.Args) >=2 && os.Args[1] == "-v"
    client, _ := md.NewClient("tcp://localhost:5555", verbose)
    defer client.Close()

    count := 0
    for ; count < 1e5; count += 1 {
        request := [][]byte{[]byte("Hello world")}
        reply := client.Send([]byte("echo"), request)
        if len(reply) == 0 { break }
    }

    fmt.Printf("%d requests/replies processed\n", count)
}
```

###Broker Example###

```go
package main

import (
    "os"
    md "github.com/ianoshen/majordomo"
)

func main() {
    verbose := len(os.Args) >=2 && os.Args[1] == "-v"
    broker, _ := NewBroker("tcp://*:5555", verbose)
    defer broker.Close()
    broker.Run()
}
```

###Worker Example###

```go
package main

import (
    "os"
    md "github.com/ianoshen/majordomo"
)

func main() {
    verbose := len(os.Args) >=2 && os.Args[1] == "-v"
    worker, _ := NewWorker("tcp://localhost:5555", "echo", verbose)
    for reply := [][]byte{};;{
        request := worker.Recv(reply)
        if len(request) == 0 { break }
        reply = request
    }
}
```
