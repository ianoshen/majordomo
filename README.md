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
    client := md.NewClient("tcp://localhost:5555")
    defer client.Close()

    count := 0
    for ; count < 1e5; count += 1 {
        request := [][]byte{[]byte("Hello world")}
        reply, _ := client.Send([]byte("echo"), request)
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
    "fmt"
    md "github.com/ianoshen/majordomo"
)

func main() {
    broker, _ := NewBroker("tcp://*:5555")
    defer broker.Close()
    go fmt.Println(<-broker.Errors())
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
    worker, _ := NewWorker("tcp://localhost:5555", "echo")
    for reply := [][]byte{};;{
        request, _ := worker.Recv(reply)
        if len(request) == 0 { break }
        reply = request
    }
}
```
