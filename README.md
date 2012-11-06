#Majordomo#

Based on [Majordomo Protocol 0.1](http://rfc.zeromq.org/spec:7)

Reqirement: [gozmq](http://github.com/alecthomas/gozmq)

##TODO##

* broker has not been tested yet
* add unit-test

##Examples##

###Client Example###

<pre><code>
package main

import (
    "os"
    md "github.com/ianoshen/majordomo"
)

func main() {
    verbose := os.Args[1] == "-v"
    client := md.NewClient("tcp://localhost:5555", verbose)
    defer client.Close()

    count := 0
    for ; count < 1e5; count += 1 {
        request := [][]byte{[]byte("Hello world")}
        reply := client.Send([]byte("echo"), request)
        if len(reply) == 0 { break }
    }

    fmt.Printf("%d requests/replies processed\n", count)
}
</code></pre>

###Broker Example###

<pre><code>
package main

import (
    "os"
    md "github.com/ianoshen/majordomo"
)

func main() {
    verbose := os.Args[1] == "-v"
    broker := NewBroker("tcp://*:5555", verbose)
    defer broker.Close()
    broker.Run()
}
</code></pre>

###Worker Example###

<pre><code>
package main

import (
    "os"
    md "github.com/ianoshen/majordomo"
)

func main() {
    worker := NewWorker("tcp://localhost:5555", "echo", true)
    for reply := [][]byte{};;{
        request := worker.Recv(reply)
        if len(request) == 0 { break }
        reply = request
    }
}
</code></pre>
