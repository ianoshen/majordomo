// +build !mock

package majordomo

func NewBroker(endpoint string) (broker Broker, err error) {
    return newBroker(endpoint)
}

func NewWorker(broker, service string) (Worker, error) {
    return newWorker(broker, service)
}

func NewClient(broker string) (Client, error) {
    return newClient(broker)
}
