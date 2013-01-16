// +build mock

package majordomo

import (
    "code.google.com/p/gomock/gomock"
)

var (
    MockBrokerCtrl,
    MockWorkerCtrl,
    MockClientCtrl *gomock.Controller
)

func NewBroker(endpoint string) (broker Broker, err error) {
    return NewMockBroker(MockBrokerCtrl), nil
}

func NewWorker(broker, service string) (Worker, error) {
    return NewMockWorker(MockWorkerCtrl), nil
}

func NewClient(broker string) (Client, error) {
    return NewMockClient(MockClientCtrl), nil
}
