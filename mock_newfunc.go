// +build mock

package majordomo

import (
    "code.dapps.douban.com/gomock/gomock"
    "time"
)

var (
    MockBrokerCtrl,
    MockWorkerCtrl,
    MockClientCtrl *gomock.Controller
)

func NewBroker(endpoint string, heartbeatIntv, workerExpiry time.Duration) (broker Broker, err error) {
    return NewMockBroker(MockBrokerCtrl), nil
}

func NewWorker(broker, service string, heartbeatIntv, reconnectIntv time.Duration, retries int) (Worker, error) {
    return NewMockWorker(MockWorkerCtrl), nil
}

func NewClient(broker string, retries int, timeout time.Duration) (Client, error) {
    return NewMockClient(MockClientCtrl), nil
}
