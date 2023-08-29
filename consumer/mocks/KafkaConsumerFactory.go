// Code generated by mockery v2.33.0. DO NOT EDIT.

package mocks

import (
	adapter "github.com/AeroAgency/go-kafka/adapters"

	kafka "github.com/confluentinc/confluent-kafka-go/kafka"

	mock "github.com/stretchr/testify/mock"
)

// KafkaConsumerFactory is an autogenerated mock type for the KafkaConsumerFactory type
type KafkaConsumerFactory struct {
	mock.Mock
}

// NewConsumer provides a mock function with given fields: configMap
func (_m *KafkaConsumerFactory) NewConsumer(configMap *kafka.ConfigMap) (adapter.Consumer, error) {
	ret := _m.Called(configMap)

	var r0 adapter.Consumer
	var r1 error
	if rf, ok := ret.Get(0).(func(*kafka.ConfigMap) (adapter.Consumer, error)); ok {
		return rf(configMap)
	}
	if rf, ok := ret.Get(0).(func(*kafka.ConfigMap) adapter.Consumer); ok {
		r0 = rf(configMap)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(adapter.Consumer)
		}
	}

	if rf, ok := ret.Get(1).(func(*kafka.ConfigMap) error); ok {
		r1 = rf(configMap)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// NewKafkaConsumerFactory creates a new instance of KafkaConsumerFactory. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewKafkaConsumerFactory(t interface {
	mock.TestingT
	Cleanup(func())
}) *KafkaConsumerFactory {
	mock := &KafkaConsumerFactory{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}