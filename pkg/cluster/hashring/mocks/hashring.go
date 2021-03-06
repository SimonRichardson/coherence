// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/SimonRichardson/coherence/pkg/cluster/hashring (interfaces: Snapshot)

// Package mocks is a generated GoMock package.
package mocks

import (
	nodes "github.com/SimonRichardson/coherence/pkg/cluster/nodes"
	selectors "github.com/SimonRichardson/coherence/pkg/selectors"
	gomock "github.com/golang/mock/gomock"
	reflect "reflect"
)

// MockSnapshot is a mock of Snapshot interface
type MockSnapshot struct {
	ctrl     *gomock.Controller
	recorder *MockSnapshotMockRecorder
}

// MockSnapshotMockRecorder is the mock recorder for MockSnapshot
type MockSnapshotMockRecorder struct {
	mock *MockSnapshot
}

// NewMockSnapshot creates a new mock instance
func NewMockSnapshot(ctrl *gomock.Controller) *MockSnapshot {
	mock := &MockSnapshot{ctrl: ctrl}
	mock.recorder = &MockSnapshotMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockSnapshot) EXPECT() *MockSnapshotMockRecorder {
	return m.recorder
}

// Read mocks base method
func (m *MockSnapshot) Read(arg0 selectors.Key, arg1 selectors.Quorum) []nodes.Node {
	ret := m.ctrl.Call(m, "Read", arg0, arg1)
	ret0, _ := ret[0].([]nodes.Node)
	return ret0
}

// Read indicates an expected call of Read
func (mr *MockSnapshotMockRecorder) Read(arg0, arg1 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Read", reflect.TypeOf((*MockSnapshot)(nil).Read), arg0, arg1)
}

// Write mocks base method
func (m *MockSnapshot) Write(arg0 selectors.Key, arg1 selectors.Quorum) ([]nodes.Node, func([]uint32) error) {
	ret := m.ctrl.Call(m, "Write", arg0, arg1)
	ret0, _ := ret[0].([]nodes.Node)
	ret1, _ := ret[1].(func([]uint32) error)
	return ret0, ret1
}

// Write indicates an expected call of Write
func (mr *MockSnapshotMockRecorder) Write(arg0, arg1 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Write", reflect.TypeOf((*MockSnapshot)(nil).Write), arg0, arg1)
}
