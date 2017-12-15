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

// Snapshot mocks base method
func (m *MockSnapshot) Snapshot(arg0 selectors.Key, arg1 selectors.Quorum) []nodes.Node {
	ret := m.ctrl.Call(m, "Snapshot", arg0, arg1)
	ret0, _ := ret[0].([]nodes.Node)
	return ret0
}

// Snapshot indicates an expected call of Snapshot
func (mr *MockSnapshotMockRecorder) Snapshot(arg0, arg1 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Snapshot", reflect.TypeOf((*MockSnapshot)(nil).Snapshot), arg0, arg1)
}
