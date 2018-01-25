// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/SimonRichardson/coherence/pkg/store (interfaces: Store)

// Package mocks is a generated GoMock package.
package mocks

import (
	selectors "github.com/SimonRichardson/coherence/pkg/selectors"
	gomock "github.com/golang/mock/gomock"
	reflect "reflect"
)

// MockStore is a mock of Store interface
type MockStore struct {
	ctrl     *gomock.Controller
	recorder *MockStoreMockRecorder
}

// MockStoreMockRecorder is the mock recorder for MockStore
type MockStoreMockRecorder struct {
	mock *MockStore
}

// NewMockStore creates a new mock instance
func NewMockStore(ctrl *gomock.Controller) *MockStore {
	mock := &MockStore{ctrl: ctrl}
	mock.recorder = &MockStoreMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockStore) EXPECT() *MockStoreMockRecorder {
	return m.recorder
}

// Delete mocks base method
func (m *MockStore) Delete(arg0 selectors.Key, arg1 []selectors.FieldValueScore) (selectors.ChangeSet, error) {
	ret := m.ctrl.Call(m, "Delete", arg0, arg1)
	ret0, _ := ret[0].(selectors.ChangeSet)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Delete indicates an expected call of Delete
func (mr *MockStoreMockRecorder) Delete(arg0, arg1 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Delete", reflect.TypeOf((*MockStore)(nil).Delete), arg0, arg1)
}

// Insert mocks base method
func (m *MockStore) Insert(arg0 selectors.Key, arg1 []selectors.FieldValueScore) (selectors.ChangeSet, error) {
	ret := m.ctrl.Call(m, "Insert", arg0, arg1)
	ret0, _ := ret[0].(selectors.ChangeSet)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Insert indicates an expected call of Insert
func (mr *MockStoreMockRecorder) Insert(arg0, arg1 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Insert", reflect.TypeOf((*MockStore)(nil).Insert), arg0, arg1)
}

// Keys mocks base method
func (m *MockStore) Keys() ([]selectors.Key, error) {
	ret := m.ctrl.Call(m, "Keys")
	ret0, _ := ret[0].([]selectors.Key)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Keys indicates an expected call of Keys
func (mr *MockStoreMockRecorder) Keys() *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Keys", reflect.TypeOf((*MockStore)(nil).Keys))
}

// Members mocks base method
func (m *MockStore) Members(arg0 selectors.Key) ([]selectors.Field, error) {
	ret := m.ctrl.Call(m, "Members", arg0)
	ret0, _ := ret[0].([]selectors.Field)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Members indicates an expected call of Members
func (mr *MockStoreMockRecorder) Members(arg0 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Members", reflect.TypeOf((*MockStore)(nil).Members), arg0)
}

// Score mocks base method
func (m *MockStore) Score(arg0 selectors.Key, arg1 selectors.Field) (selectors.Presence, error) {
	ret := m.ctrl.Call(m, "Score", arg0, arg1)
	ret0, _ := ret[0].(selectors.Presence)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Score indicates an expected call of Score
func (mr *MockStoreMockRecorder) Score(arg0, arg1 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Score", reflect.TypeOf((*MockStore)(nil).Score), arg0, arg1)
}

// Select mocks base method
func (m *MockStore) Select(arg0 selectors.Key, arg1 selectors.Field) (selectors.FieldValueScore, error) {
	ret := m.ctrl.Call(m, "Select", arg0, arg1)
	ret0, _ := ret[0].(selectors.FieldValueScore)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Select indicates an expected call of Select
func (mr *MockStoreMockRecorder) Select(arg0, arg1 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Select", reflect.TypeOf((*MockStore)(nil).Select), arg0, arg1)
}

// Size mocks base method
func (m *MockStore) Size(arg0 selectors.Key) (int64, error) {
	ret := m.ctrl.Call(m, "Size", arg0)
	ret0, _ := ret[0].(int64)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Size indicates an expected call of Size
func (mr *MockStoreMockRecorder) Size(arg0 interface{}) *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Size", reflect.TypeOf((*MockStore)(nil).Size), arg0)
}

// String mocks base method
func (m *MockStore) String() string {
	ret := m.ctrl.Call(m, "String")
	ret0, _ := ret[0].(string)
	return ret0
}

// String indicates an expected call of String
func (mr *MockStoreMockRecorder) String() *gomock.Call {
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "String", reflect.TypeOf((*MockStore)(nil).String))
}
