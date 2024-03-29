// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/filecoin-project/lotus/storage/pipeline (interfaces: PreCommitBatcherApi)

// Package mocks is a generated GoMock package.
package mocks

import (
	context "context"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"

	address "github.com/filecoin-project/go-address"
	abi "github.com/filecoin-project/go-state-types/abi"
	big "github.com/filecoin-project/go-state-types/big"
	verifreg "github.com/filecoin-project/go-state-types/builtin/v9/verifreg"
	network "github.com/filecoin-project/go-state-types/network"

	api "github.com/filecoin-project/lotus/api"
	types "github.com/filecoin-project/lotus/chain/types"
)

// MockPreCommitBatcherApi is a mock of PreCommitBatcherApi interface.
type MockPreCommitBatcherApi struct {
	ctrl     *gomock.Controller
	recorder *MockPreCommitBatcherApiMockRecorder
}

// MockPreCommitBatcherApiMockRecorder is the mock recorder for MockPreCommitBatcherApi.
type MockPreCommitBatcherApiMockRecorder struct {
	mock *MockPreCommitBatcherApi
}

// NewMockPreCommitBatcherApi creates a new mock instance.
func NewMockPreCommitBatcherApi(ctrl *gomock.Controller) *MockPreCommitBatcherApi {
	mock := &MockPreCommitBatcherApi{ctrl: ctrl}
	mock.recorder = &MockPreCommitBatcherApiMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockPreCommitBatcherApi) EXPECT() *MockPreCommitBatcherApiMockRecorder {
	return m.recorder
}

// ChainHead mocks base method.
func (m *MockPreCommitBatcherApi) ChainHead(arg0 context.Context) (*types.TipSet, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ChainHead", arg0)
	ret0, _ := ret[0].(*types.TipSet)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ChainHead indicates an expected call of ChainHead.
func (mr *MockPreCommitBatcherApiMockRecorder) ChainHead(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ChainHead", reflect.TypeOf((*MockPreCommitBatcherApi)(nil).ChainHead), arg0)
}

// GasEstimateMessageGas mocks base method.
func (m *MockPreCommitBatcherApi) GasEstimateMessageGas(arg0 context.Context, arg1 *types.Message, arg2 *api.MessageSendSpec, arg3 types.TipSetKey) (*types.Message, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GasEstimateMessageGas", arg0, arg1, arg2, arg3)
	ret0, _ := ret[0].(*types.Message)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GasEstimateMessageGas indicates an expected call of GasEstimateMessageGas.
func (mr *MockPreCommitBatcherApiMockRecorder) GasEstimateMessageGas(arg0, arg1, arg2, arg3 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GasEstimateMessageGas", reflect.TypeOf((*MockPreCommitBatcherApi)(nil).GasEstimateMessageGas), arg0, arg1, arg2, arg3)
}

// MpoolPushMessage mocks base method.
func (m *MockPreCommitBatcherApi) MpoolPushMessage(arg0 context.Context, arg1 *types.Message, arg2 *api.MessageSendSpec) (*types.SignedMessage, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "MpoolPushMessage", arg0, arg1, arg2)
	ret0, _ := ret[0].(*types.SignedMessage)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// MpoolPushMessage indicates an expected call of MpoolPushMessage.
func (mr *MockPreCommitBatcherApiMockRecorder) MpoolPushMessage(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "MpoolPushMessage", reflect.TypeOf((*MockPreCommitBatcherApi)(nil).MpoolPushMessage), arg0, arg1, arg2)
}

// StateAccountKey mocks base method.
func (m *MockPreCommitBatcherApi) StateAccountKey(arg0 context.Context, arg1 address.Address, arg2 types.TipSetKey) (address.Address, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "StateAccountKey", arg0, arg1, arg2)
	ret0, _ := ret[0].(address.Address)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// StateAccountKey indicates an expected call of StateAccountKey.
func (mr *MockPreCommitBatcherApiMockRecorder) StateAccountKey(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StateAccountKey", reflect.TypeOf((*MockPreCommitBatcherApi)(nil).StateAccountKey), arg0, arg1, arg2)
}

// StateGetAllocation mocks base method.
func (m *MockPreCommitBatcherApi) StateGetAllocation(arg0 context.Context, arg1 address.Address, arg2 verifreg.AllocationId, arg3 types.TipSetKey) (*verifreg.Allocation, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "StateGetAllocation", arg0, arg1, arg2, arg3)
	ret0, _ := ret[0].(*verifreg.Allocation)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// StateGetAllocation indicates an expected call of StateGetAllocation.
func (mr *MockPreCommitBatcherApiMockRecorder) StateGetAllocation(arg0, arg1, arg2, arg3 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StateGetAllocation", reflect.TypeOf((*MockPreCommitBatcherApi)(nil).StateGetAllocation), arg0, arg1, arg2, arg3)
}

// StateGetAllocationForPendingDeal mocks base method.
func (m *MockPreCommitBatcherApi) StateGetAllocationForPendingDeal(arg0 context.Context, arg1 abi.DealID, arg2 types.TipSetKey) (*verifreg.Allocation, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "StateGetAllocationForPendingDeal", arg0, arg1, arg2)
	ret0, _ := ret[0].(*verifreg.Allocation)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// StateGetAllocationForPendingDeal indicates an expected call of StateGetAllocationForPendingDeal.
func (mr *MockPreCommitBatcherApiMockRecorder) StateGetAllocationForPendingDeal(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StateGetAllocationForPendingDeal", reflect.TypeOf((*MockPreCommitBatcherApi)(nil).StateGetAllocationForPendingDeal), arg0, arg1, arg2)
}

// StateLookupID mocks base method.
func (m *MockPreCommitBatcherApi) StateLookupID(arg0 context.Context, arg1 address.Address, arg2 types.TipSetKey) (address.Address, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "StateLookupID", arg0, arg1, arg2)
	ret0, _ := ret[0].(address.Address)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// StateLookupID indicates an expected call of StateLookupID.
func (mr *MockPreCommitBatcherApiMockRecorder) StateLookupID(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StateLookupID", reflect.TypeOf((*MockPreCommitBatcherApi)(nil).StateLookupID), arg0, arg1, arg2)
}

// StateMinerAvailableBalance mocks base method.
func (m *MockPreCommitBatcherApi) StateMinerAvailableBalance(arg0 context.Context, arg1 address.Address, arg2 types.TipSetKey) (big.Int, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "StateMinerAvailableBalance", arg0, arg1, arg2)
	ret0, _ := ret[0].(big.Int)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// StateMinerAvailableBalance indicates an expected call of StateMinerAvailableBalance.
func (mr *MockPreCommitBatcherApiMockRecorder) StateMinerAvailableBalance(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StateMinerAvailableBalance", reflect.TypeOf((*MockPreCommitBatcherApi)(nil).StateMinerAvailableBalance), arg0, arg1, arg2)
}

// StateMinerInfo mocks base method.
func (m *MockPreCommitBatcherApi) StateMinerInfo(arg0 context.Context, arg1 address.Address, arg2 types.TipSetKey) (api.MinerInfo, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "StateMinerInfo", arg0, arg1, arg2)
	ret0, _ := ret[0].(api.MinerInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// StateMinerInfo indicates an expected call of StateMinerInfo.
func (mr *MockPreCommitBatcherApiMockRecorder) StateMinerInfo(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StateMinerInfo", reflect.TypeOf((*MockPreCommitBatcherApi)(nil).StateMinerInfo), arg0, arg1, arg2)
}

// StateNetworkVersion mocks base method.
func (m *MockPreCommitBatcherApi) StateNetworkVersion(arg0 context.Context, arg1 types.TipSetKey) (network.Version, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "StateNetworkVersion", arg0, arg1)
	ret0, _ := ret[0].(network.Version)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// StateNetworkVersion indicates an expected call of StateNetworkVersion.
func (mr *MockPreCommitBatcherApiMockRecorder) StateNetworkVersion(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StateNetworkVersion", reflect.TypeOf((*MockPreCommitBatcherApi)(nil).StateNetworkVersion), arg0, arg1)
}

// WalletBalance mocks base method.
func (m *MockPreCommitBatcherApi) WalletBalance(arg0 context.Context, arg1 address.Address) (big.Int, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "WalletBalance", arg0, arg1)
	ret0, _ := ret[0].(big.Int)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// WalletBalance indicates an expected call of WalletBalance.
func (mr *MockPreCommitBatcherApiMockRecorder) WalletBalance(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "WalletBalance", reflect.TypeOf((*MockPreCommitBatcherApi)(nil).WalletBalance), arg0, arg1)
}

// WalletHas mocks base method.
func (m *MockPreCommitBatcherApi) WalletHas(arg0 context.Context, arg1 address.Address) (bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "WalletHas", arg0, arg1)
	ret0, _ := ret[0].(bool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// WalletHas indicates an expected call of WalletHas.
func (mr *MockPreCommitBatcherApiMockRecorder) WalletHas(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "WalletHas", reflect.TypeOf((*MockPreCommitBatcherApi)(nil).WalletHas), arg0, arg1)
}
