// Automatically generated by MockGen. DO NOT EDIT!
// Source: fs.go

package fs

import (
	gomock "github.com/golang/mock/gomock"
	common "github.com/cloud9-tools/go-cas/common"
)

// Mock of FileSystem interface
type MockFileSystem struct {
	ctrl     *gomock.Controller
	recorder *_MockFileSystemRecorder
}

// Recorder for MockFileSystem (not exported)
type _MockFileSystemRecorder struct {
	mock *MockFileSystem
}

func NewMockFileSystem(ctrl *gomock.Controller) *MockFileSystem {
	mock := &MockFileSystem{ctrl: ctrl}
	mock.recorder = &_MockFileSystemRecorder{mock}
	return mock
}

func (_m *MockFileSystem) EXPECT() *_MockFileSystemRecorder {
	return _m.recorder
}

func (_m *MockFileSystem) OpenMetadata(_param0 WriteType) (File, error) {
	ret := _m.ctrl.Call(_m, "OpenMetadata", _param0)
	ret0, _ := ret[0].(File)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockFileSystemRecorder) OpenMetadata(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "OpenMetadata", arg0)
}

func (_m *MockFileSystem) OpenMetadataBackup(_param0 WriteType) (File, error) {
	ret := _m.ctrl.Call(_m, "OpenMetadataBackup", _param0)
	ret0, _ := ret[0].(File)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockFileSystemRecorder) OpenMetadataBackup(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "OpenMetadataBackup", arg0)
}

func (_m *MockFileSystem) OpenData(_param0 WriteType) (BlockFile, error) {
	ret := _m.ctrl.Call(_m, "OpenData", _param0)
	ret0, _ := ret[0].(BlockFile)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockFileSystemRecorder) OpenData(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "OpenData", arg0)
}

// Mock of File interface
type MockFile struct {
	ctrl     *gomock.Controller
	recorder *_MockFileRecorder
}

// Recorder for MockFile (not exported)
type _MockFileRecorder struct {
	mock *MockFile
}

func NewMockFile(ctrl *gomock.Controller) *MockFile {
	mock := &MockFile{ctrl: ctrl}
	mock.recorder = &_MockFileRecorder{mock}
	return mock
}

func (_m *MockFile) EXPECT() *_MockFileRecorder {
	return _m.recorder
}

func (_m *MockFile) Name() string {
	ret := _m.ctrl.Call(_m, "Name")
	ret0, _ := ret[0].(string)
	return ret0
}

func (_mr *_MockFileRecorder) Name() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Name")
}

func (_m *MockFile) Close() error {
	ret := _m.ctrl.Call(_m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockFileRecorder) Close() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Close")
}

func (_m *MockFile) ReadContents() ([]byte, error) {
	ret := _m.ctrl.Call(_m, "ReadContents")
	ret0, _ := ret[0].([]byte)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

func (_mr *_MockFileRecorder) ReadContents() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "ReadContents")
}

func (_m *MockFile) WriteContents(_param0 []byte) error {
	ret := _m.ctrl.Call(_m, "WriteContents", _param0)
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockFileRecorder) WriteContents(arg0 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "WriteContents", arg0)
}

// Mock of BlockFile interface
type MockBlockFile struct {
	ctrl     *gomock.Controller
	recorder *_MockBlockFileRecorder
}

// Recorder for MockBlockFile (not exported)
type _MockBlockFileRecorder struct {
	mock *MockBlockFile
}

func NewMockBlockFile(ctrl *gomock.Controller) *MockBlockFile {
	mock := &MockBlockFile{ctrl: ctrl}
	mock.recorder = &_MockBlockFileRecorder{mock}
	return mock
}

func (_m *MockBlockFile) EXPECT() *_MockBlockFileRecorder {
	return _m.recorder
}

func (_m *MockBlockFile) Name() string {
	ret := _m.ctrl.Call(_m, "Name")
	ret0, _ := ret[0].(string)
	return ret0
}

func (_mr *_MockBlockFileRecorder) Name() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Name")
}

func (_m *MockBlockFile) Close() error {
	ret := _m.ctrl.Call(_m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockBlockFileRecorder) Close() *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "Close")
}

func (_m *MockBlockFile) ReadBlock(blknum uint32, block *common.Block) error {
	ret := _m.ctrl.Call(_m, "ReadBlock", blknum, block)
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockBlockFileRecorder) ReadBlock(arg0, arg1 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "ReadBlock", arg0, arg1)
}

func (_m *MockBlockFile) WriteBlock(blknum uint32, block *common.Block) error {
	ret := _m.ctrl.Call(_m, "WriteBlock", blknum, block)
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockBlockFileRecorder) WriteBlock(arg0, arg1 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "WriteBlock", arg0, arg1)
}

func (_m *MockBlockFile) EraseBlock(blknum uint32, shred bool) error {
	ret := _m.ctrl.Call(_m, "EraseBlock", blknum, shred)
	ret0, _ := ret[0].(error)
	return ret0
}

func (_mr *_MockBlockFileRecorder) EraseBlock(arg0, arg1 interface{}) *gomock.Call {
	return _mr.mock.ctrl.RecordCall(_mr.mock, "EraseBlock", arg0, arg1)
}
