package common

import (
	"encoding"
	"encoding/json"
)

var _ encoding.BinaryMarshaler = new(FileSaveInfo)
var _ encoding.BinaryUnmarshaler = new(FileSaveInfo)

type ResponseData struct {
	Code ResCode     `json:"code"`
	Msg  interface{} `json:"msg"`
	Data interface{} `json:"data"`
}

type ResCode int64

type FileSaveInfo struct {
	BucketName   string `json:"bucket_name"`
	ObjectName   string `json:"object_name"`
	LastModified string `json:"lastModified"`
	Size         int64  `json:"size"`
	Md5          string `json:"md5"`
}

func (m *FileSaveInfo) MarshalBinary() (data []byte, err error) {
	return json.Marshal(m)
}

func (m *FileSaveInfo) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, m)
}

const (
	CodeSuccess ResCode = 1000 + iota
	CodeInternalServerError
	CodeInternalParamsError
	CodeServerBusy
)

var codeMsgMap = map[ResCode]string{
	CodeSuccess:             "success",
	CodeInternalServerError: "内部服务器错误",
	CodeInternalParamsError: "参数错误",
	CodeServerBusy:          "未知错误",
}

func (c ResCode) Msg() string {
	msg, ok := codeMsgMap[c]
	if !ok {
		msg = codeMsgMap[CodeServerBusy]
	}
	return msg
}
