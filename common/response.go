package common

type ResponseData struct {
	Code ResCode     `json:"code"`
	Msg  interface{} `json:"msg"`
	Data interface{} `json:"data"`
}

type ResCode int64

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
