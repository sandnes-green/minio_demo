package common

import (
	"encoding"
	"encoding/json"
)

var _ encoding.BinaryMarshaler = new(FileSaveInfo)
var _ encoding.BinaryUnmarshaler = new(FileSaveInfo)

type FileSaveInfo struct {
	BucketName string `json:"bucket_name"`
	ObjectName string `json:"object_name"`
}

func (m *FileSaveInfo) MarshalBinary() (data []byte, err error) {
	return json.Marshal(m)
}

func (m *FileSaveInfo) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, m)
}
