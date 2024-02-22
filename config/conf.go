package config

import (
	"fmt"
	"os"

	"sigs.k8s.io/yaml"
)

type Config struct {
	Log   Log
	Host  Host
	Minio Minio
}

type Log struct {
	Path string
}

type Host struct {
	Port    int
	Address string
}

type Minio struct {
	Port    int
	Address string

	AccessKeyID     string
	SecretAccessKey string
}

var ConfData = &Config{}

func InitConfig() {
	yamlFile, err := os.ReadFile("etc/conf.yaml")
	if err != nil {
		fmt.Println("ReadFile" + err.Error())
	}
	// 将读取的yaml文件解析为响应的 struct
	err = yaml.Unmarshal(yamlFile, &ConfData)
	if err != nil {
		fmt.Println("Unmarshal err:" + err.Error())
	}
}
