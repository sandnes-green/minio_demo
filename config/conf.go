package config

import (
	"os"

	"github.com/zituocn/logx"
	"sigs.k8s.io/yaml"
)

type Env struct {
	Dev  Config
	Test Config
	Prod Config
}

type Config struct {
	Log   Log
	Host  Host
	Redis Redis
	Minio Minio
}

type Log struct {
	Path string
}

type Host struct {
	Port    int
	Address string
}

type Redis struct {
	Port     int
	Address  string
	Password string
}

type Minio struct {
	Port    int
	Address string

	AccessKeyID     string
	SecretAccessKey string
}

var EnvData = &Env{}
var ConfData = &Config{}

func InitConfig() {
	env := os.Getenv("GO_ENV")
	if env == "" {
		env = "dev"
	}
	yamlFile, err := os.ReadFile("etc/conf.yaml")
	if err != nil {
		logx.Error("ReadFile" + err.Error())
	}
	// 将读取的yaml文件解析为响应的 struct
	err = yaml.Unmarshal(yamlFile, &EnvData)
	if err != nil {
		logx.Error("Unmarshal err:" + err.Error())
	}
	switch env {
	case "dev":
		ConfData = &EnvData.Dev
	case "test":
		ConfData = &EnvData.Test
	case "prod":
		ConfData = &EnvData.Prod
	default:
		ConfData = &EnvData.Dev
	}
}
