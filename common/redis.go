package common

import (
	"fmt"
	"minio_demo/config"
	"strconv"
	"time"

	"github.com/go-redis/redis"
)

var redisdb *redis.Client

func InitRedis() {
	redisdb = redis.NewClient(&redis.Options{
		Addr:         config.ConfData.Redis.Address + ":" + strconv.Itoa(config.ConfData.Redis.Port),
		Password:     config.ConfData.Redis.Password,
		DialTimeout:  10 * time.Second,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		PoolSize:     10,
		PoolTimeout:  30 * time.Second,
	})
	fmt.Println(config.ConfData.Redis.Address + ":" + strconv.Itoa(config.ConfData.Redis.Port))
}

func GetClient() *redis.Client {
	return redisdb
}
