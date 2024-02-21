package common

import (
	"time"

	"github.com/go-redis/redis"
)

var redisdb *redis.Client

func init() {
	redisdb = redis.NewClient(&redis.Options{
		Addr:         "192.168.3.11:6379",
		Password:     "25892326-cfWF",
		DialTimeout:  10 * time.Second,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		PoolSize:     10,
		PoolTimeout:  30 * time.Second,
	})
}

func GetClient() *redis.Client {
	return redisdb
}
