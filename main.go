package main

import (
	"io"
	"minio_demo/common"
	"minio_demo/config"
	"minio_demo/middleware"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/zituocn/logx"
)

func main() {
	config.InitConfig()
	common.InitRedis()
	common.InitMinio()
	mux := http.NewServeMux()
	mux.Handle("/create_bucket", middleware.Cors(http.HandlerFunc(common.CreateBucket)))
	mux.Handle("/remove_bucket", middleware.Cors(http.HandlerFunc(common.RemoveBucket)))
	mux.Handle("/put_object", middleware.Cors(http.HandlerFunc(common.PutObject)))
	mux.Handle("/list_object", middleware.Cors(http.HandlerFunc(common.ListObjects)))
	mux.Handle("/upload", middleware.Cors(http.HandlerFunc(common.Upload)))
	mux.Handle("/download", middleware.Cors(http.HandlerFunc(common.DownLoad)))
	mux.Handle("/get_bucket_list", middleware.Cors(http.HandlerFunc(common.GetBucketList)))
	mux.Handle("/stat_object", middleware.Cors(http.HandlerFunc(common.GetObjectInfo)))
	mux.Handle("/test", middleware.Cors(http.HandlerFunc(common.Test)))
	server := &http.Server{
		Addr:         config.ConfData.Host.Address + ":" + strconv.Itoa(config.ConfData.Host.Port),
		WriteTimeout: time.Second * 300,
		Handler:      mux,
	}
	logx.SetWriter(io.MultiWriter(
		os.Stdout,
		logx.NewFileWriter(logx.FileOptions{
			StorageType: logx.StorageTypeDay,
			MaxDay:      100,
			Dir:         config.ConfData.Log.Path,
			Prefix:      "minio_demo",
		}))).SetColor(false).SetFormat(logx.LogFormatJSON).SetPrefix("minio_demo")
	logx.Info("start server " + strconv.Itoa(config.ConfData.Host.Port))
	logx.Fatal(server.ListenAndServe())
}
