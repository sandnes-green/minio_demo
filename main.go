package main

import (
	"log"
	"minio_demo/common"
	"minio_demo/config"
	"minio_demo/middleware"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	config.InitConfig()
	common.InitMinio()
	mux := http.NewServeMux()
	mux.Handle("/create_bucket", middleware.Cors(http.HandlerFunc(common.CreateBucket)))
	mux.Handle("/remove_bucket", middleware.Cors(http.HandlerFunc(common.RemoveBucket)))
	mux.Handle("/put_object", middleware.Cors(http.HandlerFunc(common.PutObject)))
	mux.Handle("/list_object", middleware.Cors(http.HandlerFunc(common.ListObjects)))
	mux.Handle("/upload", middleware.Cors(http.HandlerFunc(common.Upload)))
	mux.Handle("/download", middleware.Cors(http.HandlerFunc(common.DownLoad)))
	mux.Handle("/get_bucket_list", middleware.Cors(http.HandlerFunc(common.GetBucketList)))
	server := &http.Server{
		Addr:         config.ConfData.Host.Address + ":" + strconv.Itoa(config.ConfData.Host.Port),
		WriteTimeout: time.Second * 300,
		Handler:      mux,
	}
	fs, _ := os.Create(config.ConfData.Log.Path)
	log.SetOutput(fs)
	log.Println("start server " + strconv.Itoa(config.ConfData.Host.Port))
	log.Fatal(server.ListenAndServe())
}
