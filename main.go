package main

import (
	"log"
	"minio_demo/common"
	"minio_demo/middleware"
	"net/http"
	"os"
	"time"
)

func main() {

	mux := http.NewServeMux()
	// mux.HandleFunc("/get_object", common.GetObject)

	mux.Handle("/create_bucket", middleware.Cors(http.HandlerFunc(common.CreateBucket)))
	mux.Handle("/remove_bucket", middleware.Cors(http.HandlerFunc(common.RemoveBucket)))
	mux.Handle("/put_object", middleware.Cors(http.HandlerFunc(common.PutObject)))
	mux.Handle("/list_object", middleware.Cors(http.HandlerFunc(common.ListObjects)))
	mux.Handle("/put_object_demo", middleware.Cors(http.HandlerFunc(common.PutObjectDemo)))
	mux.Handle("/download", middleware.Cors(http.HandlerFunc(common.DownLoad)))
	mux.Handle("/get_bucket_list", middleware.Cors(http.HandlerFunc(common.GetBucketList)))
	server := &http.Server{
		Addr:         "127.0.0.1:8800",
		WriteTimeout: time.Second * 300,
		Handler:      mux,
	}
	fs, _ := os.Create("./logs/logs.txt")
	log.SetOutput(fs)
	log.Println("start server 8800")
	log.Fatal(server.ListenAndServe())
}
