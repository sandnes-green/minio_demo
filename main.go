package main

import (
	"log"
	"minio_demo/common"
	"net/http"
	"os"
	"time"
)

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", r.Header.Get("Origin"))

		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE,UPDATE") // 服务器支持的所有跨域请求的方法,为了避免浏览次请求的多次'预检'请求
		//  header的类型
		w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Length, X-CSRF-Token, Token,session,X_Requested_With,Accept, Origin, Host, Connection, Accept-Encoding, Accept-Language,DNT, X-CustomHeader, Keep-Alive, User-Agent, X-Requested-With, If-Modified-Since, Cache-Control, Content-Type, Pragma")
		// 允许跨域设置                                                                                                      可以返回其他子段
		w.Header().Set("Access-Control-Expose-Headers", "Content-Length, Access-Control-Allow-Origin, Access-Control-Allow-Headers,Cache-Control,Content-Language,Content-Type,Expires,Last-Modified,Pragma,FooBar") // 跨域关键设置 让浏览器可以解析
		w.Header().Set("Access-Control-Max-Age", "172800")                                                                                                                                                           // 缓存请求信息 单位为秒
		w.Header().Set("Access-Control-Allow-Credentials", "true")                                                                                                                                                   //  跨域请求是否需要带cookie信息 默认设置为true
		w.Header().Set("content-type", "application/json")

		next.ServeHTTP(w, r)
	})
}

func main() {

	mux := http.NewServeMux()

	mux.HandleFunc("/put_object", common.PutObject)
	mux.HandleFunc("/create_bucket", common.CreateBucket)
	mux.HandleFunc("/remove_bucket", common.RemoveBucket)
	mux.HandleFunc("/get_object", common.GetObject)

	mux.Handle("/list_object", corsMiddleware(http.HandlerFunc(common.ListObjects)))
	mux.Handle("/put_object_demo", corsMiddleware(http.HandlerFunc(common.PutObjectDemo)))
	mux.Handle("/download", corsMiddleware(http.HandlerFunc(common.DownLoad)))
	mux.Handle("/get_bucket_list", corsMiddleware(http.HandlerFunc(common.GetBucketList)))
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
