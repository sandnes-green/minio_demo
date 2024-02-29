package common

import (
	"fmt"
	"io"
	"log"
	"minio_demo/config"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/minio/minio-go"
	"github.com/zeromicro/go-zero/rest/httpx"
	"github.com/zituocn/logx"
)

var (
	client        *minio.Client
	is_exists_key map[string]map[string]bool
	muxing        map[string]bool

	mu sync.RWMutex
)

type SrcInfo struct {
	Etag string
	Name string
}

type BucketInfo struct {
	Name       string
	CreateTime string
}

type ObjectInfo struct {
	ETag string `json:"etag"`

	Key          string `json:"name"`         // Name of the object
	LastModified string `json:"lastModified"` // Date and time the object was last modified.
	Size         int64  `json:"size"`         // Size in bytes of the object.
	ContentType  string `json:"contentType"`  // A standard MIME type describing the format of the object data.

	// Collection of additional metadata on the object.
	// eg: x-amz-meta-*, content-encoding etc.
	Metadata http.Header `json:"metadata" xml:"-"`

	// Owner name.
	Owner struct {
		DisplayName string `json:"name"`
		ID          string `json:"id"`
	} `json:"owner"`

	// The class of storage used to store the object.
	StorageClass string `json:"storageClass"`
}

func InitMinio() {
	is_exists_key = make(map[string]map[string]bool, 0)
	muxing = make(map[string]bool, 0)
	client = InitMinioClient()
}

func InitMinioClient() *minio.Client {
	// 基本的配置信息
	endpoint := config.ConfData.Minio.Address + ":" + strconv.Itoa(config.ConfData.Minio.Port)
	accessKeyID := config.ConfData.Minio.AccessKeyID
	secretAccessKey := config.ConfData.Minio.SecretAccessKey
	minioClient, err := minio.New(endpoint, accessKeyID, secretAccessKey, false)
	if err != nil {
		logx.Fatalf("初始化MinioClient错误：%s", err.Error())
	} else {
		logx.Info("Minio client start")
	}
	return minioClient
}

// 创建桶
func Test(w http.ResponseWriter, r *http.Request) {
	x, err := redisdb.Incr("minio").Result()
	fmt.Println("x==", x)
	fmt.Println("err==", err)
	x, err = redisdb.Incr("minio").Result()
	fmt.Println("x==", x)
	fmt.Println("err==", err)
	var a int
	str, _ := redisdb.Get("minio").Result()
	fmt.Println("a==", a)
	fmt.Println("str==", str)

	redisdb.Set("minio", 1, 0)
	x, err = redisdb.Incr("minio").Result()
	fmt.Println("x==", x)
	fmt.Println("err==", err)

	redisdb.Get("minio").Scan(&a)
	fmt.Println("a==1:", a == 1)

	redisdb.Del("minio")
}

// 创建桶
func CreateBucket(w http.ResponseWriter, r *http.Request) {
	bucketname := r.PostFormValue("bucket_name")

	err := client.MakeBucket(bucketname, "")
	if err != nil {
		httpx.OkJson(w, ResponseData{
			Code: CodeInternalParamsError,
			Msg:  "创建桶失败",
		})
		return
	}
	httpx.OkJson(w, ResponseData{
		Code: CodeSuccess,
		Msg:  "创建桶成功",
	})
}

// 查询对象
func GetObjectInfo(w http.ResponseWriter, r *http.Request) {
	bucketname := r.PostFormValue("bucket_name")
	objectname := r.PostFormValue("object_name")

	info, _ := GetStatObject(bucketname, objectname)
	httpx.OkJson(w, info)
}

// 展示桶列表
func GetBucketList(w http.ResponseWriter, r *http.Request) {
	lists, err := client.ListBuckets()

	bucket_list := make([]BucketInfo, 0, len(lists))

	for _, v := range lists {
		info := BucketInfo{
			Name:       v.Name,
			CreateTime: v.CreationDate.Format("2006-01-02 15:04:05"),
		}
		bucket_list = append(bucket_list, info)
	}
	if err != nil {
		httpx.OkJson(w, ResponseData{
			Code: CodeInternalParamsError,
			Msg:  err.Error(),
		})
		return
	}

	httpx.OkJson(w, ResponseData{
		Code: CodeSuccess,
		Msg:  "success",
		Data: bucket_list,
	})
}

// 移除桶
func RemoveBucket(w http.ResponseWriter, r *http.Request) {
	bucketname := r.PostFormValue("bucket_name")
	isExist, err := IsBuckets(bucketname)

	if err != nil {
		httpx.OkJson(w, ResponseData{
			Code: CodeInternalParamsError,
			Msg:  "Invalid params",
		})
		return
	}

	if !isExist {
		httpx.OkJson(w, ResponseData{
			Code: CodeInternalParamsError,
			Msg:  "Invalid params",
		})
		return
	}

	err = client.RemoveBucket(bucketname)
	if err != nil {
		httpx.OkJson(w, ResponseData{
			Code: CodeInternalServerError,
			Msg:  err.Error(),
		})
		return
	}

	httpx.OkJson(w, ResponseData{
		Code: CodeSuccess,
		Msg:  fmt.Sprintf("删除%s桶成功", bucketname),
	})
}

// 展示对象
func ListObjects(w http.ResponseWriter, r *http.Request) {
	bucketname := r.PostFormValue("bucket_name")
	objectname := r.PostFormValue("object_name")
	isExist, err := IsBuckets(bucketname)

	if err != nil {
		logx.Error("IsBuckets error:", err.Error())
		httpx.OkJson(w, ResponseData{
			Code: CodeInternalParamsError,
			Msg:  err.Error(),
			Data: nil,
		})
	}

	if !isExist {
		logx.Error("bucket not found")
		httpx.OkJson(w, ResponseData{
			Code: CodeInternalParamsError,
			Msg:  "bucket not found",
			Data: nil,
		})
		return
	}

	doneCh := make(chan struct{})
	defer close(doneCh)

	objectInfos := make([]*FileSaveInfo, 0)

	for message := range client.ListObjects(bucketname, objectname, true, doneCh) {
		objectInfo := &FileSaveInfo{
			BucketName:   bucketname,
			ObjectName:   message.Key,
			LastModified: message.LastModified.Format("2006-01-02 15:04:05"),
			Size:         message.Size,
			Md5:          removeBackslashAndQuotes(message.ETag),
		}
		objectInfos = append(objectInfos, objectInfo)
	}

	httpx.OkJson(w, ResponseData{
		Code: CodeSuccess,
		Msg:  "success",
		Data: objectInfos,
	})
}

// 获取对象信息
func GetObject(w http.ResponseWriter, r *http.Request) {
	bucketname := r.PostFormValue("bucket_name")
	objectname := r.PostFormValue("object_name")
	object, err := client.GetObject(bucketname, objectname, minio.GetObjectOptions{})
	if err != nil {
		httpx.OkJson(w, ResponseData{
			Code: CodeInternalServerError,
			Msg:  err.Error(),
		})
		return
	}
	defer func(object *minio.Object) {
		err := object.Close()
		if err != nil {
			httpx.Error(w, err)
			return
		}
	}(object)

	localFile, err := os.Create("images/" + objectname)
	if err != nil {
		httpx.OkJson(w, ResponseData{
			Code: CodeInternalServerError,
			Msg:  err.Error(),
		})
		return
	}
	defer func(localFile *os.File) {
		err := localFile.Close()
		if err != nil {
			httpx.Error(w, err)
			return
		}
	}(localFile)
	if _, err = io.Copy(localFile, object); err != nil {
		httpx.OkJson(w, ResponseData{
			Code: CodeInternalServerError,
			Msg:  err.Error(),
		})
		return
	}

	httpx.OkJson(w, ResponseData{
		Code: CodeSuccess,
		Msg:  "success",
	})
}

func PutObject(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", r.Header.Get("Origin"))

	r.ParseMultipartForm(32 << 20) //32M
	mForm := r.MultipartForm
	bucketName := mForm.Value["bucketName"]
	// 设置自定义的响应头
	w.Header().Set("Content-Type", "application/json")
	res := ResponseData{}
	for k := range mForm.File {
		file, fileHeader, err := r.FormFile(k)
		if err != nil {
			httpx.OkJson(w, ResponseData{
				Code: CodeInternalServerError,
				Msg:  err.Error(),
			})
			return
		}

		defer file.Close()
		n, err := client.PutObject(bucketName[0], fileHeader.Filename, file, fileHeader.Size, minio.PutObjectOptions{})
		if err != nil {
			httpx.OkJson(w, ResponseData{
				Code: CodeInternalServerError,
				Msg:  err.Error(),
			})
			return
		}

		logx.Info("Successfully uploaded bytes: ", n)
	}
	res.Code = CodeSuccess
	res.Msg = "Successfully upload"
	res.Data = nil
	httpx.OkJson(w, res)
}

// 下载文件
func DownLoad(w http.ResponseWriter, r *http.Request) {
	// r.ParseMultipartForm(32 << 20) //32M
	bucketname := r.PostFormValue("bucket_name")
	objectname := r.PostFormValue("object_name")
	object, err := client.GetObject(bucketname, objectname, minio.GetObjectOptions{})
	log.Printf("%+v\n", object)
	if err != nil {
		httpx.Error(w, err)
		return
	}
	defer func(object *minio.Object) {
		err := object.Close()
		if err != nil {
			httpx.Error(w, err)
			return
		}
	}(object)
	chunk, err := io.ReadAll(object)
	if err != nil {
		httpx.Error(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", "attachement;filename=\""+objectname+"\"")
	w.Write(chunk)
}

// 分片上传
func Upload(w http.ResponseWriter, r *http.Request) {
	res := ResponseData{}
	if err := r.ParseMultipartForm(32 << 20); err != nil { //32M
		logx.Errorf("Cannot ParseMultipartForm, error: %v\n", err)
		res.Code = CodeInternalParamsError
		res.Msg = CodeInternalParamsError.Msg()
		httpx.OkJson(w, res)
		return
	}
	mForm := r.MultipartForm
	// 获取存储桶名
	bucketname := mForm.Value["BucketName"][0]
	// 获取文件md值
	identifier := mForm.Value["identifier"][0]
	// 分片规格
	chunkSize := mForm.Value["chunkSize"][0]
	// 当前分片索引
	chunkNumber := mForm.Value["chunkNumber"][0]
	// 文件名
	filename := mForm.Value["filename"][0]
	// 总分片
	totalChunks := mForm.Value["totalChunks"][0]
	// 文件总大小
	totalSize := mForm.Value["totalSize"][0]
	total_size, err := strconv.ParseInt(totalSize, 10, 64)
	if err != nil {
		res.Code = CodeInternalParamsError
		res.Msg = "Parse Invalid total size"
		httpx.OkJson(w, res)
		return
	}
	total_chunks, err := strconv.Atoi(totalChunks)
	if err != nil {
		logx.Error("total_chunks parse err:", err)
		res.Code = CodeInternalParamsError
		res.Msg = CodeInternalParamsError.Msg()
		httpx.OkJson(w, res)
		return
	}

	// 查询上传记录
	info, err := GetInfoForIdentifier(identifier)
	if err != nil {
		logx.Error("GetInfoForIdentifier:%s\n", err.Error())
	} else {
		res.Code = CodeSuccess
		res.Msg = "GetInfoForIdentifier:文件已在系统内:秒传成功！"
		res.Data = info
		httpx.OkJson(w, res)
		return
	}

	// 查询同名文件
	info, err = GetFileSaveInfo(bucketname, filename)
	if err != nil {
		logx.Error("GetFileSaveInfo:", err.Error())
	} else if identifier != info.Md5 {
		filename = identifier + "-" + filename
	} else if identifier == info.Md5 {
		res.Code = CodeSuccess
		res.Msg = "GetFileSaveInfo:文件已在系统内:秒传成功！"
		res.Data = info
		httpx.OkJson(w, res)
		return
	}

	// 检查桶状态
	isExist, _ := IsBuckets(bucketname)
	if !isExist {
		logx.Error("err:检查桶状态：不存在的存储桶")
		res.Code = CodeInternalParamsError
		res.Msg = CodeInternalParamsError.Msg()
		httpx.OkJson(w, res)
		return
	}

	// 已完成上传的大小
	var have_uploaded_size int64 = 0
	// 已完成上传的分片
	var have_uploaded_count int64 = 0
	doneCh := make(chan struct{})
	defer close(doneCh)

	// 初始化map
	if _, ok := is_exists_key[identifier+"_"+chunkSize]; !ok {
		is_exists_key[identifier+"_"+chunkSize] = make(map[string]bool)
	}

reUpload:
	_, err = GetInfoForIdentifier(identifier)
	if is_exist, ok := is_exists_key[identifier+"_"+chunkSize][chunkNumber+".part"]; err != nil && (!ok || !is_exist) {
		logx.Info("开始上传分片！")
		for k := range mForm.File {
			file, fileHeader, err := r.FormFile(k)
			if err != nil {
				res.Code = CodeInternalServerError
				res.Msg = CodeInternalServerError.Msg()
				httpx.OkJson(w, res)
				return
			}

			defer file.Close()
			n, err := client.PutObject(bucketname, identifier+"_"+chunkSize+"/"+chunkNumber+".part", file, fileHeader.Size, minio.PutObjectOptions{})
			if err != nil {
				httpx.Error(w, err)
				return
			}
			// 标记上传分片
			mu.Lock()
			if _, ok := is_exists_key[identifier+"_"+chunkSize]; !ok {
				is_exists_key[identifier+"_"+chunkSize] = make(map[string]bool)
				is_exists_key[identifier+"_"+chunkSize][chunkNumber+".part"] = true
			}
			mu.Unlock()

			logx.Info("Successfully uploaded bytes: ", n)
		}
	}
	shardPaths := make([]SrcInfo, 0)
	// 文件上传完成
	finished := false
	// 检查当前分片是否成功上传到临时文件
	isUploaded := false

	// 查询已上传的分片文件
	for message := range client.ListObjects(bucketname, identifier+"_"+chunkSize, true, doneCh) {
		arr := strings.Split(message.Key, "/")
		tmp_name := ""
		park_name := ""
		if len(arr) >= 1 {
			tmp_name = arr[1]
			key_arr := strings.Split(tmp_name, ".")
			if len(key_arr) >= 1 {
				park_name = key_arr[0]
			}
		}
		// 标记分片已上传状态
		if park_name == chunkNumber {
			isUploaded = true
		}

		if v, err := strconv.Atoi(park_name); err == nil && v > 0 && v <= total_chunks {
			shardPaths = append(shardPaths, SrcInfo{
				Name: message.Key,
				Etag: message.ETag,
			})
			have_uploaded_count += 1
			have_uploaded_size += message.Size
		}
		res.Code = CodeSuccess
		res.Msg = "继续上传"
		if _, ok := muxing[identifier]; !ok {
			muxing[identifier] = false
		}
		// 合并临时文件
		if have_uploaded_size == total_size && int(have_uploaded_count) == total_chunks && !muxing[identifier] {
			muxing[identifier] = true
			logx.Info("开始合并")
			err := ComposeObject(bucketname, filename, identifier, shardPaths)
			// 文件合并失败
			if err != nil {
				res.Msg = "merge file error: " + err.Error()
				res.Code = CodeInternalServerError
				muxing[identifier] = false
				httpx.OkJson(w, res)
				return
			} else {
				// 完成上传
				finished = true
			}
		}
	}

	// 重试计数
	retry := 0
	// 丢失临时文件
	if !isUploaded {
		logx.Error("临时文件丢失，正在重新上传！")
		delete(is_exists_key[identifier+"_"+chunkSize], chunkNumber+".part")
		retry++
		if retry > 4 {
			res.Code = CodeInternalServerError
			res.Msg = "上传失败"
			httpx.OkJson(w, res)
			return
		}
		goto reUpload
	}

	// 检查文件是否已经合并完成
	if finished {
		logx.Info("Finished")
		// 删除临时文件
		removeObjectList(shardPaths, bucketname)
		// 检查文件
		info, err := GetStatObject(bucketname, filename)
		if err != nil {
			logx.Error("查询上传记录:%s\n", err.Error())
			finished = false
			muxing[identifier] = false
			res.Code = CodeInternalServerError
			res.Msg = CodeInternalServerError.Msg()
			res.Data = nil
		} else {
			// 标记md5值，后续处理相同md5值的文件
			err := redisdb.Set(identifier, info, 0).Err()
			if err != nil {
				logx.Info("Set Error：", err.Error())
			}

			// 标记桶名、文件名,后续处理同名但是MD5值不同的文件
			err = redisdb.HSet(bucketname, filename, info).Err()
			if err != nil {
				logx.Info("HSet Error：", err.Error())
			}

			res.Code = CodeSuccess
			res.Msg = CodeSuccess.Msg()
			res.Data = info
			delete(is_exists_key, identifier+"_"+chunkSize)
			delete(muxing, identifier)
		}
	}

	httpx.OkJson(w, res)
}
