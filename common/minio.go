package common

import (
	"errors"
	"fmt"
	"io"
	"log"
	"minio_demo/config"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/minio/minio-go"
	"github.com/zeromicro/go-zero/rest/httpx"
)

var (
	client        *minio.Client
	is_exists_key map[string]map[string]bool
	mu            sync.RWMutex
	muxing        bool
)

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
	muxing = false
	is_exists_key = make(map[string]map[string]bool, 0)
	client = InitMinioClient()
}

func InitMinioClient() *minio.Client {
	// 基本的配置信息
	endpoint := config.ConfData.Minio.Address + ":" + strconv.Itoa(config.ConfData.Minio.Port)
	accessKeyID := config.ConfData.Minio.AccessKeyID
	secretAccessKey := config.ConfData.Minio.SecretAccessKey
	minioClient, err := minio.New(endpoint, accessKeyID, secretAccessKey, false)
	if err != nil {
		log.Fatalf("初始化MinioClient错误：%s", err.Error())
	} else {
		log.Printf("Minio client start")
	}
	return minioClient
}

// 创建桶
func CreateBucket(w http.ResponseWriter, r *http.Request) {
	bucketname := r.PostFormValue("bucket_name")

	err := client.MakeBucket(bucketname, "")
	if err != nil {
		httpx.Error(w, err)
		return
	}
	httpx.OkJson(w, ResponseData{
		Code: CodeSuccess,
		Msg:  "创建桶成功",
	})
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
		httpx.Error(w, err)
	}

	httpx.OkJson(w, ResponseData{
		Code: CodeSuccess,
		Msg:  "success",
		Data: bucket_list,
	})
}

// 桶是否存在
func IsBuckets(name string) (bool, error) {
	isExist, err := client.BucketExists(name)

	if err != nil {
		log.Printf("Check %s err:%s", name, err.Error())
		return isExist, err
	}
	return isExist, nil
}

// 移除桶
func RemoveBucket(w http.ResponseWriter, r *http.Request) {
	bucketname := r.PostFormValue("bucket_name")
	isExist, err := IsBuckets(bucketname)

	if err != nil {
		httpx.Error(w, err)
		return
	}

	if !isExist {
		httpx.Error(w, errors.New("bucket not found"))
		return
	}

	err = client.RemoveBucket(bucketname)
	if err != nil {
		httpx.Error(w, err)
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
		log.Println("IsBuckets error:", err)
		httpx.OkJson(w, ResponseData{
			Code: CodeInternalParamsError,
			Msg:  err.Error(),
			Data: nil,
		})
	}

	if !isExist {
		log.Println("bucket not found")
		httpx.OkJson(w, ResponseData{
			Code: CodeInternalParamsError,
			Msg:  "bucket not found",
			Data: nil,
		})
		return
	}

	doneCh := make(chan struct{})
	defer close(doneCh)

	objectInfos := make([]ObjectInfo, 0)

	for message := range client.ListObjects(bucketname, objectname, true, doneCh) {
		objectInfo := ObjectInfo{
			Key:          message.Key,
			LastModified: message.LastModified.Format("2006-01-02 15:04:05"),
			Size:         message.Size,
			ContentType:  message.ContentType,
			Metadata:     message.Metadata,
			Owner:        message.Owner,
			StorageClass: message.StorageClass,
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

	localFile, err := os.Create("images/" + objectname)
	if err != nil {
		httpx.Error(w, err)
		return
	}
	defer func(localFile *os.File) {
		err := localFile.Close()
		if err != nil {
			return
		}
	}(localFile)
	if _, err = io.Copy(localFile, object); err != nil {
		httpx.Error(w, err)
		return
	}

	httpx.OkJson(w, ResponseData{
		Code: CodeSuccess,
		Msg:  "success",
	})
}

func PutObject1(w http.ResponseWriter, r *http.Request) {
	reader, err := r.MultipartReader()

	if err != nil {
		log.Println("MultipartReader:", err)
		return
	}
	bucketName := strings.Builder{}

	for {
		part, err := reader.NextPart()
		if err == io.EOF {
			break
		}

		if err != nil {
			log.Printf("error:%s\n", err.Error())
			return
		}

		if part.FileName() == "" { // 普通表单字段

			_, _ = io.Copy(&bucketName, part)
			log.Printf("dist:%+v\n", bucketName)
		} else { // 文件字段
			n, err := client.PutObject(bucketName.String(), part.FileName(), part, -1, minio.PutObjectOptions{})
			if err != nil {
				log.Println("PutObject err=>", err)
				return
			}
			log.Println("Successfully uploaded bytes: ", n)
		}
	}
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
			httpx.Error(w, err)
			return
		}

		defer file.Close()
		n, err := client.PutObject(bucketName[0], fileHeader.Filename, file, fileHeader.Size, minio.PutObjectOptions{})
		if err != nil {
			httpx.Error(w, err)
			return
		}

		log.Println("Successfully uploaded bytes: ", n)
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
		log.Printf("Cannot ParseMultipartForm, error: %v\n", err)
		res.Code = CodeInternalParamsError
		res.Msg = CodeInternalParamsError.Msg()
		httpx.OkJson(w, res)
		return
	}

	if r.MultipartForm == nil {
		log.Printf("MultipartForm is null\n")
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
		res.Msg = CodeInternalParamsError.Msg()
		httpx.OkJson(w, res)
		return
	}

	// 查询上传记录
	n, err := redisdb.Exists(identifier).Result()
	if err != nil {
		log.Printf("err=%s\n", err.Error())
	}

	// 已查询到文件存储记录
	if n == 1 {
		result := &FileSaveInfo{}
		// 查询redis是否有文件存放记录
		err := redisdb.Get(identifier).Scan(result)
		if err != nil {
			res.Msg = err.Error()
		} else {
			objCh := make(chan struct{})
			defer close(objCh)
			// 查询系统内是否存在文件
			ch := client.ListObjects(result.BucketName, result.ObjectName, true, objCh)
			if val, ok := <-ch; ok {
				res.Code = CodeSuccess
				res.Msg = CodeSuccess.Msg()
				res.Data = struct {
					BucketName string
					ObjectName string
				}{
					BucketName: result.BucketName,
					ObjectName: result.ObjectName,
				}
				httpx.OkJson(w, res)
				return
			} else {
				// 文件不存在，删除记录
				log.Println("failed===", val)
				// 删除map
				delete(is_exists_key, identifier+"_"+chunkSize)
				redisdb.Del(identifier)
			}
		}
	}

	// 检查桶
	isExist, err := IsBuckets(bucketname)

	if err != nil {
		log.Println("failed===", err)
		res.Code = CodeInternalParamsError
		res.Msg = CodeInternalParamsError.Msg()
		httpx.OkJson(w, res)
		return
	}

	if !isExist {
		log.Println("err", errors.New("不存在的存储桶"))
		res.Code = CodeInternalParamsError
		res.Msg = CodeInternalParamsError.Msg()
		httpx.OkJson(w, res)
		return
	}

	// 已完成上传的大小
	var have_uploaded_size int64 = 0

	// 已完成上传的分片
	var have_uploaded_count int64 = 0

	total_chunks, err := strconv.Atoi(totalChunks)
	if err != nil {
		log.Println("err===", err)
		res.Code = CodeInternalParamsError
		res.Msg = CodeInternalParamsError.Msg()
		httpx.OkJson(w, res)
		return
	}

	doneCh := make(chan struct{})
	defer close(doneCh)

	// 初始化map
	if _, ok := is_exists_key[identifier+"_"+chunkSize]; !ok {
		is_exists_key[identifier+"_"+chunkSize] = make(map[string]bool)
	}
reUpload:
	if is_exist, ok := is_exists_key[identifier+"_"+chunkSize][chunkNumber+".part"]; !ok || !is_exist {
		log.Println("开始上传！")
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
			is_exists_key[identifier+"_"+chunkSize][chunkNumber+".part"] = true
			mu.Unlock()

			log.Println("Successfully uploaded bytes: ", n)
		}
	}

	shardPaths := make([]string, 0)
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

		if park_name == chunkNumber {
			isUploaded = true
		}

		if v, err := strconv.Atoi(park_name); err == nil && v > 0 && v <= total_chunks {
			shardPaths = append(shardPaths, message.Key)
			have_uploaded_count += 1
			have_uploaded_size += message.Size
		}
		res.Msg = "继续上传"

		// fmt.Println("have_uploaded_size==", have_uploaded_size)
		// fmt.Println("total_size==", total_size)
		// fmt.Println("have_uploaded_count==", have_uploaded_count)
		// fmt.Println("total_chunks==", total_chunks)
		// fmt.Println("muxing==", muxing)
		// 合并临时文件
		if have_uploaded_size == total_size && int(have_uploaded_count) == total_chunks && !muxing {
			muxing = true
			res.Msg = "开始合并"
			err := mergeShards(client, bucketname, filename, shardPaths)
			if err != nil {
				res.Msg = "merge file error: " + err.Error()
				res.Code = CodeInternalServerError
				httpx.OkJson(w, res)
				return
			} else {
				// 保存
				data := &FileSaveInfo{
					BucketName: bucketname,
					ObjectName: filename,
				}
				err := redisdb.Set(identifier, data, 0).Err()
				if err != nil {
					log.Println("Error：", err.Error())
				}
				res.Msg = "合并成功"
				res.Code = CodeSuccess
				res.Data = struct {
					BucketName string
					ObjectName string
				}{
					BucketName: bucketname,
					ObjectName: filename,
				}
				finished = true
				muxing = false
			}
		}
	}

	// 重试计数
	retry := 0
	// 丢失临时文件
	if !isUploaded {
		log.Println("临时文件丢失，正在重新上传！")
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

	if finished {
		log.Println("Finished")
		// 删除临时文件
		removeObjectList(shardPaths, bucketname)
		// 删除map
		delete(is_exists_key, identifier+"_"+chunkSize)
	}

	httpx.OkJson(w, res)
}

// 批量删除文件
func removeObjectList(paths []string, bucketname string) error {
	fileCh := make(chan string, len(paths))
	defer close(fileCh)
	for _, file := range paths {
		fileCh <- file
	}
	ch := client.RemoveObjects(bucketname, fileCh)
	select {
	case err := <-ch:
		if err.Err != nil {
			return err.Err
		}
	default:
		fmt.Println("success")
	}
	return nil
}

// 合并文件
func mergeShards(client *minio.Client, bucketName string, objectName string, shardPaths []string) error {
	sort.SliceStable(shardPaths, partSort(shardPaths))
	// 创建目标文件
	file, err := os.Create("merged_object") // 这里的 "merged_object" 为最终合并后的文件名
	if err != nil {
		return fmt.Errorf("无法创建目标文件: %w", err)
	}
	defer file.Close()
	for _, path := range shardPaths {
		// 打开每个分片文件
		partFile, err := client.GetObject(bucketName, path, minio.GetObjectOptions{})
		if err != nil {
			return fmt.Errorf("无法获取分片文件 '%s': %w", path, err)
		}
		defer partFile.Close()

		_, err = io.Copy(file, partFile)
		if err != nil {
			return fmt.Errorf("无法将分片内容复制到目标文件: %w", err)
		}
	}
	_, err = client.FPutObject(bucketName, objectName, "merged_object", minio.PutObjectOptions{ContentType: "application/octet-stream"})
	if err != nil {
		return fmt.Errorf("无法上传合并后的文件到 MinIO: %w", err)
	}
	return nil
}

// 自定义排序
func partSort(shardPaths []string) func(int, int) bool {
	return func(i, j int) bool {
		tmp_i := strings.Split(shardPaths[i], "/")[1]
		tmp_j := strings.Split(shardPaths[j], "/")[1]
		num_i := strings.Split(tmp_i, ".")[0]
		num_j := strings.Split(tmp_j, ".")[0]
		a, _ := strconv.Atoi(num_i)
		b, _ := strconv.Atoi(num_j)
		return a < b
	}
}
