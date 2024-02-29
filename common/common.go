package common

import (
	"errors"
	"sort"
	"strconv"
	"strings"

	"github.com/minio/minio-go"
	"github.com/zituocn/logx"
)

// 自定义排序
func partSort(shardPaths []SrcInfo) func(int, int) bool {
	return func(i, j int) bool {
		tmp_i := strings.Split(shardPaths[i].Name, "/")[1]
		tmp_j := strings.Split(shardPaths[j].Name, "/")[1]
		num_i := strings.Split(tmp_i, ".")[0]
		num_j := strings.Split(tmp_j, ".")[0]
		a, _ := strconv.Atoi(num_i)
		b, _ := strconv.Atoi(num_j)
		return a < b
	}
}

// minio合并分片小于5M时，会报错
func ComposeObject(bucketname, dst_name, md5 string, shardPaths []SrcInfo) error {
	sort.SliceStable(shardPaths, partSort(shardPaths))

	src_list := make([]minio.SourceInfo, 0)
	for _, v := range shardPaths {
		item := minio.NewSourceInfo(bucketname, v.Name, nil)
		item.SetMatchETagCond(v.Etag)
		src_list = append(src_list, item)
	}
	dst, err := minio.NewDestinationInfo(bucketname, dst_name, nil, nil)
	if err != nil {
		logx.Error("NewDestinationInfo error:", err)
		return err
	}
	err = client.ComposeObject(dst, src_list)
	if err != nil {
		logx.Error("ComposeObject error:", err)
		return err
	}
	return nil
}

// 批量删除文件
func removeObjectList(paths []SrcInfo, bucketname string) error {
	fileCh := make(chan string, len(paths))
	defer close(fileCh)
	for _, file := range paths {
		fileCh <- file.Name
	}
	ch := client.RemoveObjects(bucketname, fileCh)
	select {
	case err := <-ch:
		if err.Err != nil {
			logx.Errorf("remove: bucketname%s %v", bucketname, err)
			return err.Err
		}
	default:
		logx.Info("removeObjectList success")
	}
	return nil
}

// 桶是否存在
func IsBuckets(name string) (bool, error) {
	isExist, err := client.BucketExists(name)

	if err != nil {
		logx.Errorf("Check %s err:%s", name, err.Error())
		return false, err
	}
	return isExist, nil
}

// 获取对象信息
func GetStatObject(bucketname, objectname string) (*FileSaveInfo, error) {
	info, err := client.StatObject(bucketname, objectname, minio.StatObjectOptions{})
	if err != nil {
		logx.Errorf("StatObject error: %v", err)
		return nil, err
	}
	return &FileSaveInfo{
		BucketName:   bucketname,
		ObjectName:   info.Key,
		LastModified: info.LastModified.Format("2006-01-02 15:04:05"),
		Size:         info.Size,
		Md5:          info.ETag,
	}, nil
}

func removeBackslashAndQuotes(str string) string {
	result := strings.ReplaceAll(str, "\\", "")   // 移除反斜线
	result = strings.ReplaceAll(result, "\"", "") // 移除双引号
	return result
}

// 根据md5值查询文件记录
func GetInfoForIdentifier(md5 string) (*FileSaveInfo, error) {
	n, err := redisdb.Exists(md5).Result()
	if err != nil || n == 0 {
		logx.Notice("redisdb.Exists md5 not found")
		return nil, errors.New("redisdb.Exists md5 not found")
	}
	info := &FileSaveInfo{}
	err = redisdb.Get(md5).Scan(info)
	if err != nil {
		logx.Notice("redisdb.Get md5 not found")
		return nil, err
	}

	return info, nil
}

// 根据文件名、桶名获取文件记录
func GetFileSaveInfo(bucketname, filename string) (*FileSaveInfo, error) {
	flag, err := redisdb.HExists(bucketname, filename).Result()
	if err != nil || !flag {
		logx.Notice("redis HExists bucketname:%s,filename:%s not found", bucketname, filename)
	}
	info := &FileSaveInfo{}
	err = redisdb.HGet(bucketname, filename).Scan(&is_exists_key)
	if err != nil {
		logx.Notice("redis HGet bucketname:%s,filename:%s not found", bucketname, filename)
	}

	info, err = GetStatObject(bucketname, filename)
	if err != nil {
		return nil, err
	}
	redisdb.HSet(bucketname, filename, info)
	return info, err
}
