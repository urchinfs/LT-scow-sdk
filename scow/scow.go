package scow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/donnie4w/go-logger/logger"
	"github.com/go-resty/resty/v2"
	"github.com/urchinfs/LT-scow-sdk/types"
	urchinutil "github.com/urchinfs/urchin_util/redis"
	"io"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"
)

const (
	DefaultClusterId = "dev-k8s"
	DefaultRootPath  = "/data/home/xiaoyan"
)

var ClusterId, RootPath string

type Client interface {
	StorageVolExists(ctx context.Context, storageVolName string) (bool, error)

	MakeStorageVol(ctx context.Context, storageVolName string) (err error)

	ListStorageVols(ctx context.Context) ([]StorageVolInfo, error)

	StatFile(ctx context.Context, storageVolName, fileName string) (FileInfo, error)

	UploadFile(ctx context.Context, storageVolName, fileName, digest string, reader io.Reader) error

	DownloadFile(ctx context.Context, storageVolName, fileName string) (io.ReadCloser, error)

	RemoveFile(ctx context.Context, storageVolName, fileName string) error

	RemoveFiles(ctx context.Context, storageVolName string, objects []*FileInfo) error

	RemoveFolder(ctx context.Context, storageVolName string, folderName string) error

	ListFiles(ctx context.Context, storageVolName, prefix, marker string, limit int64) ([]*FileInfo, error)

	ListDirFiles(ctx context.Context, storageVolName, prefix string) ([]*FileInfo, error)

	IsFileExist(ctx context.Context, storageVolName, fileName string) (bool, error)

	IsStorageVolExist(ctx context.Context, storageVolName string) (bool, error)

	GetDownloadLink(ctx context.Context, storageVolName, fileName string, expire time.Duration) (string, error)

	CreateDir(ctx context.Context, storageVolName, folderName string) error

	StatFolder(ctx context.Context, storageVolName, folderName string) (*FileInfo, bool, error)

	PostTransfer(ctx context.Context, storageVolName, fileName string, isSuccess bool) error
}

type client struct {
	httpClient     *resty.Client
	redisStorage   *urchinutil.RedisStorage
	token          string
	username       string
	password       string
	tokenUrl       string
	endpoint       string
	redisEndpoints []string
	redisPassword  string
	enableCluster  bool
}

func New(username, password, tokenUrl, endpoint string, redisEndpoints []string, redisPassword string, enableCluster bool) (Client, error) {
	c := &client{
		username:       username,
		password:       password,
		tokenUrl:       tokenUrl,
		endpoint:       endpoint,
		redisEndpoints: redisEndpoints,
		redisPassword:  redisPassword,
		enableCluster:  enableCluster,
		httpClient:     resty.New(),
		redisStorage:   urchinutil.NewRedisStorage(redisEndpoints, redisPassword, enableCluster),
	}

	c.tokenUrl = "http://" + c.tokenUrl
	c.endpoint = "http://" + c.endpoint
	if c.username == "" || c.password == "" || c.tokenUrl == "" || c.endpoint == "" {
		return nil, types.ErrorInvalidParameter
	}
	if c.redisStorage == nil {
		return nil, errors.New("init redis error")
	}

	return c, nil
}

type StorageVolInfo struct {
	Name       string    `json:"name"`
	ModifyDate time.Time `json:"modifyDate"`
}

type FileInfo struct {
	Key          string
	Size         int64
	ETag         string
	ContentType  string
	LastModified time.Time
	Expires      time.Time
	Metadata     http.Header
}

type GetTokenReply struct {
	Code  int32  `json:"respCode"`
	Error string `json:"respError"`
	Msg   string `json:"respMessage"`
	Data  struct {
		ID    int32  `json:"id"`
		Token string `json:"core-sctoken"`
	} `json:"respBody"`
}

type FileListReply struct {
	Data []struct {
		FileInfoReply
	} `json:"data"`
	Total int64 `json:"total"`
}

type FileInfoReply struct {
	Type  string `json:"type"`
	Name  string `json:"name"`
	Mtime string `json:"mtime"`
	Size  int64  `json:"size"`
	Mode  int64  `json:"mode"`
}

type ErrorReply struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type Reply struct {
	RespCode    json.RawMessage `json:"respCode"`
	RespError   json.RawMessage `json:"respError"`
	RespMessage json.RawMessage `json:"respMessage"`
	RespBody    json.RawMessage `json:"respBody"`
}

func parseBody(ctx context.Context, reply *Reply, body interface{}) error {
	if reply.RespError != nil && string(reply.RespError) != "" {
		return errors.New(string(reply.RespError))
	}

	if body != nil {
		err := json.Unmarshal(reply.RespBody, body)
		if err != nil {
			logger.Error(ctx, "parseBody json Unmarshal failed, error:%v", err)
			return types.ErrorJsonUnmarshalFailed
		}
	}

	return nil
}

func (c *client) completePath(storageVolName string, elem ...string) string {
	if RootPath == "" {
		rootPathKey := c.redisStorage.MakeStorageKey([]string{}, types.StoragePrefixScowRootPath)
		value, err := c.redisStorage.Get(rootPathKey)
		if err != nil || string(value) == "" {
			rootPath, err := c.getHomePath(context.Background())
			if err != nil {
				RootPath = DefaultRootPath
			} else {
				RootPath = rootPath
			}
		} else {
			RootPath = string(value)
		}
	}

	elems := append([]string{RootPath, storageVolName}, elem...)
	return path.Join(elems...)
}

func (c *client) clusterId() string {
	if ClusterId == "" {
		clusterIdKey := c.redisStorage.MakeStorageKey([]string{}, types.StoragePrefixScowClusterId)
		value, err := c.redisStorage.Get(clusterIdKey)
		if err != nil || string(value) == "" {
			id, err := c.getClusterId(context.Background())
			if err != nil {
				ClusterId = DefaultClusterId
			} else {
				ClusterId = id
			}
		} else {
			ClusterId = string(value)
		}
	}

	return ClusterId
}

func (c *client) needRetry(r *ErrorReply) bool {
	if strings.Contains(r.Code, "UNAUTHORIZED") || strings.Contains(r.Message, "UNAUTHORIZED") {
		tokenKey := c.redisStorage.MakeStorageKey([]string{}, types.StoragePrefixScowToken)
		_ = c.redisStorage.Del(tokenKey)
		_ = c.refreshToken(context.Background())

		return true
	}

	return false
}

func (c *client) sendHttpRequest(ctx context.Context, httpMethod, httpPath string, jsonBody string, respData interface{}) error {
	if !strings.HasPrefix(httpPath, "/") {
		httpPath = "/" + httpPath
	}
	httpUrl := c.endpoint + httpPath

	for {
		r := &Reply{}
		response := &resty.Response{}
		var err error
		if httpMethod == types.HttpMethodGet {
			response, err = c.httpClient.R().
				SetHeader(types.AuthHeader, c.token).
				SetResult(r).
				Get(httpUrl)
			if err != nil {
				return err
			}
		} else if httpMethod == types.HttpMethodPost {
			response, err = c.httpClient.R().
				SetHeader("Content-Type", "application/json").
				SetHeader(types.AuthHeader, c.token).
				SetBody(jsonBody).SetResult(r).
				Post(httpUrl)
			if err != nil {
				return err
			}
		} else if httpMethod == types.HttpMethodDelete {
			response, err = c.httpClient.R().
				SetHeader(types.AuthHeader, c.token).
				SetResult(r).
				Delete(httpUrl)
			if err != nil {
				return err
			}
		} else {
			return types.ErrorInternal
		}

		if !response.IsSuccess() {
			if response.StatusCode() == http.StatusUnauthorized {
				err := c.delToken(ctx)
				if err != nil {
					return err
				}
				err = c.refreshToken(ctx)
				if err != nil {
					return err
				}

				time.Sleep(time.Second * 2)
				continue
			}

			r := &ErrorReply{}
			err := json.Unmarshal(response.Body(), r)
			if err == nil {
				if c.needRetry(r) {
					time.Sleep(time.Second * 2)
					continue
				}
			}

			return errors.New("Code:" + strconv.FormatInt(int64(response.StatusCode()), 10) + ", Msg:" + string(response.Body()))
		}

		var respCode int
		err = json.Unmarshal(r.RespCode, &respCode)
		if err != nil {
			return types.ErrorJsonUnmarshalFailed
		}

		if respCode != http.StatusOK || string(r.RespError) != "" {
			return fmt.Errorf("resp Code:%v, Err:%s", respCode, string(r.RespError))
		}

		err = parseBody(ctx, r, respData)
		if err != nil {
			return err
		}

		break
	}

	return nil
}

func (c *client) getToken(ctx context.Context) (string, error) {
	type GetAuthRequest struct {
		UserName string `json:"username"`
		Password string `json:"password"`
	}

	req := GetAuthRequest{
		UserName: c.username,
		Password: c.password,
	}

	jsonBody, err := json.Marshal(req)
	if err != nil {
		return "", types.ErrorJsonMarshalFailed
	}

	urlPath := fmt.Sprintf("/v1/sys/user/login")
	resp := &GetTokenReply{}
	response, err := c.httpClient.R().
		SetHeader("Content-Type", "application/json").
		SetBody(jsonBody).SetResult(resp).Post(c.tokenUrl + urlPath)
	if err != nil {
		return "", err
	}

	if !response.IsSuccess() {
		return "", fmt.Errorf("http status Code:%v, Body:%s", response.StatusCode(), response.Body())
	}

	if resp.Code != http.StatusOK {
		return "", fmt.Errorf("authentication Failed, Code:%v, Msg%s", resp.Code, resp.Msg)
	}
	authToken := resp.Data.Token

	return authToken, nil
}

func (c *client) refreshToken(ctx context.Context) error {
	tokenKey := c.redisStorage.MakeStorageKey([]string{}, types.StoragePrefixScowToken)
	value, err := c.redisStorage.Get(tokenKey)
	if err != nil || len(value) <= 0 {
		if errors.Is(err, types.ErrorNotExists) || len(value) <= 0 {
			token, err := c.getToken(ctx)
			if err != nil {
				return err
			}

			c.token = token
			err = c.redisStorage.SetWithTimeout(tokenKey, []byte(token), types.DefaultTokenExpireTime)
			if err != nil {
				return err
			}

			return nil
		}

		return err
	}

	c.token = string(value)
	return nil
}

func (c *client) delToken(ctx context.Context) error {
	tokenKey := c.redisStorage.MakeStorageKey([]string{}, types.StoragePrefixScowToken)
	return c.redisStorage.Del(tokenKey)
}

func (c *client) StorageVolExists(ctx context.Context, storageVolName string) (bool, error) {
	vols, err := c.ListStorageVols(ctx)
	if err != nil {
		return false, err
	}

	for _, vol := range vols {
		if vol.Name == storageVolName {
			return true, nil
		}
	}

	return false, nil
}

func (c *client) MakeStorageVol(ctx context.Context, storageVolName string) (err error) {
	if err := c.refreshToken(ctx); err != nil {
		return err
	}

	type MakeStorageVolReq struct {
		ClusterId string `json:"clusterId"`
		Path      string `json:"path"`
	}

	req := &MakeStorageVolReq{
		ClusterId: c.clusterId(),
		Path:      c.completePath(storageVolName),
	}

	jsonBody, err := json.Marshal(req)
	if err != nil {
		return types.ErrorJsonMarshalFailed
	}

	reqPath := fmt.Sprintf("/v1/ai/api/file/mkdir")
	err = c.sendHttpRequest(ctx, types.HttpMethodPost, reqPath, string(jsonBody), nil)
	if err != nil {
		return err
	}

	return nil
}

func (c *client) ListStorageVols(ctx context.Context) ([]StorageVolInfo, error) {
	err := c.refreshToken(ctx)
	if err != nil {
		return []StorageVolInfo{}, err
	}

	pageIndex := 0
	const (
		pageSize = 100000
	)

	var storageVolsInfo []StorageVolInfo
	for {
		reqPath := fmt.Sprintf("/v1/ai/api/file/listDirectory?clusterId=%s&path=%s", c.clusterId(), c.completePath(""))
		resp := &FileListReply{}
		err := c.sendHttpRequest(ctx, types.HttpMethodGet, reqPath, "", resp)
		if err != nil {
			return storageVolsInfo, err
		}

		if resp.Total != int64(len(resp.Data)) {
			return storageVolsInfo, fmt.Errorf("list item not the same, resp Total:%v, Data:%v", resp.Total, len(resp.Data))
		}

		for _, storageVol := range resp.Data {
			timeObj, err := time.ParseInLocation(time.RFC3339Nano, storageVol.Mtime, time.Local)
			if err != nil {
				timeObj = time.Time{}
			}

			if storageVol.Type != types.StorageListTypeDir {
				continue
			}

			storageVolsInfo = append(storageVolsInfo, StorageVolInfo{
				Name:       storageVol.Name,
				ModifyDate: timeObj,
			})
		}

		if resp.Total < pageSize {
			break
		}

		pageIndex += pageSize

		//-todo not support page
		break
	}

	return storageVolsInfo, nil
}

func (c *client) statType(ctx context.Context, storageVolName, statKey string, isFolder bool) (FileInfo, error) {
	err := c.refreshToken(ctx)
	if err != nil {
		return FileInfo{}, err
	}

	type existResp struct {
		Data struct {
			Exists bool `json:"exists"`
		} `json:"data"`
	}
	reqPath := fmt.Sprintf("/v1/ai/api/file/checkExist?clusterId=%s&path=%s", c.clusterId(), c.completePath(storageVolName, statKey))
	fileExist := &existResp{}
	err = c.sendHttpRequest(ctx, types.HttpMethodGet, reqPath, "", fileExist)
	if err != nil {
		return FileInfo{}, err
	}
	if !fileExist.Data.Exists {
		return FileInfo{}, errors.New("noSuchKey")
	}

	type statResp struct {
		Data struct {
			Type string `json:"type"`
			Size int64  `json:"size"`
		} `json:"data"`
	}

	reqPath = fmt.Sprintf("/v1/ai/api/file/fileType?clusterId=%s&path=%s", c.clusterId(), c.completePath(storageVolName, statKey))
	fileStat := &statResp{}
	err = c.sendHttpRequest(ctx, types.HttpMethodGet, reqPath, "", fileStat)
	if err != nil {
		return FileInfo{}, err
	}

	target := types.StorageListTypeFile
	if isFolder {
		target = types.StorageListTypeDir
	}
	if strings.ToUpper(fileStat.Data.Type) != target {
		return FileInfo{}, errors.New("noSuchKey")
	}

	return FileInfo{
		Key:  path.Base(statKey),
		Size: fileStat.Data.Size,
	}, nil
}

func (c *client) StatFile(ctx context.Context, storageVolName, fileName string) (FileInfo, error) {
	return c.statType(ctx, storageVolName, fileName, false)
}

func (c *client) UploadFile(ctx context.Context, storageVolName, fileName, digest string, reader io.Reader) error {
	err := c.refreshToken(ctx)
	if err != nil {
		return err
	}

	reqPath := fmt.Sprintf("/v1/ai/api/files/upload?clusterId=%s&path=%s", c.clusterId(), c.completePath(storageVolName, fileName))
	r := &ErrorReply{}
	response, err := c.httpClient.R().
		SetHeader("Content-Type", "application/octet-stream").
		SetHeader(types.AuthHeader, c.token).
		SetFileReader("file", fileName, reader).
		//SetBody(reader).
		SetResult(r).
		Post(c.endpoint + reqPath)

	if err != nil {
		return err
	}
	if !response.IsSuccess() {
		r := &ErrorReply{}
		err = json.Unmarshal(response.Body(), r)
		if err != nil {
			logger.Errorf("UploadFile json.Unmarshal Error: %v, Body:%v", err, response.StatusCode())
			return types.ErrorJsonUnmarshalFailed
		}

		return errors.New("Http Code:" + response.Status() + ", Code:" + r.Code + ", Msg:" + r.Message)
	}

	if r.Message != "success" {
		return errors.New("Code:" + r.Code + ", Msg:" + r.Message)
	}

	return nil
}

func (c *client) DownloadFile(ctx context.Context, storageVolName, filePath string) (io.ReadCloser, error) {
	err := c.refreshToken(ctx)
	if err != nil {
		return nil, err
	}

	reqPath := fmt.Sprintf("/v1/ai/api/file/download?clusterId=%s&path=%s&download=true", c.clusterId(), c.completePath(storageVolName, filePath))
	r := &Reply{}
	response, err := c.httpClient.R().
		SetHeader(types.AuthHeader, c.token).
		SetDoNotParseResponse(true).
		SetResult(r).
		Get(c.endpoint + reqPath)
	if err != nil {
		return nil, err
	}

	if !response.IsSuccess() {
		r := &ErrorReply{}
		err = json.Unmarshal(response.Body(), r)
		if err != nil {
			return nil, errors.New("internal error")
		}

		return nil, errors.New("Code:" + r.Code + ", Msg:" + r.Message)
	}

	return response.RawBody(), nil
}

func (c *client) deleteOp(ctx context.Context, storageVolName, deleteKey string, isFolder bool) error {
	if err := c.refreshToken(ctx); err != nil {
		return err
	}

	type delReq struct {
		Path      string `json:"path"`
		ClusterId string `json:"clusterId"`
		Target    string `json:"target"`
	}

	target := types.StorageListTypeFile
	if isFolder {
		target = types.StorageListTypeDir
	}

	req := delReq{
		Path:      c.completePath(storageVolName, deleteKey),
		ClusterId: c.clusterId(),
		Target:    target,
	}
	jsonData, err := json.Marshal(&req)
	if err != nil {
		logger.Error(ctx, "parseBody json Marshal failed, error:%v", err)
		return types.ErrorJsonMarshalFailed
	}

	reqPath := fmt.Sprintf("/v1/ai/api/file/delete")
	r := &Reply{}
	err = c.sendHttpRequest(ctx, types.HttpMethodPost, reqPath, string(jsonData), r)
	if err != nil {
		return err
	}

	return nil
}

func (c *client) RemoveFile(ctx context.Context, storageVolName, fileName string) error {
	return c.deleteOp(ctx, storageVolName, fileName, false)
}

func (c *client) RemoveFiles(ctx context.Context, storageVolName string, objects []*FileInfo) error {
	for _, obj := range objects {
		err := c.RemoveFile(ctx, storageVolName, obj.Key)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *client) RemoveFolder(ctx context.Context, storageVolName string, folderName string) error {
	return c.deleteOp(ctx, storageVolName, folderName, true)
}

func (c *client) ListFiles(ctx context.Context, storageVolName, prefix, marker string, limit int64) ([]*FileInfo, error) {
	if prefix == "." || prefix == ".." {
		return nil, nil
	}

	if err := c.refreshToken(ctx); err != nil {
		return nil, nil
	}

	pageIndex := 0
	const (
		pageSize = 100000
	)

	var objects []*FileInfo
	for {
		resp := &FileListReply{}
		reqPath := fmt.Sprintf("/v1/ai/api/file/listDirectory/ClusterId=%s&path=%s", c.clusterId(), c.completePath(storageVolName, prefix))
		err := c.sendHttpRequest(ctx, types.HttpMethodGet, reqPath, "", resp)
		if err != nil {
			return nil, err
		}

		if resp.Total != int64(len(resp.Data)) {
			return nil, fmt.Errorf("list item not the same, resp Total:%v, Data:%v", resp.Total, len(resp.Data))
		}

		for _, item := range resp.Data {
			timeObj, err := time.ParseInLocation(time.RFC3339Nano, item.Mtime, time.Local)
			if err != nil {
				timeObj = time.Time{}
			}

			objects = append(objects, &FileInfo{
				Key:          item.Name,
				Size:         item.Size,
				LastModified: timeObj,
			})
		}

		if resp.Total < pageSize {
			break
		}

		pageIndex += pageSize

		//-todo not support page
		break
	}

	return objects, nil
}

func (c *client) listDirObjs(ctx context.Context, storageVolName, dirPath string) ([]*FileInfo, error) {
	if dirPath == "." || dirPath == ".." {
		return nil, nil
	}

	pageIndex := 0
	const (
		pageSize = 100000
	)

	var objects []*FileInfo
	for {
		resp := &FileListReply{}
		reqPath := fmt.Sprintf("/v1/ai/api/file/listDirectory?clusterId=%s&path=%s", c.clusterId(), c.completePath(storageVolName, dirPath))
		err := c.sendHttpRequest(ctx, types.HttpMethodGet, reqPath, "", resp)
		if err != nil {
			return nil, err
		}

		if resp.Total != int64(len(resp.Data)) {
			return nil, fmt.Errorf("list item not the same, resp Total:%v, Data:%v", resp.Total, len(resp.Data))
		}

		for _, item := range resp.Data {
			timeObj, err := time.ParseInLocation(time.RFC3339Nano, item.Mtime, time.Local)
			if err != nil {
				timeObj = time.Time{}
			}

			objects = append(objects, &FileInfo{
				Key:          path.Join(dirPath, item.Name),
				Size:         item.Size,
				LastModified: timeObj,
			})

			if strings.ToUpper(item.Type) == types.StorageListTypeDir {
				tmpObjs, err := c.listDirObjs(ctx, storageVolName, path.Join(dirPath, item.Name))
				if err != nil {
					return nil, err
				}

				objects = append(objects, tmpObjs...)
			}
		}

		if resp.Total < pageSize {
			break
		}

		pageIndex += pageSize

		//-todo not support page
		break
	}

	return objects, nil
}

func (c *client) ListDirFiles(ctx context.Context, storageVolName, prefix string) ([]*FileInfo, error) {
	if err := c.refreshToken(ctx); err != nil {
		return nil, err
	}

	resp, err := c.listDirObjs(ctx, storageVolName, prefix)
	if err != nil {
		return nil, err
	}

	if prefix != "" {
		resp = append(resp, &FileInfo{
			Key: prefix,
		})
	}

	//if !strings.HasSuffix(prefix, "/") {
	//	prefix += "/"
	//}
	//resp = append(resp, &FileInfo{
	//	Key: prefix,
	//})

	return resp, nil
}

func (c *client) IsFileExist(ctx context.Context, storageVolName, fileName string) (bool, error) {
	err := c.refreshToken(ctx)
	if err != nil {
		return false, err
	}

	type existResp struct {
		Data struct {
			Exists bool `json:"exists"`
		} `json:"data"`
	}
	reqPath := fmt.Sprintf("/v1/ai/api/file/checkExist?clusterId=%s&path=%s", c.clusterId(), c.completePath(storageVolName, fileName))
	fileExist := &existResp{}
	err = c.sendHttpRequest(ctx, types.HttpMethodGet, reqPath, "", fileExist)
	if err != nil {
		return false, err
	}

	return fileExist.Data.Exists, nil
}

func (c *client) IsStorageVolExist(ctx context.Context, storageVolName string) (bool, error) {
	return c.StorageVolExists(ctx, storageVolName)
}

func (c *client) GetDownloadLink(ctx context.Context, storageVolName, fileName string, expire time.Duration) (string, error) {
	if err := c.refreshToken(ctx); err != nil {
		return "", err
	}

	signedUrl := fmt.Sprintf("%s/v1/ai/api/file/downloadWithToken?clusterId=%s&path=%s&download=true&token=%s",
		c.endpoint, c.clusterId(), c.completePath(storageVolName, fileName), c.token)
	return signedUrl, nil
}

func (c *client) CreateDir(ctx context.Context, storageVolName, folderName string) error {
	if err := c.refreshToken(ctx); err != nil {
		return err
	}

	type createReq struct {
		ClusterId string `json:"clusterId"`
		Path      string `json:"path"`
	}

	req := createReq{
		Path:      c.completePath(storageVolName, folderName),
		ClusterId: c.clusterId(),
	}
	jsonData, err := json.Marshal(&req)
	if err != nil {
		logger.Error(ctx, "parseBody json Marshal failed, error:%v", err)
		return types.ErrorJsonMarshalFailed
	}

	reqPath := fmt.Sprintf("/v1/ai/api/file/mkdir")
	r := &Reply{}
	err = c.sendHttpRequest(ctx, types.HttpMethodPost, reqPath, string(jsonData), r)
	if err != nil {
		return err
	}

	return nil
}

func (c *client) StatFolder(ctx context.Context, storageVolName, folderName string) (*FileInfo, bool, error) {
	statInfo, err := c.statType(ctx, storageVolName, folderName, true)
	if err != nil {
		return nil, false, err
	}

	return &statInfo, true, nil
}

func (c *client) PostTransfer(ctx context.Context, storageVolName, fileName string, isSuccess bool) error {
	return nil
}

func (c *client) getHomePath(ctx context.Context) (string, error) {
	err := c.refreshToken(ctx)
	if err != nil {
		return "", err
	}

	type homePathResp struct {
		Data struct {
			Path string `json:"path"`
		} `json:"data"`
	}
	reqPath := fmt.Sprintf("/v1/ai/api/file/homeDir?clusterId=%s", c.clusterId())
	resp := &homePathResp{}
	err = c.sendHttpRequest(ctx, types.HttpMethodGet, reqPath, "", resp)
	if err != nil {
		return "", err
	}

	rootPathKey := c.redisStorage.MakeStorageKey([]string{}, types.StoragePrefixScowRootPath)
	_ = c.redisStorage.SetWithTimeout(rootPathKey, []byte(resp.Data.Path), types.DefaultTokenExpireTime)

	return resp.Data.Path, nil
}

func (c *client) getClusterId(ctx context.Context) (string, error) {
	err := c.refreshToken(ctx)
	if err != nil {
		return "", err
	}

	type configResp struct {
		Data struct {
			ClusterSortedIdList []string `json:"CLUSTER_SORTED_ID_LIST"`
		} `json:"data"`
	}
	reqPath := fmt.Sprintf("/v1/ai/api/config")
	resp := &configResp{}
	err = c.sendHttpRequest(ctx, types.HttpMethodGet, reqPath, "", resp)
	if err != nil {
		return "", err
	}
	if len(resp.Data.ClusterSortedIdList) <= 0 {
		return "", types.ErrorNotExists
	}

	clusterIdKey := c.redisStorage.MakeStorageKey([]string{}, types.StoragePrefixScowClusterId)
	id := resp.Data.ClusterSortedIdList[0]
	_ = c.redisStorage.SetWithTimeout(clusterIdKey, []byte(id), types.DefaultTokenExpireTime)

	return id, nil
}
