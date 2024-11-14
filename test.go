package main

import (
	"LT-scow-sdk/scow"
	"context"
	"io"
	"log"
	"os"
	"time"
)

// - init client
func initClient() (scow.Client, error) {
	client, err := scow.New("xiaoyan", "Xiaoyan@123", "100.127.0.124:30090",
		"100.127.0.124:30080", []string{"192.168.242.28:6379"}, "dragonfly", false)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func main() {

	ctx := context.Background()
	scowClient, _ := initClient()
	//
	//signedUrl, err := scowClient.GetDownloadLink(ctx, "urchincache", "jobs/node/attachments/mariadb_10.6.tar", 0, hashKey)
	//if err != nil {
	//	log.Printf("GetDownloadLink err:%v", err)
	//}
	//log.Printf("GetDownloadLink signedUrl:%v", signedUrl)

	//==========================================================================
	//exist, err := scowClient.StorageVolExists(ctx, "grampus2")
	//if err != nil {
	//	log.Printf("err:%v", err)
	//	return
	//}
	//log.Printf("exist:%v", exist)

	//vols, err := scowClient.ListStorageVols(ctx)
	//if err != nil {
	//	log.Printf("err:%v", err)
	//	return
	//}
	//log.Printf("vols:%v", vols)

	//exist, err := scowClient.StatFile(ctx, "urchincache", "bootstrap")
	//if err != nil {
	//	log.Printf("err:%v", err)
	//	return
	//}
	//log.Printf("exist:%v", exist)

	fd, _ := os.Open("E:\\Exchange_dir\\tmp\\ipfs\\data\\init_model.car")
	var readCloser io.ReadCloser = fd
	err := scowClient.UploadFile(ctx, "urchincache", "glin/demo_x/init_model.car", "", readCloser)
	if err != nil {
		log.Printf("UploadFile err:%v", err)
		return
	}

	//err := scowClient.MakeStorageVol(ctx, "grampus")
	//if err != nil {
	//	log.Printf("err:%v", err)
	//	return
	//}

	//err := scowClient.CreateDir(ctx, "urchincache", "glin/demo_x")
	//if err != nil {
	//	log.Printf("err:%v", err)
	//	return
	//}

	//err := scowClient.RemoveFile(ctx, "urchincache", "glin/demo_x/bootstrap")
	//if err != nil {
	//	log.Printf("RemoveFile err:%v", err)
	//	return
	//}

	//rdc, err := scowClient.DownloadFile(ctx, "urchincache", "glin/demo_x/R-50.pkl")
	//if err != nil {
	//	log.Printf("DownloadFile err:%v", err)
	//	return
	//}
	//file, err := os.Create("E:\\Exchange_dir\\tmp\\ipfs\\data\\R-50.pkl.copy")
	//_, err = io.Copy(file, rdc)
	//if err != nil {
	//	log.Printf("err:%v", err)
	//	return
	//}

	//exist, err := scowClient.ListDirFiles(ctx, "urchincache", "test3")
	//if err != nil {
	//	log.Printf("err:%v", err)
	//	return
	//}
	//for _, file := range exist {
	//	log.Printf("Key:%s, Size:%v, modify:%v", file.Key, file.Size, file.LastModified)
	//}

	//file, exist, err := scowClient.StatFolder(ctx, "urchincache", "glin/")
	//if err != nil {
	//	log.Printf("err:%v", err)
	//	return
	//}
	//log.Printf("exist:%v:%v", exist, file)

	signedUrl, err := scowClient.GetDownloadLink(ctx, "urchincache", "glin/demo_x/init_model.car", time.Second*60)
	if err != nil {
		log.Printf("err:%v", err)
		return
	}
	log.Printf("signedUrl:%s", signedUrl)

	log.Printf("ok...")
}
