package ch

import (
	"fmt"
	"path"

	"github.com/YenchangChan/ch2s3/config"
	"github.com/YenchangChan/ch2s3/log"
	"github.com/YenchangChan/ch2s3/utils"
)

func u_init(opts utils.SshOptions, cwd string) error {
	//上传s3uploader 到对端机器
	if err := utils.ScpUploadFile(path.Join(cwd, "bin", "s3uploader"), "/tmp/s3uploader", opts); err != nil {
		return err
	}
	if _, err := utils.RemoteExecute(opts, "chmod u+x /tmp/s3uploader"); err != nil {
		return err
	}
	return nil
}

func u_done(opts utils.SshOptions) error {
	if _, err := utils.RemoteExecute(opts, "rm -f /tmp/s3uploader"); err != nil {
		return err
	}
	return nil
}

func Upload(opts utils.SshOptions, paths map[string]utils.PathInfo, conf config.S3, cwd string) error {
	if err := u_init(opts, cwd); err != nil {
		return err
	}
	//执行s3uploader 命令
	/*
		./s3uploader
			-b 19700101/default.test_ck_dataq_r30/192.168.101.93/data/default/test_ck_dataq_r30/19700101_0_0_0
			-f /data01/clickhouse/store/3cc/3ccf8474-fa31-469f-8ace-26ece20686d6/19700101_0_0_0
			-a VdmPbwvMlH8ryeqW
			-s 8z16tUktXpvcjjy5M4MqXvCks5MMHb63
			-r zh-west-1
			-e http://192.168.101.94:49000/backup
	*/
	pathInfo := make(map[string]utils.PathInfo)
	for k, v := range paths {
		newKey := path.Dir(k)
		if _, ok := pathInfo[newKey]; !ok {
			if v.Host == opts.Host {
				newValue := utils.PathInfo{
					Host:  v.Host,
					MD5:   v.MD5,
					RPath: newKey,
					LPath: path.Dir(v.LPath),
				}
				pathInfo[newKey] = newValue
			}
		}
	}

	for _, v := range pathInfo {
		log.Logger.Debugf("[%s]lpath: %s, rpath: %v", opts.Host, v.LPath, v.RPath)
		cmd := fmt.Sprintf("/tmp/s3uploader -b %s -f %s -a %s -s %s -r %s -e %s",
			v.RPath, v.LPath, conf.AccessKey, conf.SecretKey, conf.Region, conf.Endpoint)
		log.Logger.Infof("[%s]cmd: %s", opts.Host, cmd)
		if _, err := utils.RemoteExecute(opts, cmd); err != nil {
			return err
		}

	}
	//删除s3uploader工具
	if err := u_done(opts); err != nil {
		return err
	}
	return nil
}

func UploadFiles(opts utils.SshOptions, paths map[string]utils.PathInfo, conf config.S3, cwd string) error {
	if err := u_init(opts, cwd); err != nil {
		return err
	}
	pathInfo := make(map[string]utils.PathInfo)
	for k, v := range paths {
		newKey := path.Dir(k)
		if pinfo, ok := pathInfo[newKey]; !ok {
			if v.Host == opts.Host {
				newValue := utils.PathInfo{
					Host:  v.Host,
					MD5:   v.MD5,
					RPath: newKey,
					LPath: v.LPath,
				}
				pathInfo[newKey] = newValue
			}
		} else {
			if v.Host == opts.Host {
				lpath := pinfo.LPath + "," + v.LPath
				newValue := utils.PathInfo{
					Host:  v.Host,
					MD5:   v.MD5,
					RPath: newKey,
					LPath: lpath,
				}
				pathInfo[newKey] = newValue
			}
		}
	}

	for _, v := range pathInfo {
		log.Logger.Infof("[%s]s3uploader rpath: %v", opts.Host, v.RPath)
		if conf.CheckCnt {
			cmd := fmt.Sprintf("for lpath in `ls %s`; do /tmp/s3uploader -b %s -f %s$lpath -a %s -s %s -r %s -e %s; done",
				v.LPath, v.RPath, v.LPath, conf.AccessKey, conf.SecretKey, conf.Region, conf.Endpoint)
			log.Logger.Infof("[%s]cmd: %s", opts.Host, cmd)
			if _, err := utils.RemoteExecute(opts, cmd); err != nil {
				return err
			}
		} else {
			cmd := fmt.Sprintf("/tmp/s3uploader -b %s -f %s -a %s -s %s -r %s -e %s",
				v.RPath, v.LPath, conf.AccessKey, conf.SecretKey, conf.Region, conf.Endpoint)
			log.Logger.Debugf("[%s]cmd: %s", opts.Host, cmd)
			if _, err := utils.RemoteExecute(opts, cmd); err != nil {
				return err
			}
		}

	}
	if err := u_done(opts); err != nil {
		return err
	}
	return nil
}
