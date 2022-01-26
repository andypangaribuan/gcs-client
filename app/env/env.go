package env

import "github.com/andypangaribuan/evo-golang/v-env"

var (
	ServiceAccountPath  string
	BucketName          string
	ShowBucketList      bool
	GCSUploadDir        string
	DirUpload           string
	DirDirectory        string
	GCSDownloadDir      string
	GcsFileDownload     string
	DeleteAfterDownload bool
)


func init() {
	ServiceAccountPath = v_env.GetStrEnv("SERVICE_ACCOUNT_PATH")
	BucketName = v_env.GetStrEnv("BUCKET_NAME")
	ShowBucketList = v_env.GetBoolEnv("SHOW_BUCKET_LIST")
	GCSUploadDir = v_env.GetStrEnv("GCS_UPLOAD_DIR")
	DirUpload = v_env.GetStrEnv("DIR_UPLOAD")
	DirDirectory = v_env.GetStrEnv("DIR_DOWNLOAD")
	GCSDownloadDir = v_env.GetStrEnv("GCS_DOWNLOAD_DIR")
	GcsFileDownload = v_env.GetStrEnv("GCS_FILE_DOWNLOAD")
	DeleteAfterDownload = v_env.GetBoolEnv("DELETE_AFTER_DOWNLOAD")
}
