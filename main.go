package main

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"gcs-client/app/env"
	"gcs-client/app/helper"
	"github.com/andypangaribuan/evo-golang/v-log"
	"github.com/andypangaribuan/evo-golang/v-utils"
	"github.com/andypangaribuan/vision-go"
	"github.com/andypangaribuan/vision-go/models"
	"github.com/andypangaribuan/vision-go/vis"
	"github.com/machinebox/progress"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"io"
	"log"
	"os"
	"strings"
	"time"
)


type GCSClientUploader struct {
	cl            *storage.Client
	baseUploadDir string
}

type GCSClientDownloader struct {
	cl              *storage.Client
	baseDownloadDir string
}


func main() {
	vision.Initialize()

	fmt.Printf("START GCS CLIENT\n\n")
	defer fmt.Printf("\n\nSTOP GCS CLIENT")

	//testWrite()

	ctx := context.Background()
	client, err := storage.NewClient(ctx, option.WithCredentialsFile(env.ServiceAccountPath))
	if err != nil {
		errMsg := v_log.Log.Stack(err)
		log.Fatal(*errMsg)
	}

	if env.ShowBucketList {
		bucketList(client)
	}


	if env.DirUpload != "-" {
		baseUploadDir := env.GCSUploadDir
		if baseUploadDir == "-" {
			baseUploadDir = ""
		}
		if baseUploadDir != "" {
			if baseUploadDir[len(baseUploadDir)-1:] != "/" {
				baseUploadDir += "/"
			}
		}
		clUploader := &GCSClientUploader{
			cl:            client,
			baseUploadDir: baseUploadDir,
		}
		clUploader.upload()
	}


	if env.DirDirectory != "-" {
		baseDownloadDir := env.GCSDownloadDir
		if baseDownloadDir == "-" {
			baseDownloadDir = ""
		}
		if baseDownloadDir != "" {
			if baseDownloadDir[len(baseDownloadDir)-1:] != "/" {
				baseDownloadDir += "/"
			}
		}
		clDownload := &GCSClientDownloader{
			cl:              client,
			baseDownloadDir: baseDownloadDir,
		}
		clDownload.download()
	}
}


func testWrite() {
	fmt.Printf("\n\nTest write\n")

	tmStart := time.Now()
	idx := 0

	//fmt.Printf("Data")
	//fmt.Printf("\rLoad number A")
	//time.Sleep(time.Second * 3)
	//fmt.Printf("\rLoad number B")
	//time.Sleep(time.Second * 3)
	//fmt.Printf("\rLoad number C")
	//time.Sleep(time.Second * 3)

	for {
		idx++
		diff := time.Since(tmStart)
		if diff.Seconds() >= 10 {
			break
		}

		fmt.Printf("\rLoad number %v", idx)
		time.Sleep(time.Millisecond * 1)

	}

	fmt.Printf("\nDONE\n")
	os.Exit(1)
}


func (c *GCSClientUploader) upload() {
	dirUpload := env.DirUpload
	if dirUpload[len(dirUpload)-1:] != vis.Const.PathSeparator {
		dirUpload += vis.Const.PathSeparator
	}

	fmt.Printf("Upload dir: %v\n", dirUpload)

	files, err := vis.Util.ScanFiles(true, -1, dirUpload, []string{}, nil)
	logFatal(err)

	loopLogic := func(file models.FileScan) {
		uploadDir := strings.Replace(file.DirPath + vis.Const.PathSeparator, dirUpload, "", 1)
		uploadDir = strings.Replace(uploadDir, file.FileName, "", 1)

		if c.baseUploadDir != "" && c.baseUploadDir != "-" {
			v := strings.Replace(c.baseUploadDir, `\`, "/", -1)
			if v[len(v)-1:] != "/" {
				v += "/"
			}
			uploadDir = v + uploadDir
		}

		uploadDir = strings.Replace(uploadDir, `\`, "/", -1)
		if uploadDir != "" {
			if uploadDir[len(uploadDir)-1:] != "/" {
				uploadDir += "/"
			}
		}


		fmt.Printf("Uploading: %v\n", file.FilePath)
		var (
			tmStart = time.Now()
			ctx     = context.Background()
		)

		f, err := os.Open(file.FilePath)
		if err != nil {
			errMsg := v_log.Log.Stack(err)
			fmt.Printf("\nERROR\n%v", *errMsg)
			return
		}
		defer f.Close()

		fi, err := f.Stat()
		if err != nil {
			errMsg := v_log.Log.Stack(err)
			fmt.Printf("\nERROR\n%v", *errMsg)
			return
		}

		wc := c.cl.Bucket(env.BucketName).Object(uploadDir + file.FileName).NewWriter(ctx)
		wcClosed := false
		defer func() {
			if !wcClosed {
				_ = wc.Close()
			}
		}()
		proWriter := progress.NewWriter(wc)

		fmt.Printf("Uploading: %v\n", ByteCountSI(fi.Size()))
		isCopyError := false

		progressFinished := make(chan bool)
		go func() {
			if isCopyError {
				return
			}

			hisTransfer := []string{ "0%", "0%" }
			ctx := context.Background()
			progressChan := progress.NewTicker(ctx, proWriter, fi.Size(), time.Second)

			for p := range progressChan {
				percentageTransfer := fmt.Sprintf("%.2f%%", p.Percent())
				if percentageTransfer == "0.00%" {
					percentageTransfer = "0%"
				}
				if hisTransfer[1] != percentageTransfer {
					hisTransfer[0] = hisTransfer[1]
					hisTransfer[1] = percentageTransfer
				}

				//fmt.Printf("\rUploading: %v %v remaining", hisTransfer[0], p.Remaining().Round(time.Second))
				fmt.Printf("\rUploading: %v | %v", hisTransfer[0], time.Now().Sub(tmStart).Round(time.Second))
				if isCopyError {
					return
				}
			}
			fmt.Printf("\rUploading: 100%% | %v\n", time.Now().Sub(tmStart).Round(time.Second))
			progressFinished <- true
		}()

		if _, err := io.Copy(proWriter, f); err != nil {
			isCopyError = true
			errMsg := v_log.Log.Stack(err)
			fmt.Printf("\nERROR\n%v", *errMsg)
			return
		}

		<-progressFinished
		wcClosed = true

		_tmStart := time.Now()
		done := FuncTicker(time.Second, func() {
			fmt.Printf("\rUploading: finalize | %v", time.Now().Sub(_tmStart).Round(time.Second))
		})
		defer func() { done <- true }()

		if err := wc.Close(); err != nil {
			errMsg := v_log.Log.Stack(err)
			fmt.Printf("\nERROR\n%v", *errMsg)
			return
		}

		tmDiff := helper.GetTimeDiff(tmStart, time.Now())
		fmt.Printf("\nDuration: %v\n\n", tmDiff)
	}

	for _, file := range files {
		loopLogic(file)
	}
}


func bucketList(client *storage.Client) []string {
	fmt.Printf("\n:: Bucket List")

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	it := client.Bucket(env.BucketName).Objects(ctx, nil)

	fileNames := make([]string, 0)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			panic(err)
		}

		attrName := attrs.Name
		if len(attrName) > 0 && attrName[len(attrName)-1:] == "/" {
			continue
		}
		fmt.Printf("\n- %v", attrName)
		fileNames = append(fileNames, attrName)
	}

	return fileNames
}


func (c *GCSClientDownloader) download() {
	fmt.Printf("\n:: Bucket Download")

	logicDownload := func(fileName string) {
		fmt.Printf("\n:: Download file %v", fileName)
		tmStart := time.Now()

		ctx := context.Background()
		// dont use time, because it will timeout when download big size file
		//ctx, cancel := context.WithTimeout(ctx, time.Second * 10)
		//defer cancel()

		gcsFilePath := c.baseDownloadDir + fileName
		rc, err := c.cl.Bucket(env.BucketName).Object(gcsFilePath).NewReader(ctx)
		if err != nil {
			if strings.Contains(err.Error(), "object doesn't exist") {
				fmt.Printf("\n:: File not found: %v", fileName)
				fmt.Printf("\n- %v\n- %v\n", env.BucketName, gcsFilePath)
			} else {
				errLog(err)
			}
			return
		}
		defer rc.Close()

		filePath := env.DirDirectory + vis.Const.PathSeparator + fileName

		fo, err := os.Create(filePath)
		if err != nil {
			panic(err)
		}
		defer func() {
			if err := fo.Close(); err != nil {
				panic(err)
			}
		}()

		buf := make([]byte, 1024)
		for {
			n, err := rc.Read(buf)
			if err != nil && err != io.EOF {
				panic(err)
			}
			if n == 0 {
				break
			}

			if _, err := fo.Write(buf[:n]); err != nil {
				panic(err)
			}
		}

		tmDone := time.Now()
		tmDiff := helper.GetTimeDiff(tmStart, tmDone)
		fmt.Printf("\n:: Done, duration  %v", tmDiff)
	}

	if env.GcsFileDownload != "*" {
		logicDownload(env.GcsFileDownload)
		if env.DeleteAfterDownload {
			ctx := context.Background()
			_ = c.cl.Bucket(env.BucketName).Object(c.baseDownloadDir + env.GcsFileDownload).Delete(ctx)
		}
	}
}


func errLog(err error) {
	if err != nil {
		errMsg := v_log.Log.BaseStack(2, err)
		fmt.Printf("\nERROR\n%v", errMsg)
	}
}

func logFatal(err error) {
	if err != nil {
		errMsg := v_log.Log.BaseStack(2, err)
		logPrintln("ERROR\n%v", errMsg)
		os.Exit(1)
	}
}

func logPrintln(format string, pars ...interface{}) {
	logPrint(format + "\n", pars...)
}

func logPrint(format string, pars ...interface{}) {
	args := []interface{}{ v_utils.Utils.Time2StrFull(time.Now()) }
	if len(pars) > 0 {
		args = append(args, pars...)
	}

	newLineIdx := -1
	for i:=0; i<len(format); i++ {
		ch := format[i:i+1]
		if ch == "\n" {
			newLineIdx = i
		} else {
			break
		}
	}

	if newLineIdx == -1 {
		format = "%v " + format
	} else {
		format = format[:newLineIdx+1] + "%v " + format[newLineIdx+1:]
	}

	fmt.Printf(format, args...)
}




func ByteCountSI(b int64) string {
	const unit = 1000
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}

	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "kMGTPE"[exp])
}

func FuncTicker(duration time.Duration, fn func()) chan bool {
	done := make(chan bool)
	go func() {
		defer close(done)
		fn()
		for {
			select {
			case <-done:
				return
			case <-time.After(duration):
				fn()
			}
		}
	}()
	return done
}
