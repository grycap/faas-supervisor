package storage

import (
	"context"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/faassupervisor/go/internal/events"
)

// S3 provider using the AWS SDK. Credentials come from AuthData or
// from the default AWS credential chain when auth.Creds is nil.
type S3 struct {
	auth *AuthData
}

func NewS3(auth *AuthData) *S3 { return &S3{auth: auth} }

func (s *S3) GetType() string { return "S3" }

func (s *S3) client() (*s3.Client, error) {
	return newS3Client(s.auth, "S3")
}

func (s *S3) DownloadFile(parsed events.Event, inputDir string) (string, error) {
	client, err := s.client()
	if err != nil {
		return "", err
	}
	localPath := filepath.Join(inputDir, parsed.GetFileName())
	out, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(parsed.GetBucketName()),
		Key:    aws.String(parsed.GetObjectKey()),
	})
	if err != nil {
		return "", err
	}
	defer out.Body.Close()
	if _, err := copyToFile(out.Body, localPath); err != nil {
		return "", err
	}
	return localPath, nil
}

func (s *S3) UploadFile(filePath, fileName, outputPath string) error {
	client, err := s.client()
	if err != nil {
		return err
	}
	bucket := GetBucketName(outputPath)
	fileKey := GetFileKey(outputPath, fileName)
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(fileKey),
		Body:   f,
	})
	return err
}

// Minio extends S3 with endpoint/minio-specific client configuration.
type Minio struct {
	auth *AuthData
}

func NewMinio(auth *AuthData) *Minio { return &Minio{auth: auth} }

func (m *Minio) GetType() string { return "MINIO" }

func (m *Minio) client() (*s3.Client, error) {
	return newS3Client(m.auth, "MINIO")
}

func (m *Minio) DownloadFile(parsed events.Event, inputDir string) (string, error) {
	client, err := m.client()
	if err != nil {
		return "", err
	}
	localPath := filepath.Join(inputDir, parsed.GetFileName())
	out, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(parsed.GetBucketName()),
		Key:    aws.String(parsed.GetObjectKey()),
	})
	if err != nil {
		return "", err
	}
	defer out.Body.Close()
	if _, err := copyToFile(out.Body, localPath); err != nil {
		return "", err
	}
	return localPath, nil
}

func (m *Minio) UploadFile(filePath, fileName, outputPath string) error {
	client, err := m.client()
	if err != nil {
		return err
	}
	bucket := GetBucketName(outputPath)
	fileKey := GetFileKey(outputPath, fileName)
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(fileKey),
		Body:   f,
	})
	return err
}