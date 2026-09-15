package storage

import (
	"context"
	"crypto/tls"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// newS3Client builds an S3 client either from provided AuthData credentials
// ("S3" uses the default AWS credential chain when no creds present) or from
// the environment/default chain. MinIO clients always rely on the configured
// endpoint and path-style addressing.
func newS3Client(auth *AuthData, providerType string) (*s3.Client, error) {
	if auth == nil || auth.Creds == nil {
		// Default AWS credential chain.
		cfg, err := awsconfig.LoadDefaultConfig(context.TODO())
		if err != nil {
			return nil, err
		}
		return s3.NewFromConfig(cfg), nil
	}

	accessKey := auth.GetCredential("access_key")
	secretKey := auth.GetCredential("secret_key")
	region := auth.GetCredential("region")

	opts := []func(*awsconfig.LoadOptions) error{}
	if providerType == "MINIO" {
		// MinIO does not rely on the AWS region resolution; default to
		// us-east-1 when the user did not provide one.
		if region == "" {
			region = "us-east-1"
		}
		opts = append(opts, awsconfig.WithRegion(region))
	} else if region != "" {
		opts = append(opts, awsconfig.WithRegion(region))
	}
	cfg, err := awsconfig.LoadDefaultConfig(context.TODO(), opts...)
	if err != nil {
		return nil, err
	}
	if accessKey != "" || secretKey != "" {
		cfg.Credentials = credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")
	}

	if providerType == "MINIO" {
		endpoint := auth.GetCredential("endpoint")
		if endpoint == "" {
			endpoint = "http://minio-service.minio:9000"
		}
		verify := true
		if v, ok := auth.Creds["verify"].(bool); ok {
			verify = v
		}
		return s3.NewFromConfig(cfg, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
			if !verify && strings.HasPrefix(endpoint, "https://") {
				o.HTTPClient = awshttp.NewBuildableClient().WithTransportOptions(func(tr *http.Transport) {
					tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
				})
			}
		}), nil
	}
	return s3.NewFromConfig(cfg), nil
}

func copyToFile(r io.Reader, dest string) (int64, error) {
	f, err := os.Create(dest)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	return io.Copy(f, r)
}