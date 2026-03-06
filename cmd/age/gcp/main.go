// Copyright 2026 The Tessera authors. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"time"

	gcs "cloud.google.com/go/storage"
	"google.golang.org/api/option"

	"k8s.io/klog/v2"
)

var (
	bucket         = flag.String("bucket", "", "Log bucket.")
	object         = flag.String("object", "checkpoint", "Object to inspect for age.")
	interval       = flag.Duration("interval", 100*time.Millisecond, "Interval between checks")
	gcsUseGRPC     = flag.Bool("gcs_use_grpc", false, "Use gRPC-based GCS client.")
	gcsConnections = flag.Int("gcs_connections", 20, "Size of connection pool for GCS gRPC client.")
)

func main() {
	klog.InitFlags(nil)
	flag.Parse()
	ctx := context.Background()

	s := &gcsStorage{
		bucket:       *bucket,
		bucketPrefix: "",
		gcsClient:    gcsClientFromFlags(ctx, http.DefaultClient),
	}

	t := time.NewTicker(*interval)
	for {
		<-t.C
		_, a, err := s.getObject(ctx, *object)
		if err != nil {
			klog.Infof("%s:\t %v", *object, err)
			continue
		}
		klog.Infof("%s:\t%v", *object, time.Since(a.LastModified))
	}

}

func gcsClientFromFlags(ctx context.Context, httpClient *http.Client) *gcs.Client {
	if *gcsUseGRPC {
		gcsClient, err := gcs.NewGRPCClient(ctx, option.WithGRPCConnectionPool(*gcsConnections))
		if err != nil {
			klog.Exitf("Failed to create gRPC GCS client: %v", err)
		}
		return gcsClient
	}

	gcsClient, err := gcs.NewClient(ctx, gcs.WithJSONReads(), option.WithHTTPClient(httpClient))
	if err != nil {
		klog.Exitf("Failed to create GCS client: %v", err)
	}
	return gcsClient
}

// gcsStorage knows how to store and retrieve objects from GCS.
type gcsStorage struct {
	bucket       string
	bucketPrefix string
	gcsClient    *gcs.Client
}

// getObject returns the data and generation of the specified object, or an error.
func (s *gcsStorage) getObject(ctx context.Context, obj string) ([]byte, *gcs.ReaderObjectAttrs, error) {
	if s.bucketPrefix != "" {
		obj = filepath.Join(s.bucketPrefix, obj)
	}

	r, err := s.gcsClient.Bucket(s.bucket).Object(obj).NewReader(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("getObject: failed to create reader for object %q in bucket %q: %w", obj, s.bucket, err)
	}

	d, err := io.ReadAll(r)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read %q: %v", obj, err)
	}
	return d, &r.Attrs, r.Close()
}
