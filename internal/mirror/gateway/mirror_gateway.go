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

package gateway

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"

	"log/slog"

	"github.com/avast/retry-go/v4"
	"github.com/transparency-dev/tessera/client"
	"github.com/transparency-dev/tessera/client/mirror"
	"golang.org/x/mod/sumdb/note"
)

// WitnessGroup defines the subset of tessera.WitnessGroup methods needed by the gateway.
type WitnessGroup interface {
	WitnessEndpoints() map[string][]note.Verifier
}

// LogReader defines the subset of tessera.LogReader methods needed by the gateway.
type LogReader interface {
	ReadTile(ctx context.Context, level, index uint64, p uint8) ([]byte, error)
	ReadEntryBundle(ctx context.Context, index uint64, p uint8) ([]byte, error)
}

// goal represents a desired state for a mirror.
// It contains a target checkpoint (and its size broken out for simplicity).
type goal struct {
	cpSize uint64
	cp     []byte
}

// mirrorTarget represents a tlog-mirror service which we'll attempt to update.
type mirrorTarget struct {
	url    *url.URL
	client *mirror.Client
}

// Gateway manages the process of keeping mirrors up-to-date.
type Gateway struct {
	httpClient *http.Client
	lr         LogReader
	targets    []*mirrorTarget
}

// Options represents the configuration for a Gateway.
type Options struct {
	// HTTPClient is the HTTP client to use for all HTTP operations, if nil uses the DefaultHTTPClient.
	HTTPClient *http.Client
	// Mirrors defines the pool of mirrors to update.
	Mirrors []*url.URL
	// LogReader provides access to the main log.
	LogReader LogReader
	// LogOrigin is the origin ID of the log.
	LogOrigin string
}

// NewGateway creates a new Gateway that will keep mirrors up-to-date.
func NewGateway(ctx context.Context, opts Options) (*Gateway, error) {
	if opts.HTTPClient == nil {
		opts.HTTPClient = http.DefaultClient
	}

	g := &Gateway{
		httpClient: opts.HTTPClient,
		lr:         opts.LogReader,
	}

	endpoints := opts.Mirrors
	for _, u := range endpoints {
		mirrorFetcher, err := client.NewHTTPFetcher(u, opts.HTTPClient)
		if err != nil {
			return nil, fmt.Errorf("invalid mirror URL %v: %v", u, err)
		}

		mOpts := mirror.NewOptions().
			WithMirrorURL(u).
			WithHTTPClient(opts.HTTPClient).
			WithLogOrigin(opts.LogOrigin).
			WithTileFetcher(opts.LogReader.ReadTile).
			WithBundleFetcher(opts.LogReader.ReadEntryBundle).
			WithMirrorCheckpointFetcher(mirrorFetcher.ReadCheckpoint)

		c, err := mirror.NewClient(ctx, mOpts)
		if err != nil {
			slog.ErrorContext(ctx, "Failed to create mirror client", slog.String("url", u.String()), slog.Any("error", err))
			continue
		}

		target := &mirrorTarget{
			url:    u,
			client: c,
		}
		g.targets = append(g.targets, target)
	}

	return g, nil
}

// CosignCheckpoint updates the goals for all mirrors and returns a channel on which it will send
// cosignatures as they are successfully fetched from the mirrors.
// The channel is closed once all mirrors' signatures have been sent or the context is canceled.
func (g *Gateway) CosignCheckpoint(ctx context.Context, cp []byte, cpSize uint64) <-chan []byte {
	out := make(chan []byte, len(g.targets))

	if len(g.targets) == 0 {
		close(out)
		return out
	}

	wg := sync.WaitGroup{}

	// Send goals to each of the target workers, but don't block if they're
	// already busy.
	for _, target := range g.targets {
		wg.Go(func() {
			var sigs []byte
			err := retry.Do(
				func() error {
					var err error
					slog.DebugContext(ctx, "Syncing mirror", slog.String("url", target.url.String()), slog.Uint64("size", cpSize))
					sigs, err = target.client.Sync(ctx, cp, cpSize)
					if err != nil {
						slog.WarnContext(ctx, "Mirror sync failed, retrying", slog.String("url", target.url.String()), slog.Any("error", err))
						return err
					}
					return nil
				},
				retry.Context(ctx),
				retry.DelayType(retry.BackOffDelay),
				retry.MaxDelay(5*time.Second),
			)
			if err != nil {
				slog.ErrorContext(ctx, "Mirror sync failed", slog.String("url", target.url.String()), slog.Any("error", err))
				return
			}
			slog.InfoContext(ctx, "Mirror sync succeeded", slog.String("url", target.url.String()), slog.Uint64("size", cpSize))
			out <- sigs
		})
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}
