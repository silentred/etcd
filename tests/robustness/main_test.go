// Copyright 2022 The etcd Authors
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

package robustness

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"golang.org/x/sync/errgroup"

	"go.etcd.io/etcd/tests/v3/framework"
	"go.etcd.io/etcd/tests/v3/framework/e2e"
	"go.etcd.io/etcd/tests/v3/robustness/client"
	"go.etcd.io/etcd/tests/v3/robustness/failpoint"
	"go.etcd.io/etcd/tests/v3/robustness/identity"
	"go.etcd.io/etcd/tests/v3/robustness/report"
	"go.etcd.io/etcd/tests/v3/robustness/scenarios"
	"go.etcd.io/etcd/tests/v3/robustness/traffic"
	"go.etcd.io/etcd/tests/v3/robustness/validate"
)

var testRunner = framework.E2eTestRunner

var (
	WaitBeforeFailpoint = time.Second
	WaitJitter          = traffic.DefaultCompactionPeriod
	WaitAfterFailpoint  = time.Second
)

func TestMain(m *testing.M) {
	testRunner.TestMain(m)
}

func TestRobustnessExploratory(t *testing.T) {
	testRunner.BeforeTest(t)
	for _, s := range scenarios.Exploratory(t) {
		t.Run(s.Name, func(t *testing.T) {
			lg := zaptest.NewLogger(t)
			s.Cluster.Logger = lg
			ctx := t.Context()
			c, err := e2e.NewEtcdProcessCluster(ctx, t, e2e.WithConfig(&s.Cluster))
			require.NoError(t, err)
			defer forcestopCluster(c)
			s.Failpoint, err = failpoint.PickRandom(c, s.Profile)
			require.NoError(t, err)
			t.Run(s.Failpoint.Name(), func(t *testing.T) {
				testRobustness(ctx, t, lg, s, c)
			})
		})
	}
}

func TestRobustnessRegression(t *testing.T) {
	testRunner.BeforeTest(t)
	for _, s := range scenarios.Regression(t) {
		t.Run(s.Name, func(t *testing.T) {
			lg := zaptest.NewLogger(t)
			s.Cluster.Logger = lg
			ctx := t.Context()
			c, err := e2e.NewEtcdProcessCluster(ctx, t, e2e.WithConfig(&s.Cluster))
			require.NoError(t, err)
			defer forcestopCluster(c)
			testRobustness(ctx, t, lg, s, c)
		})
	}
}

func testRobustness(ctx context.Context, t *testing.T, lg *zap.Logger, s scenarios.TestScenario, c *e2e.EtcdProcessCluster) {
	serverDataPaths := report.ServerDataPaths(c)
	r := report.TestReport{
		Logger:          lg,
		ServersDataPath: serverDataPaths,
		Traffic:         &report.TrafficDetail{ExpectUniqueRevision: s.Traffic.ExpectUniqueRevision()},
	}
	// t.Failed() returns false during panicking. We need to forcibly
	// save data on panicking.
	// Refer to: https://github.com/golang/go/issues/49929
	panicked := true
	defer func() {
		_, persistResults := os.LookupEnv("PERSIST_RESULTS")
		shouldReport := t.Failed() || panicked || persistResults
		path := testResultsDirectory(t)
		if shouldReport {
			if err := r.Report(path); err != nil {
				t.Error(err)
			}
		}
	}()
	r.Client = runScenario(ctx, t, s, lg, c)
	persistedRequests, err := report.PersistedRequestsCluster(lg, c)
	if err != nil {
		t.Error(err)
	}

	validateConfig := validate.Config{ExpectRevisionUnique: s.Traffic.ExpectUniqueRevision()}
	result := validate.ValidateAndReturnVisualize(lg, validateConfig, r.Client, persistedRequests, 5*time.Minute)
	r.Visualize = result.Linearization.Visualize
	err = result.Error()
	if err != nil {
		t.Error(err)
	}
	panicked = false
}

func runScenario(ctx context.Context, t *testing.T, s scenarios.TestScenario, lg *zap.Logger, clus *e2e.EtcdProcessCluster) (reports []report.ClientReport) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	g := errgroup.Group{}
	var failpointClientReport []report.ClientReport
	failpointInjected := make(chan report.FailpointInjection, 1)

	// using baseTime time-measuring operation to get monotonic clock reading
	// see https://github.com/golang/go/blob/master/src/time/time.go#L17
	baseTime := time.Now()
	ids := identity.NewIDProvider()
	g.Go(func() error {
		defer close(failpointInjected)
		// Give some time for traffic to reach qps target before injecting failpoint.
		time.Sleep(randomizeTime(WaitBeforeFailpoint, WaitJitter))
		fr, err := failpoint.Inject(ctx, t, lg, clus, s.Failpoint, baseTime, ids)
		if err != nil {
			t.Error(err)
			cancel()
		}
		// Give some time for traffic to reach qps target after injecting failpoint.
		time.Sleep(randomizeTime(WaitAfterFailpoint, WaitJitter))
		if fr != nil {
			failpointInjected <- fr.FailpointInjection
			failpointClientReport = fr.Client
		}
		return nil
	})
	trafficSet := client.NewSet(ids, baseTime)
	defer trafficSet.Close()
	maxRevisionChan := make(chan int64, 1)
	g.Go(func() error {
		defer close(maxRevisionChan)
		operationReport := traffic.SimulateTraffic(ctx, t, lg, clus, s.Profile, s.Traffic, failpointInjected, trafficSet)
		maxRevision := report.OperationsMaxRevision(operationReport)
		maxRevisionChan <- maxRevision
		lg.Info("Finished simulating Traffic", zap.Int64("max-revision", maxRevision))
		return nil
	})
	watchSet := client.NewSet(ids, baseTime)
	defer watchSet.Close()
	g.Go(func() error {
		endpoints := processEndpoints(clus)
		err := client.CollectClusterWatchEvents(ctx, lg, endpoints, maxRevisionChan, s.Watch, watchSet)
		return err
	})
	err := g.Wait()
	if err != nil {
		t.Error(err)
	}

	err = client.CheckEndOfTestHashKV(ctx, clus)
	if err != nil {
		t.Error(err)
	}
	return slices.Concat(trafficSet.Reports(), watchSet.Reports(), failpointClientReport)
}

func randomizeTime(base time.Duration, jitter time.Duration) time.Duration {
	return base - jitter + time.Duration(rand.Int63n(int64(jitter)*2))
}

// forcestopCluster stops the etcd member with signal kill.
func forcestopCluster(clus *e2e.EtcdProcessCluster) error {
	for _, member := range clus.Procs {
		member.Kill()
	}
	return clus.ConcurrentStop()
}

func testResultsDirectory(t *testing.T) string {
	resultsDirectory, ok := os.LookupEnv("RESULTS_DIR")
	if !ok {
		resultsDirectory = "/tmp/"
	}
	resultsDirectory, err := filepath.Abs(resultsDirectory)
	if err != nil {
		panic(err)
	}
	path, err := filepath.Abs(filepath.Join(
		resultsDirectory, strings.ReplaceAll(t.Name(), "/", "_"), fmt.Sprintf("%v", time.Now().UnixNano())))
	require.NoError(t, err)
	err = os.RemoveAll(path)
	require.NoError(t, err)
	err = os.MkdirAll(path, 0o700)
	require.NoError(t, err)
	return path
}

func processEndpoints(clus *e2e.EtcdProcessCluster) []string {
	endpoints := make([]string, 0, len(clus.Procs))
	for _, proc := range clus.Procs {
		endpoints = append(endpoints, proc.EndpointsGRPC()[0])
	}
	return endpoints
}
