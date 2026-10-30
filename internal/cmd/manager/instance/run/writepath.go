/*
Copyright © contributors to CloudNativePG, established as
CloudNativePG a Series of LF Projects, LLC.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

SPDX-License-Identifier: Apache-2.0
*/

package run

import (
	"context"
	"time"

	"github.com/cloudnative-pg/machinery/pkg/log"
	"k8s.io/utils/clock"

	"github.com/cloudnative-pg/cloudnative-pg/internal/management/controller"
	"github.com/cloudnative-pg/cloudnative-pg/pkg/management/postgres"
	"github.com/cloudnative-pg/cloudnative-pg/pkg/management/postgres/writepath"
)

// suspendTimeout bounds the time given to the suspension of the primary lease
const suspendTimeout = 60 * time.Second

// newWritePathMonitor creates the runnable executing the write path check of the
// primary instance. When the write path is stalled, the instance suspends its
// primary lease and terminates its walsenders, so that a healthy replica can be
// promoted. See the `.spec.probes.liveness.writePathCheck` documentation.
func newWritePathMonitor(
	instance *postgres.Instance,
	leaseRunnable controller.PrimaryLeaseAcquirer,
) *writepath.Monitor {
	return &writepath.Monitor{
		State: &instance.WritePath,
		Clock: clock.RealClock{},
		Config: func() writepath.Config {
			cluster := instance.GetClusterOrDefault()
			return writepath.Config{
				Enabled:             cluster.IsWritePathCheckEnabled(),
				Period:              cluster.GetWritePathCheckPeriod(),
				Timeout:             cluster.GetWritePathCheckTimeout(),
				ClearAfterSuccesses: cluster.GetWritePathCheckClearAfterSuccesses(),
			}
		},
		ShouldRun: func() bool {
			if instance.MightBeUnavailable() || instance.IsFenced() {
				return false
			}
			isPrimary, err := instance.IsPrimary()
			if err != nil || !isPrimary {
				return false
			}
			return instance.IsReady() == nil
		},
		Beat: instance.WritePathHeartbeat,
		FailoverStarted: func() bool {
			targetPrimary := instance.GetClusterOrDefault().Status.TargetPrimary
			return targetPrimary != "" && targetPrimary != instance.GetPodName()
		},
		OnStalled: func() {
			// Don't block the monitor: the API server may be slow
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), suspendTimeout)
				defer cancel()
				if err := leaseRunnable.Suspend(ctx); err != nil {
					log.Error(err, "while suspending the primary lease")
				}
			}()
		},
		OnCleared: func() {
			// Only the target primary may hold the lease again. Otherwise, the
			// reconciliation loop resumes it when appropriate.
			cluster := instance.GetClusterOrDefault()
			if cluster.Status.TargetPrimary == instance.GetPodName() {
				leaseRunnable.Resume()
			}
		},
		WhileStalled: func(ctx context.Context) {
			// The walsenders of a primary with a blocked WAL flush keep the
			// WAL receivers of the replicas alive, and therefore the failover
			// waiting. They can reconnect until the operator removes this pod
			// from the read-write service, so this has to be repeated.
			if count, err := instance.TerminateWalSenders(ctx); err != nil {
				log.FromContext(ctx).Error(err, "while terminating the walsenders")
			} else if count > 0 {
				log.FromContext(ctx).Info("Terminated the walsenders of a primary with a stalled write path",
					"count", count)
			}
		},
	}
}
