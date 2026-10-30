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

package postgres

import (
	"context"
	"os"
	"path/filepath"
	"syscall"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("terminateProcesses", func() {
	var procDir string

	addProcess := func(pid, title string) {
		dir := filepath.Join(procDir, pid)
		Expect(os.MkdirAll(dir, 0o750)).To(Succeed())
		Expect(os.WriteFile(filepath.Join(dir, "cmdline"), []byte(title), 0o600)).To(Succeed())
	}

	BeforeEach(func() {
		procDir = GinkgoT().TempDir()
	})

	It("signals only the walsenders, whatever the cluster name", func(ctx SpecContext) {
		addProcess("10", "postgres: cluster-example: walsender streaming_replica 10.0.0.1(5432) streaming 0/1000\x00")
		addProcess("11", "postgres: walsender streaming_replica 10.0.0.2(5432) streaming 0/1000")
		addProcess("12", "postgres: cluster-example: walsender")
		addProcess("20", "postgres: cluster-example: walwriter")
		addProcess("21", "postgres: cluster-example: walreceiver streaming 0/1000")
		addProcess("22", "postgres: cluster-example: app app 10.0.0.9(1) idle")
		addProcess("30", "/controller/manager instance run")
		Expect(os.MkdirAll(filepath.Join(procDir, "self"), 0o750)).To(Succeed())
		Expect(os.MkdirAll(filepath.Join(procDir, "99"), 0o750)).To(Succeed()) // no cmdline

		var killed []int
		count, err := terminateProcesses(ctx, procDir, walSenderTitle, func(pid int, sig syscall.Signal) error {
			Expect(sig).To(Equal(syscall.SIGTERM))
			killed = append(killed, pid)
			return nil
		})

		Expect(err).ToNot(HaveOccurred())
		Expect(count).To(Equal(3))
		Expect(killed).To(ConsistOf(10, 11, 12))
	})

	It("ignores the processes that exited in the meantime", func(ctx SpecContext) {
		addProcess("10", "postgres: cluster-example: walsender streaming_replica 10.0.0.1(5432) streaming 0/1000")

		count, err := terminateProcesses(ctx, procDir, walSenderTitle, func(int, syscall.Signal) error {
			return syscall.ESRCH
		})

		Expect(err).ToNot(HaveOccurred())
		Expect(count).To(BeZero())
	})

	It("fails when the proc directory can't be read", func() {
		_, err := terminateProcesses(context.Background(), filepath.Join(procDir, "missing"), walSenderTitle, nil)
		Expect(err).To(HaveOccurred())
	})
})
