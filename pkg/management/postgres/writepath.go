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
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"syscall"

	"github.com/cloudnative-pg/machinery/pkg/log"
	"github.com/jackc/pgx/v5"
)

const (
	// writePathCheckApplicationName is the application name of the connection
	// used by the write path check
	writePathCheckApplicationName = "cnpg-write-path-check"

	// writePathCheckMessagePrefix is the prefix of the logical message emitted
	// by the write path check
	writePathCheckMessagePrefix = "cnpg-write-path-check"

	// writePathCheckQuery forces a local WAL flush. The synchronous commit is
	// explicitly limited to the local flush: the check has to measure the local
	// write path, not the synchronous standbys.
	writePathCheckQuery = "BEGIN; SET LOCAL synchronous_commit = local; " +
		"SELECT pg_catalog.pg_logical_emit_message(true, '" + writePathCheckMessagePrefix + "', ''); COMMIT;"
)

// walSenderTitle matches the process title of a walsender. Since `cluster_name`
// is always set, the title is "postgres: <cluster_name>: walsender ...".
var walSenderTitle = regexp.MustCompile(`^postgres: (?:[^ ]+: )?walsender( |$)`)

// WritePathHeartbeat performs a heartbeat of the write path check: it emits a
// transactional logical message, which forces a WAL flush at commit, waiting
// for the local flush only. It uses its own connection, that is kept between
// two calls and dropped on error, so that it never competes with the pool.
//
// The caller measures the duration of the call: a hung heartbeat is a stall,
// while an error is not.
func (instance *Instance) WritePathHeartbeat(ctx context.Context) error {
	if instance.writePathConn == nil || instance.writePathConn.IsClosed() {
		dsn := fmt.Sprintf(
			"host=%s port=%v user=%v sslmode=disable application_name=%v",
			GetSocketDir(),
			GetServerPort(),
			"postgres",
			writePathCheckApplicationName,
		)
		conn, err := pgx.Connect(ctx, dsn)
		if err != nil {
			return fmt.Errorf("while connecting for the write path check: %w", err)
		}
		instance.writePathConn = conn
	}

	// Without arguments, pgx uses the simple protocol, which accepts the
	// multiple statements of the query
	if _, err := instance.writePathConn.Exec(ctx, writePathCheckQuery); err != nil {
		_ = instance.writePathConn.Close(context.WithoutCancel(ctx))
		instance.writePathConn = nil
		return fmt.Errorf("while executing the write path heartbeat: %w", err)
	}

	return nil
}

// TerminateWalSenders sends SIGTERM to every walsender process of this
// PostgreSQL instance and returns the number of processes signalled.
//
// PostgreSQL can't do this on its own when its write path is blocked: the
// walsenders keep sending keepalives to the standbys, which therefore never
// notice that the primary is stuck.
func (instance *Instance) TerminateWalSenders(ctx context.Context) (int, error) {
	return terminateProcesses(ctx, "/proc", walSenderTitle, syscall.Kill)
}

// TerminateWalReceiver sends SIGTERM to the WAL receiver process of this
// PostgreSQL instance. It returns true if the process was signalled.
func (instance *Instance) TerminateWalReceiver(ctx context.Context) (bool, error) {
	superUserDB, err := instance.GetSuperUserDB()
	if err != nil {
		return false, err
	}

	var pid *int
	if err := superUserDB.QueryRowContext(ctx,
		"SELECT pid FROM pg_catalog.pg_stat_wal_receiver LIMIT 1").Scan(&pid); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	if pid == nil || *pid <= 0 {
		return false, nil
	}

	log.FromContext(ctx).Info("Terminating the WAL receiver", "pid", *pid)
	if err := syscall.Kill(*pid, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
		return false, err
	}
	return true, nil
}

// terminateProcesses signals with SIGTERM the processes in procDir whose
// command line matches the passed expression. The kill function is injected
// for testing.
func terminateProcesses(
	ctx context.Context,
	procDir string,
	cmdline *regexp.Regexp,
	kill func(pid int, sig syscall.Signal) error,
) (int, error) {
	contextLogger := log.FromContext(ctx)

	entries, err := os.ReadDir(procDir)
	if err != nil {
		return 0, err
	}

	signalled := 0
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid <= 0 {
			continue
		}

		content, err := os.ReadFile(filepath.Join(procDir, entry.Name(), "cmdline")) // #nosec G304
		if err != nil {
			// The process may be gone in the meantime
			continue
		}
		// PostgreSQL rewrites its process title, that may contain NUL
		// characters as separators
		title := bytes.ReplaceAll(content, []byte{0}, []byte{' '})
		if !cmdline.Match(title) {
			continue
		}

		if err := kill(pid, syscall.SIGTERM); err != nil {
			// A process that is already gone is what we wanted
			if !errors.Is(err, syscall.ESRCH) {
				contextLogger.Error(err, "Cannot terminate process", "pid", pid)
			}
			continue
		}
		signalled++
	}

	return signalled, nil
}
