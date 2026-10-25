//go:build linux

/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package raftwal

import (
	"os"
	"runtime"

	"golang.org/x/sys/unix"
)

// Only already fully initialized, published-capacity generations use data sync.
// Initial size/allocation and namespace publication retain full persistence.
func syncReadyData(f *os.File) error {
	for {
		err := unix.Fdatasync(int(f.Fd()))
		if err != unix.EINTR {
			// Fd exposes a raw descriptor: keep the exact file alive through the
			// final syscall on success and error, independently of finalizers.
			runtime.KeepAlive(f)
			return err
		}
	}
}
