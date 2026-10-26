//go:build !linux

/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package raftwal

import "os"

func syncReadyData(f *os.File) error { return f.Sync() }
