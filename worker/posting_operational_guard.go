/*
 * SPDX-FileCopyrightText: © 2026 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package worker

import (
	"errors"
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

var ErrPostingStoreOperationalPath = errors.New("operation requires the Badger operational posting-store backend")

func requireBadgerPostingStore(operation string) (*badger.DB, error) {
	if pstore == nil {
		return nil, fmt.Errorf("%w: %s", ErrPostingStoreOperationalPath, operation)
	}
	return pstore, nil
}
