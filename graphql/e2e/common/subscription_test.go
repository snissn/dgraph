/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package common

import (
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

func TestSubscriptionReceiveDeadline(t *testing.T) {
	for _, typ := range []string{dataMsg, "complete", "idle"} {
		t.Run(typ, func(t *testing.T) {
			release := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
				if err != nil {
					return
				}
				defer conn.Close()
				if typ != "idle" {
					_ = conn.WriteJSON(operationMessage{Type: typ, Payload: []byte(`{"data":{}}`)})
				}
				<-release
			}))
			defer server.Close()
			defer close(release)
			conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
			require.NoError(t, err)
			defer conn.Close()
			client := &GraphQLSubscriptionClient{conn: conn}
			timeout := 10 * time.Second
			if typ == "idle" {
				timeout = 50 * time.Millisecond
			}
			payload, err := client.RecvMsgWithTimeout(timeout)
			if typ == "idle" {
				var timeoutErr net.Error
				require.True(t, errors.As(err, &timeoutErr))
				require.True(t, timeoutErr.Timeout())
				return
			}
			require.NoError(t, err)
			if typ == "complete" {
				require.Nil(t, payload)
			} else {
				require.JSONEq(t, `{"data":{}}`, string(payload))
			}
		})
	}
}
