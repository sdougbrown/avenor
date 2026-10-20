package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/sdougbrown/avenor/client"
)

// startSubscribeStub serves one control connection and answers its first
// request the way the row asks: a shutdown refusal, or a dropped connection.
func startSubscribeStub(t *testing.T, refusal bool) string {
	t.Helper()
	socket := filepath.Join(t.TempDir(), "control.sock")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		line, err := bufio.NewReader(conn).ReadBytes('\n')
		if err != nil || !refusal {
			// A drop: the connection ends without answering the request.
			return
		}
		var req client.Request
		if err := json.Unmarshal(line, &req); err != nil {
			return
		}
		resp := client.Response{JSONRPC: "2.0", ID: req.ID, Error: &client.RespError{Code: -32014, Message: "control server stopped"}}
		data, err := json.Marshal(resp)
		if err != nil {
			return
		}
		_, _ = conn.Write(append(data, '\n'))
	}()
	return socket
}

// TestCmdTailSubscribeFailureRows pins which subscribe failures end a tail
// quietly: the shutdown refusal, which the server follows by closing the
// connection, and nothing else. A real transport drop is still reported.
func TestCmdTailSubscribeFailureRows(t *testing.T) {
	cases := []struct {
		name       string
		refusal    bool
		wantCode   int
		wantStderr string
	}{
		{"stopped refusal ends the stream", true, 0, ""},
		{"transport drop is reported", false, 1, "avenor control: subscribe:"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, err := client.Dial(startSubscribeStub(t, tc.refusal))
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			defer c.Close()

			oldStderr := os.Stderr
			r, w, err := os.Pipe()
			if err != nil {
				t.Fatalf("pipe: %v", err)
			}
			os.Stderr = w
			code := cmdTail(c)
			w.Close()
			os.Stderr = oldStderr

			var stderr bytes.Buffer
			_, _ = io.Copy(&stderr, r)

			if code != tc.wantCode {
				t.Fatalf("cmdTail() = %d, want %d (stderr %q)", code, tc.wantCode, stderr.String())
			}
			if tc.wantStderr == "" {
				if stderr.Len() != 0 {
					t.Fatalf("stderr = %q, want empty", stderr.String())
				}
				return
			}
			if !bytes.Contains(stderr.Bytes(), []byte(tc.wantStderr)) {
				t.Fatalf("stderr = %q, want it to contain %q", stderr.String(), tc.wantStderr)
			}
		})
	}
}
