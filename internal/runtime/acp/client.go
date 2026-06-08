package acp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/sdougbrown/avenor/internal/events"
)

type ClientConfig struct {
	Bin                  string
	Args                 []string
	Dir                  string
	Env                  map[string]string
	AutoAnswerPermission bool
	AppendCWDArg         bool
}

type Client struct {
	cmd                  *exec.Cmd
	stdin                io.WriteCloser
	stdout               io.ReadCloser
	stderr               bytes.Buffer
	dir                  string
	autoAnswerPermission bool
	events               chan events.Event
	done                 chan struct{}
	readErr              error
	nextID               atomic.Uint64
	sendMu               sync.Mutex
	pendingMu            sync.Mutex
	pending              map[string]chan rpcEnvelope
	permissionMu         sync.Mutex
	permissions          map[string]rpcEnvelope
	pid                  int
}

func NewClient(ctx context.Context, cfg ClientConfig) (*Client, error) {
	dir := cfg.Dir
	if dir == "" {
		dir = "."
	}
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}

	args := make([]string, len(cfg.Args))
	copy(args, cfg.Args)
	if cfg.AppendCWDArg {
		args = append(args, "--cwd", absDir)
	}
	cmd := exec.CommandContext(ctx, cfg.Bin, args...)
	cmd.Env = mergeEnviron(os.Environ(), cfg.Env)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}

	client := &Client{
		cmd:                  cmd,
		stdin:                stdin,
		stdout:               stdout,
		dir:                  absDir,
		events:               make(chan events.Event, 128),
		done:                 make(chan struct{}),
		pending:              map[string]chan rpcEnvelope{},
		permissions:          map[string]rpcEnvelope{},
		autoAnswerPermission: cfg.AutoAnswerPermission,
	}
	cmd.Stderr = &client.stderr

	if err := cmd.Start(); err != nil {
		return nil, err
	}
	client.pid = cmd.Process.Pid
	go client.readLoop()

	return client, nil
}

func (c *Client) PID() int {
	return c.pid
}

func (c *Client) Events() <-chan events.Event {
	return c.events
}

func (c *Client) Close() error {
	_ = c.stdin.Close()
	if c.cmd.Process != nil {
		_ = c.cmd.Process.Signal(syscall.SIGTERM)
	}

	select {
	case <-c.done:
	case <-time.After(2 * time.Second):
		if c.cmd.Process != nil {
			_ = c.cmd.Process.Kill()
		}
		<-c.done
	}

	return c.readErr
}

func (c *Client) Initialize(ctx context.Context) (json.RawMessage, error) {
	return c.request(ctx, "initialize", map[string]any{
		"protocolVersion": 1,
		"clientCapabilities": map[string]any{
			"fs": map[string]any{
				"readTextFile":  false,
				"writeTextFile": false,
			},
			"terminal": false,
		},
		"clientInfo": map[string]any{
			"name":    "avenor",
			"version": "0.0.1",
		},
	})
}

func (c *Client) Authenticate(ctx context.Context, methodID string) error {
	_, err := c.request(ctx, "authenticate", map[string]any{
		"methodId": methodID,
	})
	return err
}

func (c *Client) request(ctx context.Context, method string, params any) (json.RawMessage, error) {
	id := strconv.FormatUint(c.nextID.Add(1), 10)
	response := make(chan rpcEnvelope, 1)

	c.pendingMu.Lock()
	c.pending[id] = response
	c.pendingMu.Unlock()
	defer func() {
		c.pendingMu.Lock()
		delete(c.pending, id)
		c.pendingMu.Unlock()
	}()

	if err := c.write(rpcEnvelope{
		JSONRPC: "2.0",
		ID:      json.RawMessage(id),
		Method:  method,
		Params:  mustRaw(params),
	}); err != nil {
		return nil, err
	}

	select {
	case msg := <-response:
		if msg.Error != nil {
			return nil, fmt.Errorf("json-rpc %s failed: %s (%d)", method, msg.Error.Message, msg.Error.Code)
		}
		return msg.Result, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.done:
		if c.readErr != nil {
			return nil, c.readErr
		}
		return nil, errors.New("acp client exited")
	}
}

func (c *Client) notify(method string, params any) error {
	return c.write(rpcEnvelope{
		JSONRPC: "2.0",
		Method:  method,
		Params:  mustRaw(params),
	})
}

func (c *Client) write(msg rpcEnvelope) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	data = append(data, '\n')

	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	_, err = c.stdin.Write(data)
	return err
}

func (c *Client) readLoop() {
	defer close(c.done)
	defer close(c.events)

	reader := bufio.NewReader(c.stdout)
	for {
		data, err := readFrame(reader)
		if err != nil {
			waitErr := c.cmd.Wait()
			if !errors.Is(err, io.EOF) {
				c.readErr = fmt.Errorf("read acp client: %w%s", err, c.stderrSuffix())
			} else if waitErr != nil {
				c.readErr = fmt.Errorf("acp client exited: %w%s", waitErr, c.stderrSuffix())
			}
			return
		}

		var msg rpcEnvelope
		if err := json.Unmarshal(data, &msg); err != nil {
			c.readErr = fmt.Errorf("decode acp frame: %w", err)
			_ = c.cmd.Process.Kill()
			_ = c.cmd.Wait()
			return
		}
		c.dispatch(msg)
	}
}

func (c *Client) dispatch(msg rpcEnvelope) {
	if len(msg.ID) > 0 && msg.Method == "" {
		id := string(msg.ID)
		c.pendingMu.Lock()
		ch := c.pending[id]
		c.pendingMu.Unlock()
		if ch != nil {
			ch <- msg
		}
		return
	}

	if len(msg.ID) > 0 && msg.Method != "" {
		go c.handleRequest(msg)
		return
	}

	if msg.Method != "session/update" {
		return
	}

	event, err := mapSessionUpdate(msg.Params)
	if err == nil {
		c.events <- event
	}
}

func (c *Client) handleRequest(msg rpcEnvelope) {
	switch msg.Method {
	case "session/request_permission":
		requestID := rpcIDString(msg.ID)
		if !c.autoAnswerPermission {
			c.permissionMu.Lock()
			c.permissions[requestID] = msg
			c.permissionMu.Unlock()
		}
		c.events <- mapPermissionRequestWithID(msg.Params, requestID)
		if c.autoAnswerPermission {
			_ = c.write(rpcEnvelope{
				JSONRPC: "2.0",
				ID:      msg.ID,
				Result:  mustRaw(defaultPermissionResponse(msg.Params)),
			})
		}
	default:
		_ = c.write(rpcEnvelope{
			JSONRPC: "2.0",
			ID:      msg.ID,
			Error: &rpcError{
				Code:    -32601,
				Message: "method not implemented by avenor",
			},
		})
	}
}

func (c *Client) answerPermission(requestID string, result any) error {
	c.permissionMu.Lock()
	msg, ok := c.permissions[requestID]
	if ok {
		delete(c.permissions, requestID)
	}
	c.permissionMu.Unlock()
	if !ok {
		return fmt.Errorf("permission request %q not found", requestID)
	}

	return c.write(rpcEnvelope{
		JSONRPC: "2.0",
		ID:      msg.ID,
		Result:  mustRaw(result),
	})
}

func (c *Client) stderrSuffix() string {
	if c.stderr.Len() == 0 {
		return ""
	}
	return ": " + c.stderr.String()
}

func readFrame(reader *bufio.Reader) ([]byte, error) {
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return nil, err
	}

	trimmed := bytes.TrimSpace(line)
	if bytes.HasPrefix(trimmed, []byte("Content-Length:")) || bytes.HasPrefix(trimmed, []byte("content-length:")) {
		length, err := parseContentLength(trimmed)
		if err != nil {
			return nil, err
		}
		for {
			header, err := reader.ReadBytes('\n')
			if err != nil {
				return nil, err
			}
			if len(bytes.TrimSpace(header)) == 0 {
				break
			}
		}
		body := make([]byte, length)
		_, err = io.ReadFull(reader, body)
		return body, err
	}

	if len(trimmed) == 0 {
		return readFrame(reader)
	}
	return trimmed, nil
}

// mergeEnviron returns a copy of base with overrides applied. Keys present in
// overrides replace existing entries in base in-place (preserving their
// original order); new keys are appended. This prevents duplicate env vars
// where the caller-provided value must win (e.g. OPENCODE_CONFIG).
func mergeEnviron(base []string, overrides map[string]string) []string {
	if len(overrides) == 0 {
		return base
	}
	out := make([]string, len(base))
	copy(out, base)
	replaced := make(map[string]bool, len(overrides))
	for i, entry := range out {
		if idx := strings.IndexByte(entry, '='); idx > 0 {
			if _, ok := overrides[entry[:idx]]; ok {
				out[i] = entry[:idx] + "=" + overrides[entry[:idx]]
				replaced[entry[:idx]] = true
			}
		}
	}
	for key, value := range overrides {
		if !replaced[key] {
			out = append(out, key+"="+value)
		}
	}
	return out
}

func parseContentLength(header []byte) (int, error) {
	_, value, ok := bytes.Cut(header, []byte(":"))
	if !ok {
		return 0, errors.New("malformed Content-Length header")
	}
	length, err := strconv.Atoi(strings.TrimSpace(string(value)))
	if err != nil || length < 0 {
		return 0, fmt.Errorf("invalid Content-Length %q", value)
	}
	return length, nil
}

func mustRaw(v any) json.RawMessage {
	if v == nil {
		return nil
	}
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}

func rpcIDString(id json.RawMessage) string {
	var value any
	if err := json.Unmarshal(id, &value); err != nil {
		return string(id)
	}
	switch v := value.(type) {
	case string:
		return v
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	default:
		return fmt.Sprint(v)
	}
}

func defaultPermissionResponse(params json.RawMessage) map[string]any {
	var payload struct {
		Options []struct {
			OptionID string `json:"optionId"`
			Kind     string `json:"kind"`
		} `json:"options"`
	}
	_ = json.Unmarshal(params, &payload)

	selected := ""
	for _, opt := range payload.Options {
		if strings.HasPrefix(opt.Kind, "reject") {
			selected = opt.OptionID
			break
		}
	}
	if selected == "" && len(payload.Options) > 0 {
		selected = payload.Options[0].OptionID
	}

	return map[string]any{
		"outcome": map[string]any{
			"outcome":  "selected",
			"optionId": selected,
		},
	}
}
