package claudechannel

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// projectMCPStore owns read-modify-write access to project-level .mcp.json
// bootstrap entries used by the claude-channel transport.
type projectMCPStore struct {
	mu sync.Mutex
}

var projectMCP = &projectMCPStore{}

func upsertProjectMCPServer(path string, name string, server any) error {
	return projectMCP.Upsert(path, name, server)
}

func removeProjectMCPServer(path string, name string) error {
	return projectMCP.Remove(path, name)
}

func CleanupProjectMCP(path string) (int, error) {
	return projectMCP.Cleanup(path)
}

func (s *projectMCPStore) Upsert(path string, name string, server any) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	cfg, err := readProjectMCPConfig(path)
	if err != nil {
		return err
	}
	servers, _ := cfg["mcpServers"].(map[string]any)
	if servers == nil {
		servers = map[string]any{}
	}
	servers[name] = server
	cfg["mcpServers"] = servers
	return writeProjectMCPConfig(path, cfg)
}

func (s *projectMCPStore) Cleanup(path string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cfg, err := readProjectMCPConfig(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	servers, _ := cfg["mcpServers"].(map[string]any)
	if servers == nil {
		return 0, nil
	}
	removed := 0
	for name := range servers {
		if strings.HasPrefix(name, "avenor-channel-") {
			delete(servers, name)
			removed++
		}
	}
	if removed == 0 {
		return 0, nil
	}
	if len(servers) == 0 {
		delete(cfg, "mcpServers")
	} else {
		cfg["mcpServers"] = servers
	}
	if len(cfg) == 0 {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return 0, err
		}
		return removed, nil
	}
	return removed, writeProjectMCPConfig(path, cfg)
}

func (s *projectMCPStore) Remove(path string, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	cfg, err := readProjectMCPConfig(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	servers, _ := cfg["mcpServers"].(map[string]any)
	if servers == nil {
		return nil
	}
	delete(servers, name)
	if len(servers) == 0 {
		delete(cfg, "mcpServers")
	} else {
		cfg["mcpServers"] = servers
	}
	if len(cfg) == 0 {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	}
	return writeProjectMCPConfig(path, cfg)
}

func readProjectMCPConfig(path string) (map[string]any, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]any{}, nil
		}
		return nil, err
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return map[string]any{}, nil
	}
	var cfg map[string]any
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("decode %s: %w", path, err)
	}
	if cfg == nil {
		cfg = map[string]any{}
	}
	return cfg, nil
}

func writeProjectMCPConfig(path string, cfg map[string]any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".mcp.json.*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer func() {
		_ = os.Remove(tmpPath)
	}()
	if _, err := tmp.Write(append(data, '\n')); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}
