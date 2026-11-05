package poo

import (
	"bytes"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"text/template"
	"time"

	"github.com/sdougbrown/avenor/internal/appdir"
	"github.com/sdougbrown/avenor/internal/askclaim"
	"github.com/sdougbrown/avenor/internal/durablefile"
	"github.com/sdougbrown/avenor/internal/feed"
	"github.com/sdougbrown/avenor/internal/messaging"
)

//go:embed manifests/*.tmpl
var manifestTemplates embed.FS

var avenorExecutable = os.Executable
var manifestMaxAge = 7 * 24 * time.Hour

type manifestPaths struct{ feeds, registry string }
type manifestContent struct{ feed, registry []byte }

func renderManifests(stateHome, brokerURL, runID string) (manifestContent, error) {
	executable, err := avenorExecutable()
	if err != nil {
		return manifestContent{}, fmt.Errorf("avenor executable: %w", err)
	}
	if !filepath.IsAbs(executable) || !filepath.IsAbs(stateHome) {
		return manifestContent{}, fmt.Errorf("avenor executable and state home must be absolute")
	}
	asks, err := askclaim.DefaultDir(stateHome, brokerURL, runID)
	if err != nil {
		return manifestContent{}, err
	}
	data := struct{ Executable, LockFlag, AsksFlag, Timeout string }{
		executable, "--lock-dir=" + feed.DefaultLockDir(stateHome), "--asks-dir=" + asks,
		(messaging.MaxWait + 30*time.Second).String(),
	}
	jsonString := func(s string) string { b, _ := json.Marshal(s); return string(b) }
	t, err := template.New("manifests").Funcs(template.FuncMap{"json": jsonString}).ParseFS(manifestTemplates, "manifests/*.tmpl")
	if err != nil {
		return manifestContent{}, err
	}
	var feed, registry bytes.Buffer
	if err := t.ExecuteTemplate(&feed, "feed.json.tmpl", data); err != nil {
		return manifestContent{}, err
	}
	if err := t.ExecuteTemplate(&registry, "registry.json.tmpl", data); err != nil {
		return manifestContent{}, err
	}
	return manifestContent{feed.Bytes(), registry.Bytes()}, nil
}

func publishManifests(stateHome string, content manifestContent) (manifestPaths, error) {
	root := filepath.Join(stateHome, "poo", "manifests")
	h := sha256.New()
	// Length framing makes the two manifest boundaries unambiguous.
	_, _ = fmt.Fprintf(h, "%d:", len(content.feed))
	_, _ = h.Write(content.feed)
	_, _ = h.Write(content.registry)
	dest := filepath.Join(root, fmt.Sprintf("%x", h.Sum(nil)))
	paths := manifestPaths{filepath.Join(dest, "feeds"), filepath.Join(dest, "cli-registry")}
	complete := func() bool {
		for _, f := range []struct {
			path string
			data []byte
		}{{filepath.Join(paths.feeds, "avenor.json"), content.feed}, {filepath.Join(paths.registry, "avenor.json"), content.registry}} {
			got, err := os.ReadFile(f.path)
			if err != nil || !bytes.Equal(got, f.data) {
				return false
			}
		}
		return true
	}
	if complete() {
		now := time.Now()
		if err := os.Chtimes(dest, now, now); err != nil {
			return manifestPaths{}, err
		}
		pruneManifests(root, dest)
		return paths, nil
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		return manifestPaths{}, err
	}
	tmp, err := os.MkdirTemp(root, ".publish-")
	if err != nil {
		return manifestPaths{}, err
	}
	defer os.RemoveAll(tmp)
	for _, f := range []struct {
		dir  string
		data []byte
	}{{"feeds", content.feed}, {"cli-registry", content.registry}} {
		dir := filepath.Join(tmp, f.dir)
		if err := os.Mkdir(dir, 0700); err != nil {
			return manifestPaths{}, err
		}
		if err := durablefile.WriteFileAtomic(filepath.Join(dir, "avenor.json"), ".manifest-", f.data, 0600); err != nil {
			return manifestPaths{}, err
		}
	}
	if err := os.Rename(tmp, dest); err != nil {
		if !complete() {
			return manifestPaths{}, fmt.Errorf("publish poo manifests: %w", err)
		}
		now := time.Now()
		if err := os.Chtimes(dest, now, now); err != nil {
			return manifestPaths{}, err
		}
	}
	pruneManifests(root, dest)
	return paths, nil
}

func pruneManifests(root, current string) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return
	}
	cutoff := time.Now().Add(-manifestMaxAge)
	for _, entry := range entries {
		name := entry.Name()
		if !entry.IsDir() || len(name) != 64 || filepath.Join(root, name) == current {
			continue
		}
		if _, err := hex.DecodeString(name); err != nil {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		if info.ModTime().Before(cutoff) {
			_ = os.RemoveAll(filepath.Join(root, name))
		}
	}
}

func messagingManifests(brokerURL, runID string) (manifestPaths, error) {
	stateHome := appdir.StateHome()
	content, err := renderManifests(stateHome, brokerURL, runID)
	if err != nil {
		return manifestPaths{}, fmt.Errorf("render poo manifests: %w", err)
	}
	return publishManifests(stateHome, content)
}
