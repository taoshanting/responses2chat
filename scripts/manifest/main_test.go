package main

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBuildManifest(t *testing.T) {
	dir := t.TempDir()
	name := "responses2chat-linux-amd64"
	binary := []byte("release binary")
	digest := fmt.Sprintf("%x", sha256.Sum256(binary))
	if err := os.WriteFile(filepath.Join(dir, name), binary, 0o755); err != nil {
		t.Fatal(err)
	}
	checksumPath := filepath.Join(dir, name+".sha256")
	if err := os.WriteFile(checksumPath, []byte(digest+"  "+name+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	manifest, err := buildManifest("dev-0042-20260425-bbbbbbb", "bbbbbbb", "2026-04-25T00:00:00Z", "dev", []string{checksumPath})
	if err != nil {
		t.Fatal(err)
	}
	if manifest.ManifestVersion != manifestVersion || manifest.Assets[name] != digest {
		t.Fatalf("unexpected manifest: %#v", manifest)
	}
}

func TestBuildManifestRejectsInvalidChecksum(t *testing.T) {
	for _, tc := range []struct {
		name     string
		contents string
	}{
		{"wrong filename", strings.Repeat("a", 64) + "  other-binary\n"},
		{"invalid digest", "not-a-digest  responses2chat-linux-amd64\n"},
		{"extra fields", strings.Repeat("a", 64) + "  responses2chat-linux-amd64 extra\n"},
		{"oversized file", strings.Repeat("a", 1025)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "responses2chat-linux-amd64.sha256")
			if err := os.WriteFile(path, []byte(tc.contents), 0o644); err != nil {
				t.Fatal(err)
			}
			if _, err := buildManifest("v1.2.3", "bbbbbbb", "2026-04-25T00:00:00Z", "v1.2.3", []string{path}); err == nil {
				t.Fatal("expected invalid checksum to be rejected")
			}
		})
	}
}

func TestBuildManifestRejectsInconsistentVersion(t *testing.T) {
	for _, tc := range []struct {
		version string
		commit  string
		tag     string
	}{
		{"v1.2.3-rc.1", "bbbbbbb", "v1.2.3-rc.1"},
		{"v1.2.3", "bbbbbbb", "v1.2.4"},
		{"dev-0042-20260425-aaaaaaa", "bbbbbbb", "dev"},
	} {
		if _, err := buildManifest(tc.version, tc.commit, "2026-04-25T00:00:00Z", tc.tag, []string{"unused.sha256"}); err == nil {
			t.Fatalf("expected version=%q tag=%q to be rejected", tc.version, tc.tag)
		}
	}
}
