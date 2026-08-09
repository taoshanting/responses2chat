// Command manifest creates the signed release manifest from verified binary
// checksum files. It is intentionally dependency-free so CI can run it before
// exposing the update signing key to the signing step.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const manifestVersion = 1

type releaseManifest struct {
	ManifestVersion int               `json:"manifest_version"`
	Version         string            `json:"version"`
	Commit          string            `json:"commit"`
	BuildTime       string            `json:"build_time"`
	Tag             string            `json:"tag"`
	Assets          map[string]string `json:"assets"`
}

func main() {
	output := flag.String("output", "", "output version.json path")
	version := flag.String("version", "", "release version")
	commit := flag.String("commit", "", "seven-character commit hash")
	buildTime := flag.String("build-time", "", "RFC3339 build time")
	tag := flag.String("tag", "", "release tag")
	flag.Parse()

	manifest, err := buildManifest(*version, *commit, *buildTime, *tag, flag.Args())
	if err != nil {
		fatalf("build manifest: %v", err)
	}
	if strings.TrimSpace(*output) == "" {
		fatalf("-output is required")
	}
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		fatalf("encode manifest: %v", err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(*output, data, 0o644); err != nil {
		fatalf("write %s: %v", *output, err)
	}
}

func buildManifest(version, commit, buildTime, tag string, checksumPaths []string) (releaseManifest, error) {
	manifest := releaseManifest{
		ManifestVersion: manifestVersion,
		Version:         strings.TrimSpace(version),
		Commit:          strings.ToLower(strings.TrimSpace(commit)),
		BuildTime:       strings.TrimSpace(buildTime),
		Tag:             strings.TrimSpace(tag),
		Assets:          make(map[string]string),
	}
	if len(manifest.Commit) != 7 || !isHex(manifest.Commit) {
		return releaseManifest{}, fmt.Errorf("commit must be exactly seven hexadecimal characters")
	}
	if _, err := time.Parse(time.RFC3339, manifest.BuildTime); err != nil {
		return releaseManifest{}, fmt.Errorf("build time must be RFC3339: %w", err)
	}
	if manifest.Tag == "dev" {
		_, versionCommit, ok := parseDevVersion(manifest.Version)
		if !ok || versionCommit != manifest.Commit {
			return releaseManifest{}, fmt.Errorf("dev version %q does not match commit %q", manifest.Version, manifest.Commit)
		}
	} else if !isStableVersion(manifest.Tag) || manifest.Version != manifest.Tag {
		return releaseManifest{}, fmt.Errorf("stable version %q and tag %q must be the same strict vX.Y.Z version", manifest.Version, manifest.Tag)
	}
	if len(checksumPaths) == 0 {
		return releaseManifest{}, fmt.Errorf("at least one .sha256 file is required")
	}
	for _, checksumPath := range checksumPaths {
		name, digest, err := readChecksum(checksumPath)
		if err != nil {
			return releaseManifest{}, err
		}
		if _, exists := manifest.Assets[name]; exists {
			return releaseManifest{}, fmt.Errorf("duplicate asset %q", name)
		}
		actual, err := fileSHA256(filepath.Join(filepath.Dir(checksumPath), name))
		if err != nil {
			return releaseManifest{}, fmt.Errorf("hash asset %q: %w", name, err)
		}
		if actual != digest {
			return releaseManifest{}, fmt.Errorf("asset %q checksum mismatch: file has %s, checksum says %s", name, actual, digest)
		}
		manifest.Assets[name] = digest
	}
	return manifest, nil
}

func readChecksum(path string) (string, string, error) {
	if !strings.HasSuffix(path, ".sha256") {
		return "", "", fmt.Errorf("checksum path %q must end in .sha256", path)
	}
	f, err := os.Open(path)
	if err != nil {
		return "", "", fmt.Errorf("read checksum %q: %w", path, err)
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, 1025))
	if err != nil {
		return "", "", fmt.Errorf("read checksum %q: %w", path, err)
	}
	if len(data) > 1024 {
		return "", "", fmt.Errorf("checksum %q exceeds 1024-byte limit", path)
	}
	parts := strings.Fields(strings.TrimSpace(string(data)))
	if len(parts) != 2 || len(parts[0]) != sha256.Size*2 || !isHex(parts[0]) {
		return "", "", fmt.Errorf("checksum %q must contain exactly a 64-character hex digest and filename", path)
	}
	name := parts[1]
	if name == "" || strings.ContainsAny(name, "/\\") {
		return "", "", fmt.Errorf("checksum %q has unsafe filename %q", path, name)
	}
	expectedName := strings.TrimSuffix(filepath.Base(path), ".sha256")
	if name != expectedName {
		return "", "", fmt.Errorf("checksum %q names %q, expected %q", path, name, expectedName)
	}
	return name, strings.ToLower(parts[0]), nil
}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func isStableVersion(value string) bool {
	if !strings.HasPrefix(value, "v") {
		return false
	}
	parts := strings.Split(strings.TrimPrefix(value, "v"), ".")
	if len(parts) != 3 {
		return false
	}
	for _, part := range parts {
		if part == "" || (len(part) > 1 && part[0] == '0') || !isDigits(part) {
			return false
		}
		if _, err := strconv.ParseUint(part, 10, 64); err != nil {
			return false
		}
	}
	return true
}

func parseDevVersion(value string) (uint64, string, bool) {
	parts := strings.Split(value, "-")
	if len(parts) != 4 || parts[0] != "dev" || len(parts[1]) < 4 || len(parts[2]) != 8 || len(parts[3]) != 7 {
		return 0, "", false
	}
	if !isDigits(parts[1]) || !isDigits(parts[2]) || !isHex(parts[3]) {
		return 0, "", false
	}
	run, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil || run == 0 {
		return 0, "", false
	}
	return run, strings.ToLower(parts[3]), true
}

func isDigits(value string) bool {
	if value == "" {
		return false
	}
	for i := range value {
		if value[i] < '0' || value[i] > '9' {
			return false
		}
	}
	return true
}

func isHex(value string) bool {
	if value == "" {
		return false
	}
	for i := range value {
		c := value[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
