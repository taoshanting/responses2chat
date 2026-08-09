package updater

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lieyan/responses2chat/internal/version"
)

func TestCheckOnlySelectsNewestStableRelease(t *testing.T) {
	originalVersion := version.Version
	defer func() { version.Version = originalVersion }()
	version.Version = "v1.0.0"
	targetName := testUpdater(Config{}).targetName()
	sign := setTestSigningKey(t)
	metadata, metadataSig := makeTestManifest(t, sign, "v1.4.0", "bbbbbbb", "v1.4.0", targetName, strings.Repeat("a", 64))

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/releases/owner/repo/latest":
			_ = json.NewEncoder(w).Encode(releaseInfo{
				TagName: "v1.4.0",
				Assets:  testMetadataAssets("v1.4.0"),
			})
		case "/download/owner/repo/v1.4.0/version.json":
			_, _ = w.Write(metadata)
		case "/download/owner/repo/v1.4.0/version.json.sig":
			_, _ = w.Write(metadataSig)
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer server.Close()

	u := testUpdater(Config{
		Channel:      "stable",
		Source:       "proxy",
		ProxyBaseURL: server.URL,
		Repo:         "owner/repo",
	})

	result, err := u.CheckOnly(context.Background())
	if err != nil {
		t.Fatalf("CheckOnly returned error: %v", err)
	}
	if !result.HasUpdate {
		t.Fatalf("expected update to be available")
	}
	if result.LatestVersion != "v1.4.0" {
		t.Fatalf("expected latest version v1.4.0, got %q", result.LatestVersion)
	}
}

func TestCheckOnlySelectsNewestPrerelease(t *testing.T) {
	originalVersion := version.Version
	originalCommit := version.Commit
	defer func() { version.Version = originalVersion }()
	defer func() { version.Commit = originalCommit }()
	version.Version = "dev-0007-20260401-aaaaaaa"
	version.Commit = "aaaaaaa"
	remoteVersion := "dev-0042-20260425-bbbbbbb"
	targetName := testUpdater(Config{}).targetName()
	sign := setTestSigningKey(t)
	metadata, metadataSig := makeTestManifest(t, sign, remoteVersion, "bbbbbbb", "dev", targetName, strings.Repeat("a", 64))

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/releases/owner/repo/dev":
			_ = json.NewEncoder(w).Encode(releaseInfo{
				TagName:         "dev",
				TargetCommitish: "bbbbbbb",
				Prerelease:      true,
				Assets:          testMetadataAssets("dev"),
			})
		case "/download/owner/repo/dev/version.json":
			_, _ = w.Write(metadata)
		case "/download/owner/repo/dev/version.json.sig":
			_, _ = w.Write(metadataSig)
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer server.Close()

	u := testUpdater(Config{
		Channel:      "dev",
		Source:       "proxy",
		ProxyBaseURL: server.URL,
		Repo:         "owner/repo",
	})

	result, err := u.CheckOnly(context.Background())
	if err != nil {
		t.Fatalf("CheckOnly returned error: %v", err)
	}
	if !result.HasUpdate {
		t.Fatalf("expected prerelease update to be available")
	}
	if result.LatestVersion != remoteVersion {
		t.Fatalf("expected latest prerelease, got %q", result.LatestVersion)
	}
}

func TestCheckOnlySkipsDevReleaseForSameCommit(t *testing.T) {
	originalVersion := version.Version
	originalCommit := version.Commit
	defer func() { version.Version = originalVersion }()
	defer func() { version.Commit = originalCommit }()
	version.Version = "dev-0042-20260425-bbbbbbb"
	version.Commit = "bbbbbbb"
	targetName := testUpdater(Config{}).targetName()
	sign := setTestSigningKey(t)
	metadata, metadataSig := makeTestManifest(t, sign, version.Version, version.Commit, "dev", targetName, strings.Repeat("a", 64))

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/releases/owner/repo/dev":
			_ = json.NewEncoder(w).Encode(releaseInfo{
				TagName:         "dev",
				TargetCommitish: "bbbbbbb",
				Prerelease:      true,
				Assets:          testMetadataAssets("dev"),
			})
		case "/download/owner/repo/dev/version.json":
			_, _ = w.Write(metadata)
		case "/download/owner/repo/dev/version.json.sig":
			_, _ = w.Write(metadataSig)
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer server.Close()

	u := testUpdater(Config{
		Channel:      "dev",
		Source:       "proxy",
		ProxyBaseURL: server.URL,
		Repo:         "owner/repo",
	})

	result, err := u.CheckOnly(context.Background())
	if err != nil {
		t.Fatalf("CheckOnly returned error: %v", err)
	}
	if result.HasUpdate {
		t.Fatalf("did not expect update for same commit: %#v", result)
	}
	if result.LatestVersion != "dev-0042-20260425-bbbbbbb" {
		t.Fatalf("expected latest version from version metadata, got %q", result.LatestVersion)
	}
}

func TestPerformUpdateDownloadsAndVerifiesPrerelease(t *testing.T) {
	originalVersion := version.Version
	originalCommit := version.Commit
	defer func() { version.Version = originalVersion }()
	defer func() { version.Commit = originalCommit }()
	version.Version = "dev-0007-20260401-aaaaaaa"
	version.Commit = "aaaaaaa"

	cfg := Config{
		Channel: "dev",
		Source:  "proxy",
		Repo:    "owner/repo",
	}
	dataDir := t.TempDir()
	u := New(
		func() Config { return cfg },
		func() string { return dataDir },
		log.New(io.Discard, "", 0),
		RestartHooks{},
	)

	tag := "dev"
	remoteVersion := "dev-0042-20260425-bbbbbbb"
	targetName := u.targetName()
	binary := []byte("new binary")
	sum := fmt.Sprintf("%x", sha256.Sum256(binary))
	shaContent := sum + "  " + targetName + "\n"
	sign := setTestSigningKey(t)
	metadata, metadataSig := makeTestManifest(t, sign, remoteVersion, "bbbbbbb", tag, targetName, sum)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/releases/owner/repo/dev":
			_ = json.NewEncoder(w).Encode(releaseInfo{
				TagName:         tag,
				TargetCommitish: "bbbbbbb",
				Prerelease:      true,
				Assets: []assetInfo{
					{
						Name:               targetName,
						BrowserDownloadURL: "https://github.com/owner/repo/releases/download/" + tag + "/" + targetName,
						Size:               int64(len(binary)),
					},
					{
						Name:               targetName + ".sha256",
						BrowserDownloadURL: "https://github.com/owner/repo/releases/download/" + tag + "/" + targetName + ".sha256",
					},
					{
						Name:               targetName + ".sha256.sig",
						BrowserDownloadURL: "https://github.com/owner/repo/releases/download/" + tag + "/" + targetName + ".sha256.sig",
					},
					{
						Name:               "version.json",
						BrowserDownloadURL: "https://github.com/owner/repo/releases/download/" + tag + "/version.json",
					},
					{
						Name:               "version.json.sig",
						BrowserDownloadURL: "https://github.com/owner/repo/releases/download/" + tag + "/version.json.sig",
					},
				},
			})
		case "/download/owner/repo/" + tag + "/" + targetName:
			_, _ = w.Write(binary)
		case "/download/owner/repo/" + tag + "/" + targetName + ".sha256":
			_, _ = w.Write([]byte(shaContent))
		case "/download/owner/repo/" + tag + "/" + targetName + ".sha256.sig":
			_, _ = w.Write([]byte(sign([]byte(shaContent))))
		case "/download/owner/repo/" + tag + "/version.json":
			_, _ = w.Write(metadata)
		case "/download/owner/repo/" + tag + "/version.json.sig":
			_, _ = w.Write(metadataSig)
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer server.Close()
	cfg.ProxyBaseURL = server.URL

	u.performUpdate(context.Background())

	status := u.Status()
	if status.State != "ready" {
		t.Fatalf("expected update to be ready, got %q: %s", status.State, status.Error)
	}
	if status.LatestVersion != remoteVersion || u.pendingTag != tag {
		t.Fatalf("expected pending latest tag %q, got status=%q pending=%q", tag, status.LatestVersion, u.pendingTag)
	}
	if status.Progress != progressVerifyDone {
		t.Fatalf("expected overall progress %d, got %.0f", progressVerifyDone, status.Progress)
	}
	got, err := os.ReadFile(u.pendingBinaryPath)
	if err != nil {
		t.Fatalf("read pending binary: %v", err)
	}
	if string(got) != string(binary) {
		t.Fatalf("pending binary content mismatch")
	}
}

func TestApplyPendingMovesToApplyingBeforeAsyncRestart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	u := testUpdater(Config{})
	u.mu.Lock()
	u.bgCtx = ctx
	u.mu.Unlock()
	u.hooks.BeforeExec = func(tag string) error {
		return context.Canceled
	}
	u.status.State = "ready"
	u.pendingBinaryPath = filepath.Join(t.TempDir(), "responses2chat-new")
	if err := os.WriteFile(u.pendingBinaryPath, []byte("pending"), 0o600); err != nil {
		t.Fatal(err)
	}
	pendingPath := u.pendingBinaryPath
	u.pendingTag = "v1.2.0"

	if err := u.ApplyPending(context.Background()); err != nil {
		t.Fatalf("ApplyPending returned error: %v", err)
	}

	status := u.Status()
	if status.State != "applying" {
		t.Fatalf("expected state applying immediately, got %q", status.State)
	}
	if status.Progress != progressApplying {
		t.Fatalf("expected applying progress %d, got %.0f", progressApplying, status.Progress)
	}
	if u.pendingBinaryPath != "" || u.pendingTag != "" {
		t.Fatalf("expected pending update to be consumed")
	}

	err := u.ApplyPending(context.Background())
	if err == nil || !strings.Contains(err.Error(), "no pending update") {
		t.Fatalf("expected duplicate apply to be rejected, got %v", err)
	}

	time.Sleep(250 * time.Millisecond)
	if _, err := os.Stat(pendingPath); !os.IsNotExist(err) {
		t.Fatalf("failed apply left pending binary behind: %v", err)
	}
}

func TestPerformUpdateRejectsReleaseWithoutSHA256(t *testing.T) {
	originalVersion := version.Version
	defer func() { version.Version = originalVersion }()
	version.Version = "v1.0.0"

	cfg := Config{
		Channel: "stable",
		Source:  "proxy",
		Repo:    "owner/repo",
	}
	dataDir := t.TempDir()
	u := New(
		func() Config { return cfg },
		func() string { return dataDir },
		log.New(io.Discard, "", 0),
		RestartHooks{},
	)

	targetName := u.targetName()
	binary := []byte("new binary")
	sum := fmt.Sprintf("%x", sha256.Sum256(binary))
	sign := setTestSigningKey(t)
	metadata, metadataSig := makeTestManifest(t, sign, "v1.4.0", "bbbbbbb", "v1.4.0", targetName, sum)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/releases/owner/repo/latest":
			assets := append([]assetInfo{{
				Name:               targetName,
				BrowserDownloadURL: "https://github.com/owner/repo/releases/download/v1.4.0/" + targetName,
				Size:               int64(len(binary)),
			}}, testMetadataAssets("v1.4.0")...)
			_ = json.NewEncoder(w).Encode(releaseInfo{
				TagName: "v1.4.0",
				Assets:  assets,
			})
		case "/download/owner/repo/v1.4.0/version.json":
			_, _ = w.Write(metadata)
		case "/download/owner/repo/v1.4.0/version.json.sig":
			_, _ = w.Write(metadataSig)
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer server.Close()
	cfg.ProxyBaseURL = server.URL

	u.performUpdate(context.Background())

	status := u.Status()
	if status.State != "failed" {
		t.Fatalf("expected update to fail without sha256 asset, got state %q", status.State)
	}
	if !strings.Contains(status.Error, "sha256") {
		t.Fatalf("expected sha256 error, got %q", status.Error)
	}
}

func TestCheckOnlyGitHubDirectDevChannel(t *testing.T) {
	originalVersion := version.Version
	originalCommit := version.Commit
	defer func() { version.Version = originalVersion }()
	defer func() { version.Commit = originalCommit }()
	version.Version = "dev-0007-20260401-aaaaaaa"
	version.Commit = "aaaaaaa"
	remoteVersion := "dev-0042-20260425-bbbbbbb"

	sign := setTestSigningKey(t)
	metadata, err := json.Marshal(releaseVersionInfo{
		ManifestVersion: manifestVersion,
		Version:         remoteVersion,
		Commit:          "bbbbbbb",
		BuildTime:       "2026-04-25T00:00:00Z",
		Tag:             "dev",
		Assets:          map[string]string{testUpdater(Config{}).targetName(): strings.Repeat("a", 64)},
	})
	if err != nil {
		t.Fatal(err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/owner/repo/releases/download/dev/version.json":
			_, _ = w.Write(metadata)
		case "/owner/repo/releases/download/dev/version.json.sig":
			_, _ = w.Write([]byte(sign(metadata)))
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer server.Close()
	setTestGitHubBaseURL(t, server.URL)

	u := testUpdater(Config{Channel: "dev", Repo: "owner/repo"})
	result, err := u.CheckOnly(context.Background())
	if err != nil {
		t.Fatalf("CheckOnly returned error: %v", err)
	}
	if !result.HasUpdate {
		t.Fatalf("expected update to be available")
	}
	if result.LatestVersion != remoteVersion {
		t.Fatalf("expected latest version %q, got %q", remoteVersion, result.LatestVersion)
	}
}

func TestCheckOnlyGitHubDirectStableChannel(t *testing.T) {
	originalVersion := version.Version
	defer func() { version.Version = originalVersion }()
	version.Version = "v1.0.0"

	sign := setTestSigningKey(t)
	metadata, err := json.Marshal(releaseVersionInfo{
		ManifestVersion: manifestVersion,
		Version:         "v1.4.0",
		Commit:          "bbbbbbb",
		BuildTime:       "2026-04-25T00:00:00Z",
		Tag:             "v1.4.0",
		Assets:          map[string]string{testUpdater(Config{}).targetName(): strings.Repeat("a", 64)},
	})
	if err != nil {
		t.Fatal(err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/owner/repo/releases/latest/download/version.json":
			_, _ = w.Write(metadata)
		case "/owner/repo/releases/download/v1.4.0/version.json.sig":
			_, _ = w.Write([]byte(sign(metadata)))
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer server.Close()
	setTestGitHubBaseURL(t, server.URL)

	u := testUpdater(Config{Channel: "stable", Repo: "owner/repo"})
	result, err := u.CheckOnly(context.Background())
	if err != nil {
		t.Fatalf("CheckOnly returned error: %v", err)
	}
	if !result.HasUpdate {
		t.Fatalf("expected update to be available")
	}
	if result.LatestVersion != "v1.4.0" {
		t.Fatalf("expected latest version v1.4.0, got %q", result.LatestVersion)
	}
}

func TestCheckOnlyGitHubDirectRejectsBadSignature(t *testing.T) {
	originalVersion := version.Version
	defer func() { version.Version = originalVersion }()
	version.Version = "v1.0.0"

	setTestSigningKey(t)
	metadata, err := json.Marshal(releaseVersionInfo{Version: "v1.4.0", Tag: "v1.4.0"})
	if err != nil {
		t.Fatal(err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/owner/repo/releases/latest/download/version.json":
			_, _ = w.Write(metadata)
		case "/owner/repo/releases/download/v1.4.0/version.json.sig":
			_, _ = w.Write([]byte("bm90IGEgcmVhbCBzaWduYXR1cmU=")) // valid base64, wrong signature
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer server.Close()
	setTestGitHubBaseURL(t, server.URL)

	u := testUpdater(Config{Channel: "stable", Repo: "owner/repo"})
	if _, err := u.CheckOnly(context.Background()); err == nil || !strings.Contains(err.Error(), "signature") {
		t.Fatalf("expected signature verification error, got %v", err)
	}
}

func TestWaitForIdleStopsWhenApplicationContextIsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	u := New(
		func() Config { return Config{} },
		func() string { return "" },
		log.New(io.Discard, "", 0),
		RestartHooks{IsBusy: func() bool { return true }},
	)

	if err := u.waitForIdle(ctx); err != context.Canceled {
		t.Fatalf("waitForIdle error = %v, want context.Canceled", err)
	}
}

func TestInstallBinaryUnixReplacesStaleBackup(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix replacement semantics")
	}
	dir := t.TempDir()
	execPath := filepath.Join(dir, "responses2chat")
	backupPath := execPath + ".bak"
	newBinaryPath := filepath.Join(dir, "responses2chat-new")
	if err := os.WriteFile(execPath, []byte("current"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(backupPath, []byte("stale"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(newBinaryPath, []byte("replacement"), 0o600); err != nil {
		t.Fatal(err)
	}

	gotBackupPath, err := installBinaryUnix(newBinaryPath, execPath)
	if err != nil {
		t.Fatalf("installBinaryUnix returned error: %v", err)
	}
	if gotBackupPath != backupPath {
		t.Fatalf("backup path = %q, want %q", gotBackupPath, backupPath)
	}
	assertFileContent(t, execPath, "replacement")
	assertFileContent(t, backupPath, "current")
	info, err := os.Stat(execPath)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o755 {
		t.Fatalf("installed mode = %o, want 755", got)
	}
}

func TestInstallBinaryUnixRollsBackAfterCopyFailure(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix replacement semantics")
	}
	dir := t.TempDir()
	execPath := filepath.Join(dir, "responses2chat")
	if err := os.WriteFile(execPath, []byte("current"), 0o755); err != nil {
		t.Fatal(err)
	}

	_, err := installBinaryUnix(filepath.Join(dir, "missing-new-binary"), execPath)
	if err == nil || !strings.Contains(err.Error(), "install new binary") {
		t.Fatalf("expected install error, got %v", err)
	}
	assertFileContent(t, execPath, "current")
	if _, err := os.Stat(execPath + ".bak"); !os.IsNotExist(err) {
		t.Fatalf("rollback left backup behind: %v", err)
	}
}

func TestRollbackBinaryReportsRestoreFailure(t *testing.T) {
	dir := t.TempDir()
	cause := fmt.Errorf("install failed")
	err := rollbackBinary(filepath.Join(dir, "responses2chat"), filepath.Join(dir, "missing.bak"), cause)
	if err == nil || !strings.Contains(err.Error(), cause.Error()) || !strings.Contains(err.Error(), "rollback failed") {
		t.Fatalf("expected combined rollback error, got %v", err)
	}
}

func assertFileContent(t *testing.T, path, want string) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != want {
		t.Fatalf("content of %s = %q, want %q", path, got, want)
	}
}

func TestDownloadRejectsSignedAssetMixAndMatch(t *testing.T) {
	sign := setTestSigningKey(t)
	for _, tc := range []struct {
		name           string
		checksumName   func(string) string
		manifestDigest string
		wantError      string
	}{
		{
			name:           "checksum names another platform",
			checksumName:   func(target string) string { return target + "-other-platform" },
			manifestDigest: "actual",
			wantError:      "expected",
		},
		{
			name:           "old signed binary disagrees with manifest",
			checksumName:   func(target string) string { return target },
			manifestDigest: strings.Repeat("0", 64),
			wantError:      "signed manifest",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dataDir := t.TempDir()
			u := New(
				func() Config { return Config{Channel: "dev", Source: SourceGitHub, Repo: "owner/repo"} },
				func() string { return dataDir },
				log.New(io.Discard, "", 0),
				RestartHooks{},
			)
			targetName := u.targetName()
			binary := []byte("previous signed binary")
			digest := fmt.Sprintf("%x", sha256.Sum256(binary))
			manifestDigest := tc.manifestDigest
			if manifestDigest == "actual" {
				manifestDigest = digest
			}
			checksum := digest + "  " + tc.checksumName(targetName) + "\n"

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/binary":
					_, _ = w.Write(binary)
				case "/checksum":
					_, _ = w.Write([]byte(checksum))
				case "/signature":
					_, _ = w.Write([]byte(sign([]byte(checksum))))
				default:
					t.Fatalf("unexpected path: %s", r.URL.Path)
				}
			}))
			defer server.Close()

			release := &releaseInfo{
				TagName:        "dev",
				ManifestSHA256: manifestDigest,
				Assets: []assetInfo{
					{Name: targetName, BrowserDownloadURL: server.URL + "/binary", Size: int64(len(binary))},
					{Name: targetName + ".sha256", BrowserDownloadURL: server.URL + "/checksum"},
					{Name: targetName + ".sha256.sig", BrowserDownloadURL: server.URL + "/signature"},
				},
			}
			if _, err := u.download(context.Background(), normalizeConfig(u.cfg()), release); err == nil || !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("download error = %v, want %q", err, tc.wantError)
			}
			tmpPath := filepath.Join(dataDir, "updates", "responses2chat-dev.tmp")
			if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
				t.Fatalf("temporary download was not removed: %v", err)
			}
		})
	}
}

func TestProxyRejectsReplayedSignedManifestUnderNewTag(t *testing.T) {
	originalVersion := version.Version
	defer func() { version.Version = originalVersion }()
	version.Version = "v2.0.0"
	targetName := testUpdater(Config{}).targetName()
	sign := setTestSigningKey(t)
	metadata, metadataSig := makeTestManifest(t, sign, "v1.0.0", "aaaaaaa", "v1.0.0", targetName, strings.Repeat("a", 64))
	assets := []assetInfo{
		{Name: "version.json", BrowserDownloadURL: "https://github.com/owner/repo/releases/download/v999.0.0/version.json"},
		{Name: "version.json.sig", BrowserDownloadURL: "https://github.com/owner/repo/releases/download/v999.0.0/version.json.sig"},
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/releases/owner/repo/latest":
			_ = json.NewEncoder(w).Encode(releaseInfo{TagName: "v999.0.0", Assets: assets})
		case "/download/owner/repo/v999.0.0/version.json":
			_, _ = w.Write(metadata)
		case "/download/owner/repo/v999.0.0/version.json.sig":
			_, _ = w.Write(metadataSig)
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer server.Close()

	u := testUpdater(Config{Channel: "stable", Source: SourceProxy, ProxyBaseURL: server.URL, Repo: "owner/repo"})
	if _, err := u.CheckOnly(context.Background()); err == nil || !strings.Contains(err.Error(), "does not match discovered tag") {
		t.Fatalf("CheckOnly error = %v, want signed tag mismatch", err)
	}
}

func TestResolveDownloadURLRejectsUntrustedAssets(t *testing.T) {
	u := testUpdater(Config{})
	cfg := normalizeConfig(Config{Source: SourceProxy, ProxyBaseURL: "https://mirror.example/base", Repo: "owner/repo"})
	for _, assetURL := range []string{
		"http://127.0.0.1/admin",
		"https://example.com/owner/repo/releases/download/dev/version.json",
		"https://github.com/other/repo/releases/download/dev/version.json",
		"https://github.com/owner/repo/releases/download/other/version.json",
	} {
		asset := &assetInfo{Name: "version.json", BrowserDownloadURL: assetURL}
		if _, err := u.resolveDownloadURL(cfg, "dev", asset); err == nil {
			t.Fatalf("expected URL %q to be rejected", assetURL)
		}
	}
	asset := &assetInfo{Name: "version.json", BrowserDownloadURL: "https://github.com/owner/repo/releases/download/dev/version.json"}
	got, err := u.resolveDownloadURL(cfg, "dev", asset)
	if err != nil {
		t.Fatal(err)
	}
	if got != "https://mirror.example/base/download/owner/repo/dev/version.json" {
		t.Fatalf("resolved URL = %q", got)
	}
}

func TestProxyAssetRedirectCannotBypassURLValidation(t *testing.T) {
	var targetHits atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetHits.Add(1)
		_, _ = io.WriteString(w, "sensitive local response")
	}))
	defer target.Close()
	mirror := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/admin", http.StatusFound)
	}))
	defer mirror.Close()

	cfg := normalizeConfig(Config{Source: SourceProxy, ProxyBaseURL: mirror.URL, Repo: "owner/repo"})
	asset := &assetInfo{
		Name:               "version.json",
		BrowserDownloadURL: "https://github.com/owner/repo/releases/download/dev/version.json",
	}
	u := testUpdater(cfg)
	if _, err := u.fetchAsset(context.Background(), cfg, "dev", asset, maxManifestSize); err == nil || !strings.Contains(err.Error(), "status 302") {
		t.Fatalf("fetchAsset error = %v, want redirect rejection", err)
	}
	if hits := targetHits.Load(); hits != 0 {
		t.Fatalf("redirect target received %d requests", hits)
	}
}

func TestGitHubRedirectPolicy(t *testing.T) {
	for _, tc := range []struct {
		url  string
		want bool
	}{
		{"https://github.com/owner/repo/releases/download/dev/file", true},
		{"https://release-assets.githubusercontent.com/file?token=x", true},
		{"https://objects.githubusercontent.com/file", true},
		{"http://github.com/file", false},
		{"https://github.com.evil.example/file", false},
		{"https://127.0.0.1/admin", false},
		{"https://github.com:444/file", false},
	} {
		req, err := http.NewRequest(http.MethodGet, tc.url, nil)
		if err != nil {
			t.Fatal(err)
		}
		if got := isTrustedGitHubRedirect(req.URL); got != tc.want {
			t.Fatalf("isTrustedGitHubRedirect(%q) = %v, want %v", tc.url, got, tc.want)
		}
	}
}

func TestProxyReleaseBodyLimit(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, strings.Repeat("x", maxReleaseSize+1))
	}))
	defer server.Close()
	u := testUpdater(Config{Source: SourceProxy, ProxyBaseURL: server.URL, Repo: "owner/repo"})
	if _, err := u.CheckOnly(context.Background()); err == nil || !strings.Contains(err.Error(), "limit") {
		t.Fatalf("CheckOnly error = %v, want body limit", err)
	}
}

func TestDownloadFileEnforcesKnownAndChunkedLimits(t *testing.T) {
	for _, tc := range []struct {
		name    string
		handler http.HandlerFunc
	}{
		{
			name: "known content length",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Length", "5")
				_, _ = io.WriteString(w, "12345")
			},
		},
		{
			name: "chunked body",
			handler: func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.WriteString(w, "123")
				w.(http.Flusher).Flush()
				_, _ = io.WriteString(w, "45")
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(tc.handler)
			defer server.Close()
			dest := filepath.Join(t.TempDir(), "update.tmp")
			u := testUpdater(Config{})
			if err := u.downloadFileWithLimit(context.Background(), server.URL, dest, 0, 4); err == nil || !strings.Contains(err.Error(), "limit") {
				t.Fatalf("download error = %v, want limit", err)
			}
			if _, err := os.Stat(dest); !os.IsNotExist(err) {
				t.Fatalf("partial download was not removed: %v", err)
			}
		})
	}
}

func TestDownloadFileRefusesExistingOrLinkedDestination(t *testing.T) {
	target := filepath.Join(t.TempDir(), "target")
	if err := os.WriteFile(target, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	linked := target + ".tmp"
	if err := os.Symlink(target, linked); err != nil {
		if runtime.GOOS == "windows" {
			t.Skipf("symlink unavailable: %v", err)
		}
		t.Fatal(err)
	}
	u := testUpdater(Config{})
	if err := u.downloadFileWithLimit(context.Background(), "http://127.0.0.1/unused", linked, 0, 4); err == nil {
		t.Fatal("expected an existing symlink destination to be rejected")
	}
	contents, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if string(contents) != "keep" {
		t.Fatalf("symlink target was modified: %q", contents)
	}
}

func TestDevVersionComparisonIsMonotonic(t *testing.T) {
	originalVersion := version.Version
	defer func() { version.Version = originalVersion }()
	version.Version = "dev-0042-20260425-bbbbbbb"
	u := testUpdater(Config{})
	for _, tc := range []struct {
		remote string
		want   bool
	}{
		{"dev-0041-20260424-aaaaaaa", false},
		{"dev-0042-20260425-bbbbbbb", false},
		{"dev-0042-20260425-ccccccc", false},
		{"dev-0043-20260426-ccccccc", true},
	} {
		if got := u.isNewer(releaseInfo{TagName: "dev", Version: tc.remote}, "dev"); got != tc.want {
			t.Fatalf("isNewer(%q) = %v, want %v", tc.remote, got, tc.want)
		}
	}
}

func TestVersionComparisonAllowsExplicitChannelSwitch(t *testing.T) {
	originalVersion := version.Version
	defer func() { version.Version = originalVersion }()
	u := testUpdater(Config{})

	version.Version = "v1.2.3"
	if !u.isNewer(releaseInfo{TagName: "dev", Version: "dev-0042-20260425-bbbbbbb"}, "dev") {
		t.Fatal("stable build could not switch to the dev channel")
	}

	version.Version = "dev-0042-20260425-bbbbbbb"
	if !u.isNewer(releaseInfo{TagName: "v1.2.3", Version: "v1.2.3"}, "stable") {
		t.Fatal("dev build could not switch to the stable channel")
	}
}

func TestStableVersionComparisonIsStrict(t *testing.T) {
	for _, invalid := range []string{"1.2.3", "v1.2", "v1.2.3-rc.1", "v01.2.3", "v1.2.3.4", "version"} {
		if isStableVersion(invalid) || semverGreater(invalid, "v1.0.0") {
			t.Fatalf("invalid stable version %q was accepted", invalid)
		}
	}
	if !semverGreater("v1.2.4", "v1.2.3") || semverGreater("v1.2.3", "v1.2.3") {
		t.Fatal("valid stable version ordering failed")
	}
}

func TestReleaseManifestIsRequired(t *testing.T) {
	targetName := testUpdater(Config{}).targetName()
	_, err := validateReleaseManifest(releaseVersionInfo{
		Version: "v1.2.3",
		Tag:     "v1.2.3",
		Assets:  map[string]string{targetName: strings.Repeat("a", 64)},
	}, "stable", "v1.2.3", targetName)
	if err == nil || !strings.Contains(err.Error(), "manifest version") {
		t.Fatalf("validateReleaseManifest error = %v, want missing manifest version", err)
	}
}

func TestReleaseManifestRejectsNonCanonicalDevCommit(t *testing.T) {
	targetName := testUpdater(Config{}).targetName()
	_, err := validateReleaseManifest(releaseVersionInfo{
		ManifestVersion: manifestVersion,
		Version:         "dev-0042-20260425-bbbbbbb",
		Commit:          "bbbbbbb-extra",
		Tag:             "dev",
		Assets:          map[string]string{targetName: strings.Repeat("a", 64)},
	}, "dev", "dev", targetName)
	if err == nil || !strings.Contains(err.Error(), "commit") {
		t.Fatalf("validateReleaseManifest error = %v, want non-canonical commit rejection", err)
	}
}

func TestNormalizeConfigInvalidChannelDefaultsToStable(t *testing.T) {
	if got := normalizeConfig(Config{Channel: "stabel"}).Channel; got != "stable" {
		t.Fatalf("invalid channel normalized to %q, want stable", got)
	}
	if got := normalizeConfig(Config{Channel: "DEV"}).Channel; got != "dev" {
		t.Fatalf("dev channel normalized to %q", got)
	}
}

func TestValidateConfigRejectsInvalidPolicies(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  Config
	}{
		{name: "channel", cfg: Config{Channel: "dve"}},
		{name: "source", cfg: Config{Source: "proxi"}},
		{name: "repo", cfg: Config{Repo: "owner/repo/extra"}},
		{name: "proxy credentials", cfg: Config{Source: SourceProxy, ProxyBaseURL: "https://user:secret@mirror.example"}},
		{name: "proxy query", cfg: Config{Source: SourceProxy, ProxyBaseURL: "https://mirror.example?"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidateConfig(tc.cfg); err == nil {
				t.Fatal("expected invalid update configuration to be rejected")
			} else if strings.Contains(err.Error(), "secret") {
				t.Fatalf("validation error leaked credentials: %v", err)
			}
		})
	}
}

func setTestSigningKey(t *testing.T) func(data []byte) string {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate test signing key: %v", err)
	}
	original := signingPublicKeyHex
	signingPublicKeyHex = hex.EncodeToString(pub)
	t.Cleanup(func() { signingPublicKeyHex = original })
	return func(data []byte) string {
		return base64.StdEncoding.EncodeToString(ed25519.Sign(priv, data))
	}
}

func setTestGitHubBaseURL(t *testing.T, url string) {
	t.Helper()
	original := githubBaseURL
	githubBaseURL = url
	t.Cleanup(func() { githubBaseURL = original })
}

func testUpdater(cfg Config) *Updater {
	return New(
		func() Config { return cfg },
		func() string { return "" },
		log.New(io.Discard, "", 0),
		RestartHooks{},
	)
}

func makeTestManifest(t *testing.T, sign func([]byte) string, releaseVersion, commit, tag, targetName, digest string) ([]byte, []byte) {
	t.Helper()
	metadata, err := json.Marshal(releaseVersionInfo{
		ManifestVersion: manifestVersion,
		Version:         releaseVersion,
		Commit:          commit,
		BuildTime:       "2026-04-25T00:00:00Z",
		Tag:             tag,
		Assets:          map[string]string{targetName: digest},
	})
	if err != nil {
		t.Fatal(err)
	}
	return metadata, []byte(sign(metadata))
}

func testMetadataAssets(tag string) []assetInfo {
	base := "https://github.com/owner/repo/releases/download/" + tag + "/"
	return []assetInfo{
		{Name: "version.json", BrowserDownloadURL: base + "version.json"},
		{Name: "version.json.sig", BrowserDownloadURL: base + "version.json.sig"},
	}
}
