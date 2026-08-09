package updater

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lieyan/responses2chat/internal/version"
)

type Config struct {
	Enabled       bool   `json:"enabled"`
	Channel       string `json:"channel"`
	CheckInterval int    `json:"check_interval"`
	Source        string `json:"source"`
	ProxyBaseURL  string `json:"proxy_base_url"`
	Repo          string `json:"repo"`
	AdminToken    string `json:"admin_token"`
}

type Status struct {
	State            string  `json:"state"`
	CurrentVersion   string  `json:"current_version"`
	LatestVersion    string  `json:"latest_version,omitempty"`
	IsPrerelease     bool    `json:"is_prerelease"`
	Progress         float64 `json:"progress,omitempty"`
	DownloadProgress float64 `json:"download_progress,omitempty"`
	Error            string  `json:"error,omitempty"`
	LastCheck        string  `json:"last_check,omitempty"`
	ReleaseNotes     string  `json:"release_notes,omitempty"`
}

type CheckResult struct {
	HasUpdate      bool   `json:"has_update"`
	CurrentVersion string `json:"current_version"`
	LatestVersion  string `json:"latest_version,omitempty"`
	IsPrerelease   bool   `json:"is_prerelease"`
	ReleaseNotes   string `json:"release_notes,omitempty"`
	Channel        string `json:"channel"`
}

type RestartHooks struct {
	BeforeExec    func(tag string) error
	OnExecFailure func(error)
	// IsBusy reports whether the application is processing work that should
	// not be interrupted by a restart. When set, the updater waits for it to
	// return false (bounded by idleWaitTimeout) before applying an update.
	IsBusy func() bool
}

type Updater struct {
	cfg     func() Config
	dataDir func() string
	logger  *log.Logger
	hooks   RestartHooks

	mu     sync.RWMutex
	status Status

	bgCtx context.Context

	pendingBinaryPath string
	pendingTag        string
}

const (
	idleWaitTimeout  = 10 * time.Minute
	idlePollInterval = 5 * time.Second
	downloadTimeout  = 30 * time.Minute
	maxBinarySize    = 256 << 20
	maxReleaseSize   = 1 << 20
	maxManifestSize  = 64 << 10
	maxChecksumSize  = 1024
	maxSignatureSize = 4 << 10
	manifestVersion  = 1

	// SourceGitHub downloads release assets straight from
	// github.com/<repo>/releases/download/... links, which are CDN
	// redirects and not subject to GitHub REST API rate limits.
	SourceGitHub = "github"
	// SourceProxy routes release lookups and downloads through the
	// configured proxy_base_url mirror.
	SourceProxy = "proxy"

	progressChecking      = 5
	progressReleaseFound  = 10
	progressDownloadStart = 10
	progressDownloadDone  = 90
	progressVerifyStart   = 92
	progressVerifyDone    = 95
	progressApplying      = 98
	progressComplete      = 100
)

func New(cfg func() Config, dataDir func() string, logger *log.Logger, hooks RestartHooks) *Updater {
	if logger == nil {
		logger = log.Default()
	}
	return &Updater{
		cfg:     cfg,
		dataDir: dataDir,
		logger:  logger,
		hooks:   hooks,
		status: Status{
			State:          "idle",
			CurrentVersion: version.Version,
		},
	}
}

func (u *Updater) Status() Status {
	u.mu.RLock()
	defer u.mu.RUnlock()
	s := u.status
	s.CurrentVersion = version.Version
	return s
}

func (u *Updater) CheckOnly(ctx context.Context) (CheckResult, error) {
	rawCfg := u.cfg()
	if err := ValidateConfig(rawCfg); err != nil {
		return CheckResult{}, err
	}
	cfg := normalizeConfig(rawCfg)
	result := CheckResult{
		CurrentVersion: version.Version,
		Channel:        cfg.Channel,
	}

	release, hasUpdate, err := u.checkForUpdate(ctx, cfg)
	if err != nil {
		return result, err
	}

	u.mu.Lock()
	u.status.LastCheck = time.Now().UTC().Format(time.RFC3339)
	u.mu.Unlock()

	if release == nil {
		return result, nil
	}

	result.HasUpdate = hasUpdate
	result.LatestVersion = release.displayVersion()
	result.IsPrerelease = release.Prerelease
	result.ReleaseNotes = release.Body

	u.mu.Lock()
	u.status.LatestVersion = release.displayVersion()
	u.status.IsPrerelease = release.Prerelease
	u.status.ReleaseNotes = release.Body
	u.mu.Unlock()

	return result, nil
}

func (u *Updater) StartUpdate(_ context.Context) {
	go u.performUpdate(u.bgContext())
}

func (u *Updater) ApplyPending(_ context.Context) error {
	u.mu.Lock()
	state := u.status.State
	path := u.pendingBinaryPath
	tag := u.pendingTag

	if state != "ready" || path == "" {
		u.mu.Unlock()
		return fmt.Errorf("no pending update to apply")
	}

	u.status.State = "applying"
	u.status.Progress = progressApplying
	u.status.DownloadProgress = 0
	u.pendingBinaryPath = ""
	u.pendingTag = ""
	u.mu.Unlock()

	go func() {
		time.Sleep(200 * time.Millisecond)
		if err := u.waitForIdle(u.bgContext()); err != nil {
			_ = os.Remove(path)
			u.setError("apply canceled while waiting for idle: " + err.Error())
			return
		}
		if err := u.applyUpdate(path, tag); err != nil {
			_ = os.Remove(path)
			u.notifyExecFailure(err)
			u.setError("apply failed: " + err.Error())
		}
	}()
	return nil
}

func (u *Updater) DismissPending() {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.status.State == "ready" {
		if u.pendingBinaryPath != "" {
			_ = os.Remove(u.pendingBinaryPath)
		}
		u.pendingBinaryPath = ""
		u.pendingTag = ""
		u.status.State = "idle"
		u.status.LatestVersion = ""
		u.status.Progress = 0
		u.status.DownloadProgress = 0
		u.status.Error = ""
	}
}

func (u *Updater) StartBackground(ctx context.Context) {
	rawCfg := u.cfg()
	if err := ValidateConfig(rawCfg); err != nil {
		u.logger.Printf("update: invalid configuration: %v", err)
		return
	}
	cfg := normalizeConfig(rawCfg)
	if !cfg.Enabled {
		u.logger.Printf("update: disabled")
		return
	}
	u.mu.Lock()
	u.bgCtx = ctx
	u.mu.Unlock()
	u.logger.Printf("update: enabled, channel=%s, interval=%ds", cfg.Channel, cfg.CheckInterval)
	go u.loop(ctx)
}

func (u *Updater) bgContext() context.Context {
	u.mu.RLock()
	defer u.mu.RUnlock()
	if u.bgCtx != nil {
		return u.bgCtx
	}
	return context.Background()
}

func (u *Updater) loop(ctx context.Context) {
	select {
	case <-time.After(30 * time.Second):
	case <-ctx.Done():
		return
	}

	u.checkAndUpdate(ctx)

	for {
		cfg := normalizeConfig(u.cfg())
		interval := time.Duration(cfg.CheckInterval) * time.Second
		if interval < time.Minute {
			interval = time.Minute
		}
		select {
		case <-time.After(interval):
			u.checkAndUpdate(ctx)
		case <-ctx.Done():
			return
		}
	}
}

func (u *Updater) checkAndUpdate(ctx context.Context) {
	cfg := normalizeConfig(u.cfg())
	if !cfg.Enabled {
		return
	}
	u.performUpdate(ctx)
}

func (u *Updater) performUpdate(ctx context.Context) {
	rawCfg := u.cfg()
	if err := ValidateConfig(rawCfg); err != nil {
		u.setError("invalid configuration: " + err.Error())
		return
	}
	cfg := normalizeConfig(rawCfg)

	u.mu.Lock()
	if u.status.State == "checking" || u.status.State == "ready" || u.status.State == "downloading" || u.status.State == "applying" {
		u.mu.Unlock()
		return
	}
	u.status.State = "checking"
	u.status.Progress = progressChecking
	u.status.Error = ""
	u.status.DownloadProgress = 0
	u.mu.Unlock()

	release, hasUpdate, err := u.checkForUpdate(ctx, cfg)
	if err != nil {
		u.setError("check failed: " + err.Error())
		return
	}
	if release == nil || !hasUpdate {
		u.mu.Lock()
		u.status.State = "idle"
		u.status.Progress = 0
		u.status.DownloadProgress = 0
		u.status.LastCheck = time.Now().UTC().Format(time.RFC3339)
		u.mu.Unlock()
		return
	}

	u.mu.Lock()
	u.status.LatestVersion = release.displayVersion()
	u.status.IsPrerelease = release.Prerelease
	u.status.ReleaseNotes = release.Body
	u.status.LastCheck = time.Now().UTC().Format(time.RFC3339)
	u.status.Progress = progressReleaseFound
	u.mu.Unlock()

	binaryPath, err := u.download(ctx, cfg, release)
	if err != nil {
		u.setError("download failed: " + err.Error())
		return
	}

	if cfg.Channel == "stable" {
		u.mu.Lock()
		u.status.State = "applying"
		u.status.Progress = progressApplying
		u.status.DownloadProgress = 0
		u.mu.Unlock()
		if err := u.waitForIdle(ctx); err != nil {
			_ = os.Remove(binaryPath)
			u.setError("apply canceled while waiting for idle: " + err.Error())
			return
		}
		if err := u.applyUpdate(binaryPath, release.TagName); err != nil {
			_ = os.Remove(binaryPath)
			u.notifyExecFailure(err)
			u.setError("apply failed: " + err.Error())
		}
		return
	}

	u.mu.Lock()
	u.status.State = "ready"
	u.status.Progress = progressVerifyDone
	u.status.DownloadProgress = 0
	u.pendingBinaryPath = binaryPath
	u.pendingTag = release.TagName
	u.mu.Unlock()
	u.logger.Printf("update: pre-release %s ready, waiting for admin confirmation", release.TagName)
}

func (u *Updater) setError(msg string) {
	u.logger.Printf("update: %s", msg)
	u.mu.Lock()
	defer u.mu.Unlock()
	u.status.State = "failed"
	u.status.Error = msg
	u.status.LastCheck = time.Now().UTC().Format(time.RFC3339)
}

func clampProgress(progress float64) float64 {
	if progress < 0 {
		return 0
	}
	if progress > 100 {
		return 100
	}
	return progress
}

func overallDownloadProgress(downloadProgress float64) float64 {
	downloadProgress = clampProgress(downloadProgress)
	span := progressDownloadDone - progressDownloadStart
	return progressDownloadStart + downloadProgress*float64(span)/100
}

func (u *Updater) notifyExecFailure(err error) {
	if err == nil || u.hooks.OnExecFailure == nil {
		return
	}
	u.hooks.OnExecFailure(err)
}

// waitForIdle blocks until the application reports no in-flight work. A
// canceled application context aborts the update; only the deliberate idle
// timeout permits applying while work is still reported as active.
func (u *Updater) waitForIdle(ctx context.Context) error {
	if u.hooks.IsBusy == nil || !u.hooks.IsBusy() {
		return nil
	}
	u.logger.Printf("update: waiting for in-flight jobs to finish before applying (max %s)", idleWaitTimeout)
	deadline := time.NewTimer(idleWaitTimeout)
	defer deadline.Stop()
	ticker := time.NewTicker(idlePollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			u.logger.Printf("update: idle wait timed out, applying anyway")
			return nil
		case <-ticker.C:
			if !u.hooks.IsBusy() {
				return nil
			}
		}
	}
}

type releaseInfo struct {
	TagName         string      `json:"tag_name"`
	TargetCommitish string      `json:"target_commitish"`
	Prerelease      bool        `json:"prerelease"`
	Body            string      `json:"body"`
	Assets          []assetInfo `json:"assets"`
	Version         string      `json:"-"`
	Commit          string      `json:"-"`
	BuildTime       string      `json:"-"`
	ManifestSHA256  string      `json:"-"`
}

type assetInfo struct {
	Name               string `json:"name"`
	BrowserDownloadURL string `json:"browser_download_url"`
	Size               int64  `json:"size"`
}

type releaseVersionInfo struct {
	ManifestVersion int               `json:"manifest_version"`
	Version         string            `json:"version"`
	Commit          string            `json:"commit"`
	BuildTime       string            `json:"build_time"`
	Tag             string            `json:"tag"`
	Assets          map[string]string `json:"assets"`
}

func (r releaseInfo) displayVersion() string {
	if strings.TrimSpace(r.Version) != "" {
		return strings.TrimSpace(r.Version)
	}
	return r.TagName
}

var (
	// githubBaseURL is a var so tests can point direct-source checks at a
	// local server.
	githubBaseURL = "https://github.com"
	// signingPublicKeyHex is the Ed25519 public key matching the
	// UPDATE_SIGNING_KEY repository secret used by CI to sign release
	// assets (see scripts/sign).
	signingPublicKeyHex = "84f05a289f964f3ac0c7ca590f43b1d8bb12ff0a19753cacff1a8cc9733443ed"
)

func (u *Updater) checkForUpdate(ctx context.Context, cfg Config) (*releaseInfo, bool, error) {
	checkCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	var (
		release *releaseInfo
		err     error
	)
	if cfg.Source == SourceProxy {
		release, err = u.fetchReleaseViaProxy(checkCtx, cfg)
	} else {
		release, err = u.fetchReleaseFromGitHub(checkCtx, cfg)
	}
	if err != nil || release == nil {
		return nil, false, err
	}
	if !u.isNewer(*release, cfg.Channel) {
		u.logger.Printf("update: already up to date (%s)", release.displayVersion())
		return release, false, nil
	}
	return release, true, nil
}

// fetchReleaseFromGitHub resolves the latest release without touching the
// GitHub REST API: it fetches the signed version.json straight from the
// release download URL (fixed "dev" tag, or the "latest" redirect for
// stable) and synthesizes the asset list from the tag it names.
func (u *Updater) fetchReleaseFromGitHub(ctx context.Context, cfg Config) (*releaseInfo, error) {
	owner, repo, err := splitRepo(cfg.Repo)
	if err != nil {
		return nil, err
	}
	base := githubBaseURL + "/" + owner + "/" + repo + "/releases"
	versionURL := base + "/latest/download/version.json"
	if cfg.Channel != "stable" {
		versionURL = base + "/download/dev/version.json"
	}
	u.logger.Printf("update: checking %s", versionURL)

	body, status, err := u.httpGet(ctx, versionURL, maxManifestSize)
	if err != nil {
		return nil, err
	}
	if status == http.StatusNotFound {
		u.logger.Printf("update: no release found for channel %s", cfg.Channel)
		return nil, nil
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %d", status)
	}

	var info releaseVersionInfo
	if err := json.Unmarshal(body, &info); err != nil {
		return nil, fmt.Errorf("decode version metadata: %w", err)
	}
	tag := strings.TrimSpace(info.Tag)
	if cfg.Channel != "stable" {
		tag = "dev"
	} else if tag == "" {
		return nil, fmt.Errorf("version metadata missing release tag")
	} else if !isStableVersion(tag) {
		return nil, fmt.Errorf("version metadata has invalid stable tag %q", tag)
	}

	// The signature lives next to the assets of the tag the metadata
	// names, so a tampered body cannot redirect verification elsewhere:
	// only the holder of the signing key can produce a matching pair.
	sig, sigStatus, err := u.httpGet(ctx, base+"/download/"+tag+"/version.json.sig", maxSignatureSize)
	if err != nil {
		return nil, fmt.Errorf("fetch version metadata signature: %w", err)
	}
	if sigStatus != http.StatusOK {
		return nil, fmt.Errorf("version metadata signature returned status %d", sigStatus)
	}
	if err := verifySignature(body, sig); err != nil {
		return nil, fmt.Errorf("version metadata: %w", err)
	}

	targetName := u.targetName()
	manifestHash, err := validateReleaseManifest(info, cfg.Channel, tag, targetName)
	if err != nil {
		return nil, err
	}
	assetNames := []string{targetName, targetName + ".sha256", targetName + ".sha256.sig", "version.json"}
	assets := make([]assetInfo, 0, len(assetNames))
	for _, name := range assetNames {
		assets = append(assets, assetInfo{
			Name:               name,
			BrowserDownloadURL: base + "/download/" + tag + "/" + name,
		})
	}
	return &releaseInfo{
		TagName:        tag,
		Prerelease:     cfg.Channel != "stable",
		Assets:         assets,
		Version:        strings.TrimSpace(info.Version),
		Commit:         strings.TrimSpace(info.Commit),
		BuildTime:      strings.TrimSpace(info.BuildTime),
		ManifestSHA256: manifestHash,
	}, nil
}

func (u *Updater) fetchReleaseViaProxy(ctx context.Context, cfg Config) (*releaseInfo, error) {
	tag := "latest"
	if cfg.Channel != "stable" {
		tag = "dev"
	}

	owner, repo, err := splitRepo(cfg.Repo)
	if err != nil {
		return nil, err
	}
	releaseURL := fmt.Sprintf("%s/api/releases/%s/%s/%s", strings.TrimRight(cfg.ProxyBaseURL, "/"), owner, repo, tag)
	u.logger.Printf("update: checking %s", releaseURL)

	body, status, err := u.httpGetNoRedirect(ctx, releaseURL, maxReleaseSize)
	if err != nil {
		return nil, err
	}
	if status == http.StatusNotFound {
		u.logger.Printf("update: no release found for channel %s", cfg.Channel)
		return nil, nil
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %d", status)
	}

	var release releaseInfo
	if err := json.Unmarshal(body, &release); err != nil {
		return nil, fmt.Errorf("decode release: %w", err)
	}
	release.TagName = strings.TrimSpace(release.TagName)
	if cfg.Channel == "stable" {
		if !isStableVersion(release.TagName) || release.Prerelease {
			return nil, fmt.Errorf("proxy returned invalid stable release tag %q", release.TagName)
		}
	} else if release.TagName != "dev" || !release.Prerelease {
		return nil, fmt.Errorf("proxy returned invalid dev release tag %q", release.TagName)
	}
	if err := u.loadReleaseVersion(ctx, cfg, &release); err != nil {
		return nil, err
	}
	return &release, nil
}

func (u *Updater) loadReleaseVersion(ctx context.Context, cfg Config, release *releaseInfo) error {
	var versionAsset, signatureAsset *assetInfo
	for i := range release.Assets {
		switch release.Assets[i].Name {
		case "version.json":
			versionAsset = &release.Assets[i]
		case "version.json.sig":
			signatureAsset = &release.Assets[i]
		}
	}
	if versionAsset == nil {
		return fmt.Errorf("version.json asset not found")
	}
	if signatureAsset == nil {
		return fmt.Errorf("version.json.sig asset not found")
	}
	body, err := u.fetchAsset(ctx, cfg, release.TagName, versionAsset, maxManifestSize)
	if err != nil {
		return fmt.Errorf("fetch version metadata: %w", err)
	}
	sig, err := u.fetchAsset(ctx, cfg, release.TagName, signatureAsset, maxSignatureSize)
	if err != nil {
		return fmt.Errorf("fetch version metadata signature: %w", err)
	}
	if err := verifySignature(body, sig); err != nil {
		return fmt.Errorf("version metadata: %w", err)
	}

	var info releaseVersionInfo
	if err := json.Unmarshal(body, &info); err != nil {
		return fmt.Errorf("decode version metadata: %w", err)
	}
	manifestHash, err := validateReleaseManifest(info, cfg.Channel, release.TagName, u.targetName())
	if err != nil {
		return err
	}
	release.Version = strings.TrimSpace(info.Version)
	release.Commit = strings.TrimSpace(info.Commit)
	release.BuildTime = strings.TrimSpace(info.BuildTime)
	release.ManifestSHA256 = manifestHash
	return nil
}

func (u *Updater) isNewer(release releaseInfo, channel string) bool {
	current := version.Version
	if current == "dev" {
		return true
	}
	remoteTag := release.TagName
	if channel == "stable" {
		if !isStableVersion(current) {
			return true
		}
		return semverGreater(remoteTag, current)
	}

	remoteVersion := release.displayVersion()
	remoteNum, remoteSHA, remoteOK := parseDevTag(remoteVersion)
	localNum, localSHA, localOK := parseDevTag(current)
	if !remoteOK {
		u.logger.Printf("update: cannot compare dev versions current=%s remote=%s, skipping", current, remoteVersion)
		return false
	}
	if !localOK {
		return true
	}
	if remoteNum != localNum {
		return remoteNum > localNum
	}
	if remoteSHA != localSHA {
		u.logger.Printf("update: dev versions reuse run %d with different commits current=%s remote=%s, skipping", localNum, localSHA, remoteSHA)
	}
	return false
}
func semverGreater(a, b string) bool {
	av, aOK := parseStableVersion(a)
	bv, bOK := parseStableVersion(b)
	if !aOK || !bOK {
		return false
	}
	for i := 0; i < 3; i++ {
		if av[i] > bv[i] {
			return true
		}
		if av[i] < bv[i] {
			return false
		}
	}
	return false
}

func isStableVersion(value string) bool {
	_, ok := parseStableVersion(value)
	return ok
}

func parseStableVersion(value string) ([3]uint64, bool) {
	var result [3]uint64
	if !strings.HasPrefix(value, "v") {
		return result, false
	}
	parts := strings.Split(strings.TrimPrefix(value, "v"), ".")
	if len(parts) != len(result) {
		return result, false
	}
	for i, part := range parts {
		if part == "" || (len(part) > 1 && part[0] == '0') || !isASCIIDigits(part) {
			return result, false
		}
		n, err := strconv.ParseUint(part, 10, 64)
		if err != nil {
			return result, false
		}
		result[i] = n
	}
	return result, true
}

func parseDevTag(tag string) (runNumber uint64, sha string, ok bool) {
	parts := strings.Split(tag, "-")
	if len(parts) != 4 || parts[0] != "dev" || len(parts[1]) < 4 || len(parts[2]) != 8 || len(parts[3]) != 7 {
		return 0, "", false
	}
	if !isASCIIDigits(parts[1]) || !isASCIIDigits(parts[2]) || !isHex(parts[3]) {
		return 0, "", false
	}
	n, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil || n == 0 {
		return 0, "", false
	}
	return n, strings.ToLower(parts[3]), true
}

func validateReleaseManifest(info releaseVersionInfo, channel, expectedTag, targetName string) (string, error) {
	if info.ManifestVersion != manifestVersion {
		return "", fmt.Errorf("unsupported release manifest version %d", info.ManifestVersion)
	}
	info.Tag = strings.TrimSpace(info.Tag)
	info.Version = strings.TrimSpace(info.Version)
	info.Commit = strings.TrimSpace(info.Commit)
	if info.Tag != expectedTag {
		return "", fmt.Errorf("signed release tag %q does not match discovered tag %q", info.Tag, expectedTag)
	}
	if channel == "stable" {
		if !isStableVersion(info.Tag) || info.Version != info.Tag {
			return "", fmt.Errorf("signed stable manifest has inconsistent version %q and tag %q", info.Version, info.Tag)
		}
	} else {
		if info.Tag != "dev" {
			return "", fmt.Errorf("signed dev manifest has invalid tag %q", info.Tag)
		}
		_, versionSHA, ok := parseDevTag(info.Version)
		if !ok {
			return "", fmt.Errorf("signed dev manifest has invalid version %q", info.Version)
		}
		if len(info.Commit) != 7 || !isHex(info.Commit) || strings.ToLower(info.Commit) != versionSHA {
			return "", fmt.Errorf("signed dev manifest commit %q does not match version %q", info.Commit, info.Version)
		}
	}
	if len(info.Assets) == 0 {
		return "", fmt.Errorf("signed release manifest has no assets")
	}
	for name, digest := range info.Assets {
		if name == "" || strings.TrimSpace(name) != name || strings.ContainsAny(name, "/\\") {
			return "", fmt.Errorf("signed release manifest has invalid asset name %q", name)
		}
		if _, err := normalizeSHA256(digest); err != nil {
			return "", fmt.Errorf("signed release manifest asset %q: %w", name, err)
		}
	}
	digest, ok := info.Assets[targetName]
	if !ok {
		return "", fmt.Errorf("signed release manifest has no hash for %s", targetName)
	}
	return normalizeSHA256(digest)
}

func normalizeSHA256(value string) (string, error) {
	if len(value) != sha256.Size*2 || !isHex(value) {
		return "", fmt.Errorf("invalid SHA256 digest %q", value)
	}
	return strings.ToLower(value), nil
}

func isASCIIDigits(value string) bool {
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

func (u *Updater) targetName() string {
	goos := runtime.GOOS
	goarch := runtime.GOARCH
	target := goos + "-" + goarch

	ext := ""
	if goos == "windows" {
		ext = ".exe"
	}
	return "responses2chat-" + target + ext
}

func (u *Updater) download(ctx context.Context, cfg Config, release *releaseInfo) (string, error) {
	u.mu.Lock()
	u.status.State = "downloading"
	u.status.Progress = progressDownloadStart
	u.status.DownloadProgress = 0
	u.mu.Unlock()

	targetName := u.targetName()
	var binaryAsset, sha256Asset, sigAsset *assetInfo
	for i := range release.Assets {
		a := &release.Assets[i]
		switch a.Name {
		case targetName:
			binaryAsset = a
		case targetName + ".sha256":
			sha256Asset = a
		case targetName + ".sha256.sig":
			sigAsset = a
		}
	}
	if binaryAsset == nil {
		return "", fmt.Errorf("no asset found for %s in release %s", targetName, release.TagName)
	}
	if sha256Asset == nil {
		return "", fmt.Errorf("release %s is missing %s.sha256, refusing unverified update", release.TagName, targetName)
	}
	if sigAsset == nil {
		return "", fmt.Errorf("release %s is missing %s.sha256.sig, refusing unsigned update", release.TagName, targetName)
	}

	updateDir := filepath.Join(u.dataDir(), "updates")
	if err := os.MkdirAll(updateDir, 0o700); err != nil {
		return "", fmt.Errorf("create update dir: %w", err)
	}
	if err := os.Chmod(updateDir, 0o700); err != nil {
		return "", fmt.Errorf("secure update dir: %w", err)
	}

	finalName := "responses2chat-" + sanitizePathPart(release.TagName)
	if runtime.GOOS == "windows" {
		finalName += ".exe"
	}
	finalPath := filepath.Join(updateDir, finalName)

	dlCtx, cancelDownload := context.WithTimeout(ctx, downloadTimeout)
	defer cancelDownload()

	downloadURL, err := u.resolveDownloadURL(cfg, release.TagName, binaryAsset)
	if err != nil {
		return "", fmt.Errorf("resolve binary download URL: %w", err)
	}
	tmpFile, err := os.CreateTemp(updateDir, "."+finalName+"-*.tmp")
	if err != nil {
		return "", fmt.Errorf("create temporary update file: %w", err)
	}
	tmpPath := tmpFile.Name()
	if err := u.downloadToOpenFile(dlCtx, downloadURL, tmpPath, tmpFile, binaryAsset.Size, maxBinarySize, cfg.Source != SourceProxy); err != nil {
		return "", fmt.Errorf("download binary: %w", err)
	}

	u.mu.Lock()
	u.status.Progress = progressVerifyStart
	u.mu.Unlock()

	shaBody, err := u.fetchAsset(dlCtx, cfg, release.TagName, sha256Asset, maxChecksumSize)
	if err != nil {
		_ = os.Remove(tmpPath)
		return "", fmt.Errorf("fetch sha256: %w", err)
	}
	sigBody, err := u.fetchAsset(dlCtx, cfg, release.TagName, sigAsset, maxSignatureSize)
	if err != nil {
		_ = os.Remove(tmpPath)
		return "", fmt.Errorf("fetch sha256 signature: %w", err)
	}
	if err := verifySignature(shaBody, sigBody); err != nil {
		_ = os.Remove(tmpPath)
		return "", fmt.Errorf("sha256 file: %w", err)
	}

	expectedHash, err := parseChecksum(shaBody, targetName)
	if err != nil {
		_ = os.Remove(tmpPath)
		return "", err
	}
	if !strings.EqualFold(expectedHash, release.ManifestSHA256) {
		_ = os.Remove(tmpPath)
		return "", fmt.Errorf("sha256 %s does not match signed manifest hash %s", expectedHash, release.ManifestSHA256)
	}
	actualHash, err := fileSHA256(tmpPath)
	if err != nil {
		_ = os.Remove(tmpPath)
		return "", fmt.Errorf("compute sha256: %w", err)
	}
	if !strings.EqualFold(actualHash, expectedHash) {
		_ = os.Remove(tmpPath)
		return "", fmt.Errorf("sha256 mismatch: expected %s, got %s", expectedHash, actualHash)
	}
	u.logger.Printf("update: signature and SHA256 verified for %s", release.TagName)

	u.mu.Lock()
	u.status.Progress = progressVerifyDone
	u.mu.Unlock()

	if err := os.Remove(finalPath); err != nil && !os.IsNotExist(err) {
		_ = os.Remove(tmpPath)
		return "", fmt.Errorf("replace previous pending update: %w", err)
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		return "", fmt.Errorf("finalize update download: %w", err)
	}

	u.logger.Printf("update: downloaded %s to %s", release.TagName, finalPath)
	return finalPath, nil
}

// resolveDownloadURL maps a GitHub browser_download_url onto the proxy
// mirror when the proxy source is selected; in direct mode the URL is used
// as-is.
func (u *Updater) resolveDownloadURL(cfg Config, tag string, asset *assetInfo) (string, error) {
	if cfg.Source != SourceProxy {
		return asset.BrowserDownloadURL, nil
	}
	owner, repo, err := splitRepo(cfg.Repo)
	if err != nil {
		return "", err
	}
	parsed, err := url.Parse(asset.BrowserDownloadURL)
	if err != nil {
		return "", fmt.Errorf("invalid GitHub asset URL: %w", err)
	}
	if parsed.Scheme != "https" || parsed.Host != "github.com" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", fmt.Errorf("asset %s has untrusted download URL %q", asset.Name, asset.BrowserDownloadURL)
	}
	expectedPath := "/" + owner + "/" + repo + "/releases/download/" + tag + "/" + asset.Name
	if parsed.Path != expectedPath {
		return "", fmt.Errorf("asset %s URL path %q does not match %q", asset.Name, parsed.Path, expectedPath)
	}
	proxy, err := parseProxyBaseURL(cfg.ProxyBaseURL)
	if err != nil {
		return "", err
	}
	proxy.Path = strings.TrimRight(proxy.Path, "/") + "/download/" + owner + "/" + repo + "/" + tag + "/" + asset.Name
	proxy.RawPath = ""
	return proxy.String(), nil
}

func (u *Updater) downloadFileWithLimit(ctx context.Context, url, destPath string, expectedSize, limit int64) (resultErr error) {
	return u.downloadFileWithOptions(ctx, url, destPath, expectedSize, limit, true)
}

func (u *Updater) downloadFileWithOptions(ctx context.Context, url, destPath string, expectedSize, limit int64, followRedirects bool) (resultErr error) {
	f, err := os.OpenFile(destPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	return u.downloadToOpenFile(ctx, url, destPath, f, expectedSize, limit, followRedirects)
}

func (u *Updater) downloadToOpenFile(ctx context.Context, url, destPath string, f *os.File, expectedSize, limit int64, followRedirects bool) (resultErr error) {
	complete := false
	defer func() {
		_ = f.Close()
		if !complete {
			_ = os.Remove(destPath)
		}
	}()
	if limit <= 0 {
		return fmt.Errorf("invalid download size limit %d", limit)
	}
	if expectedSize > limit {
		return fmt.Errorf("asset size %d exceeds %d-byte limit", expectedSize, limit)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := doHTTPRequest(req, followRedirects)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download returned status %d", resp.StatusCode)
	}
	if resp.ContentLength > limit {
		return fmt.Errorf("download size %d exceeds %d-byte limit", resp.ContentLength, limit)
	}

	totalSize := resp.ContentLength
	if totalSize <= 0 && expectedSize > 0 {
		totalSize = expectedSize
	}

	var written int64
	var lastProgress float64
	buf := make([]byte, 32*1024)
	for {
		n, readErr := resp.Body.Read(buf)
		if n > 0 {
			if written+int64(n) > limit {
				return fmt.Errorf("download exceeds %d-byte limit", limit)
			}
			if _, wErr := f.Write(buf[:n]); wErr != nil {
				return wErr
			}
			written += int64(n)
			if totalSize > 0 {
				progress := float64(written) / float64(totalSize) * 100
				if progress-lastProgress >= 1 || progress >= 100 {
					u.mu.Lock()
					u.status.DownloadProgress = clampProgress(progress)
					u.status.Progress = overallDownloadProgress(progress)
					u.mu.Unlock()
					lastProgress = progress
				}
			}
		}
		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			return readErr
		}
	}

	u.mu.Lock()
	u.status.DownloadProgress = progressComplete
	u.status.Progress = overallDownloadProgress(progressComplete)
	u.mu.Unlock()
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	complete = true
	return nil
}

// httpGet fetches url and returns up to limit bytes of the body along with
// the status code. Network errors are returned; HTTP error statuses are not.
func (u *Updater) httpGet(ctx context.Context, url string, limit int64) ([]byte, int, error) {
	return u.httpGetWithRedirects(ctx, url, limit, true)
}

func (u *Updater) httpGetNoRedirect(ctx context.Context, url string, limit int64) ([]byte, int, error) {
	return u.httpGetWithRedirects(ctx, url, limit, false)
}

func (u *Updater) httpGetWithRedirects(ctx context.Context, url string, limit int64, followRedirects bool) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, 0, err
	}
	resp, err := doHTTPRequest(req, followRedirects)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	if resp.ContentLength > limit {
		return nil, resp.StatusCode, fmt.Errorf("response body size %d exceeds %d-byte limit", resp.ContentLength, limit)
	}
	body, err := readBodyLimited(resp.Body, limit)
	if err != nil {
		return nil, resp.StatusCode, err
	}
	return body, resp.StatusCode, nil
}

func (u *Updater) fetchAsset(ctx context.Context, cfg Config, tag string, asset *assetInfo, limit int64) ([]byte, error) {
	downloadURL, err := u.resolveDownloadURL(cfg, tag, asset)
	if err != nil {
		return nil, err
	}
	body, status, err := u.httpGetWithRedirects(ctx, downloadURL, limit, cfg.Source != SourceProxy)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("%s returned status %d", asset.Name, status)
	}
	return body, nil
}

func doHTTPRequest(req *http.Request, followRedirects bool) (*http.Response, error) {
	transport := http.DefaultClient.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   http.DefaultClient.Timeout,
		CheckRedirect: func(redirect *http.Request, via []*http.Request) error {
			if followRedirects && isTrustedGitHubRedirect(redirect.URL) {
				if len(via) >= 10 {
					return fmt.Errorf("stopped after 10 redirects")
				}
				return nil
			}
			return http.ErrUseLastResponse
		},
	}
	return client.Do(req)
}

func isTrustedGitHubRedirect(target *url.URL) bool {
	if target == nil || target.Scheme != "https" || target.User != nil || (target.Port() != "" && target.Port() != "443") {
		return false
	}
	host := strings.ToLower(target.Hostname())
	return host == "github.com" || strings.HasSuffix(host, ".githubusercontent.com")
}

func readBodyLimited(body io.Reader, limit int64) ([]byte, error) {
	if limit < 0 {
		return nil, fmt.Errorf("invalid body size limit %d", limit)
	}
	data, err := io.ReadAll(io.LimitReader(body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("response body exceeds %d-byte limit", limit)
	}
	return data, nil
}

func parseChecksum(body []byte, targetName string) (string, error) {
	parts := strings.Fields(strings.TrimSpace(string(body)))
	if len(parts) != 2 {
		return "", fmt.Errorf("invalid sha256 file: expected digest and filename")
	}
	digest, err := normalizeSHA256(parts[0])
	if err != nil {
		return "", fmt.Errorf("invalid sha256 file: %w", err)
	}
	if parts[1] != targetName {
		return "", fmt.Errorf("sha256 file names %q, expected %q", parts[1], targetName)
	}
	return digest, nil
}

func splitRepo(value string) (string, string, error) {
	parts := strings.Split(value, "/")
	if len(parts) != 2 || !isGitHubName(parts[0]) || !isGitHubName(parts[1]) {
		return "", "", fmt.Errorf("invalid GitHub repo %q", value)
	}
	return parts[0], parts[1], nil
}

func isGitHubName(value string) bool {
	if value == "" || value == "." || value == ".." {
		return false
	}
	for i := range value {
		c := value[i]
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.') {
			return false
		}
	}
	return true
}

// verifySignature checks the base64 Ed25519 signature produced by
// scripts/sign against the embedded release signing public key.
func verifySignature(message, sig []byte) error {
	pub, err := hex.DecodeString(signingPublicKeyHex)
	if err != nil || len(pub) != ed25519.PublicKeySize {
		return fmt.Errorf("invalid embedded signing public key")
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(sig)))
	if err != nil {
		return fmt.Errorf("decode signature: %w", err)
	}
	if !ed25519.Verify(ed25519.PublicKey(pub), message, raw) {
		return fmt.Errorf("signature verification failed")
	}
	return nil
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

func (u *Updater) applyUpdate(newBinaryPath, tag string) error {
	u.mu.Lock()
	u.status.State = "applying"
	u.status.Progress = progressApplying
	u.mu.Unlock()

	if runtime.GOOS == "windows" {
		return u.applyUpdateWindows(newBinaryPath, tag)
	}
	return u.applyUpdateUnix(newBinaryPath, tag)
}

func (u *Updater) applyUpdateUnix(newBinaryPath, tag string) error {
	if u.hooks.BeforeExec != nil {
		if err := u.hooks.BeforeExec(tag); err != nil {
			return fmt.Errorf("prepare restart: %w", err)
		}
	}

	execPath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("resolve executable path: %w", err)
	}
	execPath, err = filepath.EvalSymlinks(execPath)
	if err != nil {
		return fmt.Errorf("resolve symlinks: %w", err)
	}

	backupPath, err := installBinaryUnix(newBinaryPath, execPath)
	if err != nil {
		return err
	}

	_ = os.Remove(newBinaryPath)

	u.logger.Printf("update: restarting with new binary %s", tag)
	u.mu.Lock()
	u.status.Progress = progressComplete
	u.mu.Unlock()
	if err := replaceProcess(execPath, os.Args, os.Environ()); err != nil {
		return rollbackBinary(execPath, backupPath, fmt.Errorf("restart with new binary: %w", err))
	}
	return nil
}

func installBinaryUnix(newBinaryPath, execPath string) (string, error) {
	info, err := os.Stat(execPath)
	if err != nil {
		return "", fmt.Errorf("stat current binary: %w", err)
	}
	backupPath := execPath + ".bak"
	// On Unix, rename atomically replaces a stale regular-file backup.
	if err := os.Rename(execPath, backupPath); err != nil {
		return "", fmt.Errorf("backup current binary: %w", err)
	}
	if err := copyFile(newBinaryPath, execPath, info.Mode().Perm()); err != nil {
		return "", rollbackBinary(execPath, backupPath, fmt.Errorf("install new binary: %w", err))
	}
	return backupPath, nil
}

func rollbackBinary(execPath, backupPath string, cause error) error {
	if err := os.Rename(backupPath, execPath); err != nil {
		return fmt.Errorf("%w; rollback failed: %v", cause, err)
	}
	return cause
}

func (u *Updater) applyUpdateWindows(newBinaryPath, tag string) error {
	execPath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("resolve executable path: %w", err)
	}
	execPath, err = filepath.Abs(execPath)
	if err != nil {
		return fmt.Errorf("resolve executable path: %w", err)
	}
	cwd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("resolve working directory: %w", err)
	}

	updateDir := filepath.Dir(newBinaryPath)
	backupPath := execPath + ".bak"
	script := strings.Join([]string{
		"$ErrorActionPreference = 'Stop'",
		fmt.Sprintf("$pidToWait = %d", os.Getpid()),
		"$exe = " + psQuote(execPath),
		"$new = " + psQuote(newBinaryPath),
		"$bak = " + psQuote(backupPath),
		"$argsList = " + psArray(os.Args[1:]),
		"$workDir = " + psQuote(cwd),
		"while (Get-Process -Id $pidToWait -ErrorAction SilentlyContinue) { Start-Sleep -Milliseconds 250 }",
		"try {",
		"  if (Test-Path $bak) { Remove-Item -Force $bak }",
		"  if (Test-Path $exe) { Move-Item -Force $exe $bak }",
		"  Copy-Item -Force $new $exe",
		"  Remove-Item -Force $new",
		"  Start-Process -FilePath $exe -ArgumentList $argsList -WorkingDirectory $workDir",
		"} catch {",
		"  if (Test-Path $bak) {",
		"    if (Test-Path $exe) { Remove-Item -Force $exe }",
		"    Move-Item -Force $bak $exe",
		"  }",
		"  throw",
		"} finally {",
		"  Remove-Item -Force $PSCommandPath -ErrorAction SilentlyContinue",
		"}",
		"",
	}, "\r\n")
	scriptFile, err := os.CreateTemp(updateDir, ".apply-*.ps1")
	if err != nil {
		return fmt.Errorf("create apply script: %w", err)
	}
	scriptPath := scriptFile.Name()
	if _, err := io.WriteString(scriptFile, script); err != nil {
		_ = scriptFile.Close()
		_ = os.Remove(scriptPath)
		return fmt.Errorf("write apply script: %w", err)
	}
	if err := scriptFile.Sync(); err != nil {
		_ = scriptFile.Close()
		_ = os.Remove(scriptPath)
		return fmt.Errorf("sync apply script: %w", err)
	}
	if err := scriptFile.Close(); err != nil {
		_ = os.Remove(scriptPath)
		return fmt.Errorf("close apply script: %w", err)
	}

	if u.hooks.BeforeExec != nil {
		if err := u.hooks.BeforeExec(tag); err != nil {
			_ = os.Remove(scriptPath)
			return fmt.Errorf("prepare restart: %w", err)
		}
	}

	proc, err := os.StartProcess("powershell.exe", []string{
		"powershell.exe",
		"-NoProfile",
		"-ExecutionPolicy", "Bypass",
		"-File", scriptPath,
	}, &os.ProcAttr{
		Files: []*os.File{os.Stdin, os.Stdout, os.Stderr},
		Env:   os.Environ(),
	})
	if err != nil {
		_ = os.Remove(scriptPath)
		return fmt.Errorf("start apply script: %w", err)
	}
	_ = proc.Release()

	u.logger.Printf("update: restarting with new binary %s", tag)
	u.mu.Lock()
	u.status.Progress = progressComplete
	u.mu.Unlock()
	os.Exit(0)
	return nil
}

func copyFile(src, dst string, mode os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return err
	}
	complete := false
	defer func() {
		if !complete {
			_ = out.Close()
			_ = os.Remove(dst)
		}
	}()
	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	if err := out.Chmod(mode); err != nil {
		return err
	}
	if err := out.Sync(); err != nil {
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	complete = true
	return nil
}

func normalizeConfig(cfg Config) Config {
	cfg.Channel = strings.ToLower(strings.TrimSpace(cfg.Channel))
	if cfg.Channel != "dev" {
		cfg.Channel = "stable"
	}
	if cfg.CheckInterval <= 0 {
		cfg.CheckInterval = 3600
	}
	cfg.Source = strings.ToLower(strings.TrimSpace(cfg.Source))
	if cfg.Source != SourceProxy {
		cfg.Source = SourceGitHub
	}
	if strings.TrimSpace(cfg.ProxyBaseURL) == "" {
		cfg.ProxyBaseURL = "https://dl.repo.chycloud.top"
	}
	cfg.ProxyBaseURL = strings.TrimRight(strings.TrimSpace(cfg.ProxyBaseURL), "/")
	cfg.Repo = strings.TrimSpace(cfg.Repo)
	if cfg.Repo == "" {
		cfg.Repo = "lieyanc/responses2chat"
	}
	cfg.AdminToken = strings.TrimSpace(cfg.AdminToken)
	return cfg
}

// ValidateConfig rejects misspelled release policies instead of silently
// switching channels or bypassing an explicitly requested mirror.
func ValidateConfig(cfg Config) error {
	channel := strings.ToLower(strings.TrimSpace(cfg.Channel))
	if channel != "" && channel != "stable" && channel != "dev" {
		return fmt.Errorf("invalid update channel %q: must be stable or dev", cfg.Channel)
	}
	source := strings.ToLower(strings.TrimSpace(cfg.Source))
	if source != "" && source != SourceGitHub && source != SourceProxy {
		return fmt.Errorf("invalid update source %q: must be github or proxy", cfg.Source)
	}
	normalized := normalizeConfig(cfg)
	if _, _, err := splitRepo(normalized.Repo); err != nil {
		return err
	}
	if normalized.Source == SourceProxy {
		if _, err := parseProxyBaseURL(normalized.ProxyBaseURL); err != nil {
			return err
		}
	}
	return nil
}

func parseProxyBaseURL(raw string) (*url.URL, error) {
	parsed, err := url.Parse(raw)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" {
		return nil, fmt.Errorf("invalid proxy_base_url")
	}
	return parsed, nil
}

func sanitizePathPart(value string) string {
	var b strings.Builder
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '.', r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	if b.Len() == 0 {
		return "update"
	}
	return b.String()
}

func psQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func psArray(values []string) string {
	if len(values) == 0 {
		return "@()"
	}
	quoted := make([]string, 0, len(values))
	for _, value := range values {
		quoted = append(quoted, psQuote(quoteWindowsArg(value)))
	}
	return "@(" + strings.Join(quoted, ", ") + ")"
}

// quoteWindowsArg preserves one Go argument when PowerShell joins the
// Start-Process ArgumentList array into a Windows command line.
func quoteWindowsArg(value string) string {
	var b strings.Builder
	b.Grow(len(value) + 2)
	b.WriteByte('"')
	backslashes := 0
	for i := 0; i < len(value); i++ {
		if value[i] == '\\' {
			backslashes++
			continue
		}
		if value[i] == '"' {
			backslashes = backslashes*2 + 1
		}
		for ; backslashes > 0; backslashes-- {
			b.WriteByte('\\')
		}
		b.WriteByte(value[i])
	}
	for ; backslashes > 0; backslashes-- {
		b.WriteString(`\\`)
	}
	b.WriteByte('"')
	return b.String()
}
