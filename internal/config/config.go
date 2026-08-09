// Package config loads the single config.json file that carries every
// runtime setting. Environment variables are deliberately not consulted:
// the config file is the only source of configuration.
package config

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/tidwall/gjson"
	"github.com/tidwall/pretty"
	"github.com/tidwall/sjson"
)

// Template is the pristine config.json bundled into the binary; it is
// extracted on first startup so operators edit a real file instead of
// copying one from documentation.
//
//go:embed config.template.json
var Template []byte

// DefaultPath is where config.json is looked up when -config is not given,
// relative to the working directory.
const DefaultPath = "config.json"

// Update carries the settings consumed by `responses2chat update`. Empty
// fields fall back to the updater's built-in defaults.
type Update struct {
	Channel      string `json:"channel"`
	Source       string `json:"source"`
	ProxyBaseURL string `json:"proxy_base_url"`
	Repo         string `json:"repo"`
}

type Config struct {
	ListenAddr                 string `json:"listen_addr"`
	UpstreamBaseURL            string `json:"upstream_base_url"`
	UpstreamChatCompletionsURL string `json:"upstream_chat_completions_url"`
	// UpstreamProxyURL routes upstream requests through the given proxy
	// (http, https, socks5 or socks5h). Empty falls back to the standard
	// proxy environment variables (HTTP_PROXY/HTTPS_PROXY/NO_PROXY).
	UpstreamProxyURL     string `json:"upstream_proxy_url"`
	ReasoningPassthrough bool   `json:"reasoning_passthrough"`
	// RetryUnsupportedParams retries a request once, with the offending
	// fields removed, when the upstream rejects it with a 400 error naming
	// unsupported parameters (e.g. "Unsupported parameter(s): `prompt_cache_key`").
	// A pointer so that a missing key defaults to enabled.
	RetryUnsupportedParams *bool  `json:"retry_unsupported_params"`
	Update                 Update `json:"update"`
}

// RetryUnsupportedParamsEnabled reports the retry_unsupported_params
// setting, defaulting to true when the key is absent.
func (c Config) RetryUnsupportedParamsEnabled() bool {
	return c.RetryUnsupportedParams == nil || *c.RetryUnsupportedParams
}

// Load parses the file at path and applies defaults. Serving traffic
// additionally requires ValidateServer; `update` runs without an upstream.
func Load(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("parse %s: %w", path, err)
	}
	cfg.ListenAddr = strings.TrimSpace(cfg.ListenAddr)
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = ":8080"
	}
	cfg.UpstreamBaseURL = strings.TrimSpace(cfg.UpstreamBaseURL)
	cfg.UpstreamChatCompletionsURL = strings.TrimSpace(cfg.UpstreamChatCompletionsURL)
	cfg.UpstreamProxyURL = strings.TrimSpace(cfg.UpstreamProxyURL)
	return cfg, nil
}

// ProxyURL parses upstream_proxy_url, returning nil when it is unset.
func (c Config) ProxyURL() (*url.URL, error) {
	if c.UpstreamProxyURL == "" {
		return nil, nil
	}
	parsed, err := url.Parse(c.UpstreamProxyURL)
	if err != nil || parsed.Host == "" || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") {
		return nil, fmt.Errorf("invalid upstream_proxy_url %q", c.UpstreamProxyURL)
	}
	switch parsed.Scheme {
	case "http", "https", "socks5", "socks5h":
		return parsed, nil
	default:
		return nil, fmt.Errorf("upstream_proxy_url %q: scheme must be http, https, socks5 or socks5h", c.UpstreamProxyURL)
	}
}

// ValidateServer checks the fields required to run the gateway.
func (c Config) ValidateServer() error {
	if c.UpstreamBaseURL == "" && c.UpstreamChatCompletionsURL == "" {
		return fmt.Errorf("upstream_base_url or upstream_chat_completions_url is required")
	}
	if _, err := c.ProxyURL(); err != nil {
		return err
	}
	if c.UpstreamChatCompletionsURL != "" {
		if err := validateUpstreamURL("upstream_chat_completions_url", c.UpstreamChatCompletionsURL, true); err != nil {
			return err
		}
	} else if err := validateUpstreamURL("upstream_base_url", c.UpstreamBaseURL, false); err != nil {
		return err
	}
	return nil
}

func validateUpstreamURL(name, raw string, allowQuery bool) error {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return fmt.Errorf("invalid %s %q: URL must use http or https", name, raw)
	}
	if parsed.User != nil || parsed.Fragment != "" || (!allowQuery && parsed.RawQuery != "") {
		return fmt.Errorf("invalid %s %q: user info, fragments and base URL query strings are not supported", name, raw)
	}
	return nil
}

// WriteTemplate writes the embedded template to path, refusing to touch an
// existing file.
func WriteTemplate(path string) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(Template); err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return err
	}
	return f.Close()
}

// Migrate appends settings that exist in the bundled template but are
// missing from the file at path, so configs written by older versions pick
// up newly introduced keys. Existing values (including unknown keys) and
// key order are preserved; the file is rewritten only when something was
// missing. It returns the JSON paths that were added.
func Migrate(path string) ([]string, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("migrate %s: config must be a regular file", path)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if !json.Valid(data) {
		return nil, fmt.Errorf("parse %s: invalid JSON", path)
	}
	var added []string
	merged := addMissing(data, gjson.ParseBytes(Template), "", &added)
	if len(added) == 0 {
		return nil, nil
	}
	merged = pretty.PrettyOptions(merged, &pretty.Options{Indent: "  "})
	if err := atomicWriteFile(path, merged, info.Mode().Perm()); err != nil {
		return nil, err
	}
	return added, nil
}

func atomicWriteFile(path string, data []byte, mode os.FileMode) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)

	if err := tmp.Chmod(mode); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	if dirHandle, err := os.Open(dir); err == nil {
		_ = dirHandle.Sync()
		_ = dirHandle.Close()
	}
	return nil
}

// addMissing walks the template object and appends every key absent from
// dst, recursing into objects present on both sides.
func addMissing(dst []byte, tmpl gjson.Result, prefix string, added *[]string) []byte {
	tmpl.ForEach(func(key, value gjson.Result) bool {
		path := key.String()
		if prefix != "" {
			path = prefix + "." + path
		}
		existing := gjson.GetBytes(dst, path)
		switch {
		case !existing.Exists():
			if withKey, err := sjson.SetRawBytes(dst, path, []byte(value.Raw)); err == nil {
				dst = withKey
				*added = append(*added, path)
			}
		case value.IsObject() && existing.IsObject():
			dst = addMissing(dst, value, path, added)
		}
		return true
	})
	return dst
}
