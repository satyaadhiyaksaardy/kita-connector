// Package config: flags, env, per-OS defaults, key file.
package config

import (
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"strings"
)

// DefaultBrain is baked in at build time:
//
//	-ldflags "-X github.com/satyaadhiyaksaardy/kita-connector/internal/config.DefaultBrain=wss://<app domain>/connector"
var DefaultBrain = ""

type Config struct {
	Brain, Key, Data, Mode, LogLevel string
}

// Flags registers the shared flags on fs; env vars give the defaults.
func (c *Config) Flags(fs *flag.FlagSet) {
	fs.StringVar(&c.Brain, "brain", env("KITA_BRAIN", DefaultBrain), "brain WebSocket URL")
	fs.StringVar(&c.Key, "key", os.Getenv("KITA_KEY"), "connector key (default: read from <data>/key)")
	fs.StringVar(&c.Data, "data", env("KITA_DATA", DefaultData()), "data directory")
	fs.StringVar(&c.Mode, "mode", env("KITA_MODE", "self"), "self | hosted")
	fs.StringVar(&c.LogLevel, "log-level", env("KITA_LOG", "info"), "debug | info | warn | error")
}

func env(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func (c *Config) Check() error {
	if err := CheckBrain(c.Brain); err != nil {
		return err
	}
	if c.Mode != "self" && c.Mode != "hosted" {
		return fmt.Errorf("--mode must be self or hosted, not %q", c.Mode)
	}
	if _, err := c.Level(); err != nil {
		return fmt.Errorf("--log-level: %w", err)
	}
	if !filepath.IsAbs(c.Data) {
		return fmt.Errorf("--data must be an absolute path, not %q", c.Data)
	}
	return nil
}

func (c *Config) Level() (slog.Level, error) {
	var l slog.Level
	err := l.UnmarshalText([]byte(c.LogLevel))
	return l, err
}

// CheckBrain accepts wss://, and ws:// only to a loopback host (hosted mode).
func CheckBrain(raw string) error {
	if raw == "" {
		return errors.New("no brain URL: pass --brain or set KITA_BRAIN")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("brain URL: %w", err)
	}
	switch u.Scheme {
	case "wss":
		return nil
	case "ws":
		host := u.Hostname()
		if ip := net.ParseIP(host); host == "localhost" || (ip != nil && ip.IsLoopback()) {
			return nil
		}
		return fmt.Errorf("refusing unencrypted ws:// to %s: use wss://", host)
	}
	return fmt.Errorf("brain URL must start with wss://, not %q", raw)
}

// DefaultData is the per-OS data directory. Under sudo it is the invoking
// user's, so `sudo kita-connector install-service` points at their session.
func DefaultData() string {
	home, _ := os.UserHomeDir()
	if runtime.GOOS != "windows" && os.Geteuid() == 0 {
		if name := os.Getenv("SUDO_USER"); name != "" {
			if u, err := user.Lookup(name); err == nil {
				home = u.HomeDir
			} else if runtime.GOOS == "darwin" {
				home = "/Users/" + name
			} else {
				home = "/home/" + name
			}
		}
	}
	return dataDir(runtime.GOOS, os.Getenv, home)
}

func dataDir(goos string, getenv func(string) string, home string) string {
	switch goos {
	case "windows":
		return filepath.Join(getenv("APPDATA"), "kita-connector")
	case "darwin":
		return filepath.Join(home, "Library", "Application Support", "kita-connector")
	}
	if x := getenv("XDG_DATA_HOME"); x != "" {
		return filepath.Join(x, "kita-connector")
	}
	return filepath.Join(home, ".local", "share", "kita-connector")
}

func (c *Config) Path(name string) string { return filepath.Join(c.Data, name) }

// EnsureData creates the data directory, owner-only.
func (c *Config) EnsureData() error {
	if err := os.MkdirAll(c.Data, 0o700); err != nil {
		return err
	}
	return os.Chmod(c.Data, 0o700)
}

// LoadKey returns --key / KITA_KEY, else the saved key file ("" if none).
func (c *Config) LoadKey() (string, error) {
	if c.Key != "" {
		return c.Key, CheckKey(c.Key)
	}
	b, err := os.ReadFile(c.Path("key"))
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	k := strings.TrimSpace(string(b))
	if err == nil {
		err = CheckKey(k)
	}
	return k, err
}

func (c *Config) SaveKey(k string) error {
	if err := c.EnsureData(); err != nil {
		return err
	}
	if err := os.WriteFile(c.Path("key"), []byte(k+"\n"), 0o600); err != nil {
		return err
	}
	return os.Chmod(c.Path("key"), 0o600)
}

// CheckKey: keys are 32 random bytes, base64url (PROTOCOL.md → Security model).
func CheckKey(k string) error {
	b, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(k, "="))
	if err != nil || len(b) != 32 {
		return errors.New("that doesn't look like a connector key (copy it again from the dashboard)")
	}
	return nil
}
