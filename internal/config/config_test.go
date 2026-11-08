package config

import (
	"crypto/rand"
	"encoding/base64"
	"path/filepath"
	"testing"
)

func TestDataDirPerOS(t *testing.T) {
	env := map[string]string{"APPDATA": `C:\Users\a\AppData\Roaming`}
	get := func(k string) string { return env[k] }
	cases := []struct{ goos, home, want string }{
		{"windows", "", filepath.Join(`C:\Users\a\AppData\Roaming`, "kita-connector")},
		{"darwin", "/Users/a", filepath.Join("/Users/a", "Library", "Application Support", "kita-connector")},
		{"linux", "/home/a", filepath.Join("/home/a", ".local", "share", "kita-connector")},
	}
	for _, c := range cases {
		if got := dataDir(c.goos, get, c.home); got != c.want {
			t.Errorf("%s: %s, want %s", c.goos, got, c.want)
		}
	}
	env["XDG_DATA_HOME"] = "/data"
	if got := dataDir("linux", get, "/home/a"); got != filepath.Join("/data", "kita-connector") {
		t.Errorf("linux XDG: %s", got)
	}
}

func TestCheckBrain(t *testing.T) {
	for url, ok := range map[string]bool{
		"wss://kita.example/connector":      true,
		"ws://127.0.0.1:8080/connector":     true,
		"ws://localhost:8080/connector":     true,
		"ws://[::1]:8080/connector":         true,
		"ws://kita.example/connector":       false,
		"ws://10.0.0.5/connector":           false,
		"ws://127.0.0.1.evil.com/connector": false,
		"https://kita.example/connector":    false,
		"":                                  false,
	} {
		if err := CheckBrain(url); (err == nil) != ok {
			t.Errorf("%q: err = %v, want ok=%v", url, err, ok)
		}
	}
}

func TestKeyFile(t *testing.T) {
	c := &Config{Data: filepath.Join(t.TempDir(), "kc")}
	if k, err := c.LoadKey(); k != "" || err != nil {
		t.Fatalf("no key file: %q, %v", k, err)
	}
	b := make([]byte, 32)
	rand.Read(b)
	key := base64.RawURLEncoding.EncodeToString(b)
	if err := c.SaveKey(key); err != nil {
		t.Fatal(err)
	}
	if k, err := c.LoadKey(); k != key || err != nil {
		t.Fatalf("saved key: %q, %v", k, err)
	}
	if CheckKey(key+"=") != nil || CheckKey("short") == nil || CheckKey(key[:40]) == nil {
		t.Fatal("CheckKey")
	}
}
