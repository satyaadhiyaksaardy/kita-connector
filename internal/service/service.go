// Package service installs the connector as an OS service (Windows service,
// launchd daemon, systemd unit) so it starts with the computer.
package service

import (
	"context"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	ks "github.com/kardianos/service"

	"github.com/satyaadhiyaksaardy/kita-connector/internal/config"
)

const name = "kita-connector"

// Interactive is false when the OS service manager started this process.
func Interactive() bool { return ks.Interactive() }

// Run runs fn until Ctrl+C (in a terminal) or until the service manager
// stops it. Under the service manager, if fn returns early (e.g. key
// invalid) the process stays up idle instead of being restarted in a loop.
func Run(fn func(context.Context) error) error {
	if ks.Interactive() {
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		return fn(ctx)
	}
	s, err := ks.New(&program{fn: fn}, &ks.Config{Name: name})
	if err != nil {
		return err
	}
	return s.Run()
}

type program struct {
	fn     func(context.Context) error
	cancel context.CancelFunc
	done   chan struct{}
}

func (p *program) Start(ks.Service) error {
	ctx, cancel := context.WithCancel(context.Background())
	p.cancel, p.done = cancel, make(chan struct{})
	go func() { defer close(p.done); _ = p.fn(ctx) }()
	return nil
}

func (p *program) Stop(ks.Service) error {
	p.cancel()
	select {
	case <-p.done:
	case <-time.After(15 * time.Second):
	}
	return nil
}

// Install registers `kita-connector run` with this config's data dir and
// brain (not started: see Start). Needs administrator / root.
func Install(cfg config.Config) error {
	c := &ks.Config{
		Name:        name,
		DisplayName: "kita-connector",
		Description: "Forwards $ messages from one WhatsApp chat to kita-finance.",
		Arguments:   []string{"run", "--data", cfg.Data, "--brain", cfg.Brain, "--log-level", cfg.LogLevel},
		Option: ks.KeyValue{
			"RunAtLoad":              true,      // launchd: start at boot
			"OnFailure":              "restart", // windows
			"OnFailureDelayDuration": "10s",
		},
	}
	if runtime.GOOS != "windows" {
		// run as the user whose session it is, not root (sudo install)
		c.UserName = os.Getenv("SUDO_USER")
	}
	if runtime.GOOS == "linux" {
		c.Dependencies = []string{"After=network-online.target", "Wants=network-online.target"}
	}
	s, err := ks.New(&program{}, c)
	if err != nil {
		return err
	}
	return s.Install()
}

func Uninstall() error {
	s, err := control()
	if err != nil {
		return err
	}
	_ = s.Stop()
	return s.Uninstall()
}

// Status: "running", "stopped", "not installed" or "unknown". Works
// without administrator rights.
func Status() string { return statusOf(name) }

func Start() error {
	s, err := control()
	if err != nil {
		return err
	}
	return s.Start()
}

func Stop() error {
	s, err := control()
	if err != nil {
		return err
	}
	return s.Stop()
}

func control() (ks.Service, error) { return ks.New(&program{}, &ks.Config{Name: name}) }
