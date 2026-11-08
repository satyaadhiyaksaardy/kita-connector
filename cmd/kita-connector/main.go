// Command kita-connector links one WhatsApp account and forwards only the
// `$` messages from the couple's chat to a kita-finance brain.
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/satyaadhiyaksaardy/kita-connector/internal/app"
	"github.com/satyaadhiyaksaardy/kita-connector/internal/config"
	"github.com/satyaadhiyaksaardy/kita-connector/internal/filter"
	"github.com/satyaadhiyaksaardy/kita-connector/internal/outbox"
	"github.com/satyaadhiyaksaardy/kita-connector/internal/service"
	"github.com/satyaadhiyaksaardy/kita-connector/internal/wa"
)

var version = "dev" // -ldflags "-X main.version=1.2.3"

const usage = `kita-connector — forwards $ messages from one WhatsApp chat to kita-finance.

  kita-connector                    first run: key → link WhatsApp → install as a service
  kita-connector login [--code +62…] link WhatsApp (QR, or a pairing code for that number)
  kita-connector run                run in the foreground (what the service runs)
  kita-connector status             linked? connected to the brain? queue length?
  kita-connector logout             unlink from WhatsApp and wipe the data directory
  kita-connector install-service    start with the computer (administrator / sudo)
  kita-connector uninstall-service
  kita-connector version

Shared flags: --brain --key --data --mode --log-level (env KITA_BRAIN, KITA_KEY, KITA_DATA, KITA_MODE, KITA_LOG)
`

var stdin = bufio.NewReader(os.Stdin)

func main() {
	cmd, args := "", os.Args[1:]
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		cmd, args = args[0], args[1:]
	}
	var err error
	switch cmd {
	case "":
		err = firstRun(args)
	case "login":
		err = login(args)
	case "run":
		err = run(args)
	case "status":
		err = status(args)
	case "logout":
		err = logout(args)
	case "install-service":
		err = installService(args)
	case "uninstall-service":
		err = service.Uninstall()
	case "version":
		fmt.Println(version)
	case "help":
		fmt.Print(usage)
	default:
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "kita-connector:", err)
		if cmd == "" { // double-clicked: keep the window open to read it
			fmt.Print("Press Enter to close.")
			stdin.ReadString('\n')
		}
		os.Exit(1)
	}
}

func parse(name string, args []string, extra func(*flag.FlagSet)) (config.Config, error) {
	var c config.Config
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	c.Flags(fs)
	if extra != nil {
		extra(fs)
	}
	if err := fs.Parse(args); err != nil {
		return c, err
	}
	c.Data, _ = filepath.Abs(c.Data)
	return c, c.Check()
}

// selfOnly refuses the self-hosting commands for a hosted connector: the
// brain's operator runs those (`run --mode hosted`).
func selfOnly(c config.Config) error {
	if c.Mode == "hosted" {
		return errors.New("hosted connectors are run by the kita brain: `kita-connector run --mode hosted`")
	}
	return nil
}

// logger: JSON lines to <data>/connector.log (3 × 5 MB), or to stdout in
// hosted mode; in a terminal (self), warnings and errors also go to
// stderr. Never message text (see wa, app).
func logger(c config.Config, terminal bool) (*slog.Logger, io.Closer, error) {
	if err := c.EnsureData(); err != nil {
		return nil, nil, err
	}
	var w io.Writer = os.Stdout
	var closer io.Closer = io.NopCloser(nil) // stdout stays open
	if c.Mode != "hosted" {
		f, err := openLog(c.Path("connector.log"))
		if err != nil {
			return nil, nil, err
		}
		w, closer = f, f
	}
	lvl, _ := c.Level()
	var h slog.Handler = slog.NewJSONHandler(w, &slog.HandlerOptions{Level: lvl, ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
		if a.Key == slog.TimeKey && len(groups) == 0 {
			a.Key = "ts"
		}
		return a
	}})
	if terminal && c.Mode != "hosted" {
		h = slog.NewMultiHandler(h, slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	}
	return slog.New(h), closer, nil
}

func firstRun(args []string) error {
	c, err := parse("kita-connector", args, nil)
	if err == nil {
		err = selfOnly(c)
	}
	if err != nil {
		return err
	}
	if service.Status() == "running" {
		fmt.Println("kita-connector is already running as a service. See `kita-connector status`.")
		fmt.Print("Press Enter to close.")
		stdin.ReadString('\n')
		return nil
	}
	return setup(c, "")
}

func login(args []string) error {
	var code string
	c, err := parse("login", args, func(fs *flag.FlagSet) {
		fs.StringVar(&code, "code", "", "link with a pairing code for this phone number (+62…) instead of a QR")
	})
	if err == nil {
		err = selfOnly(c)
	}
	if err != nil {
		return err
	}
	return setup(c, code)
}

// setup: key → link WhatsApp (if needed) → offer the service. The
// connector runs fully meanwhile, so the history sync after linking is
// filtered and forwarded.
func setup(c config.Config, code string) error {
	key, err := askKey(&c)
	if err != nil {
		return err
	}
	log, closeLog, err := logger(c, true)
	if err != nil {
		return err
	}
	defer closeLog.Close()

	svc := service.Status()
	if svc == "running" {
		fmt.Println("Stopping the kita-connector service while linking…")
		if err := service.Stop(); err != nil {
			return fmt.Errorf("stopping the service: %w (%s)", err, adminHint("login"))
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	opened := make(chan struct{}, 1)
	a := &app.App{Cfg: c, Key: key, Version: version, Log: log, Term: os.Stdout, Pair: true, PairPhone: code,
		OnOpen: func() {
			select {
			case opened <- struct{}{}:
			default:
			}
		}}
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	fmt.Println("Connecting to kita…")
	select {
	case err := <-done:
		return err
	case <-opened:
	}

	handOver := func() error {
		cancel()
		<-done
		return service.Start()
	}
	if svc == "running" || svc == "stopped" {
		if err := handOver(); err != nil {
			return fmt.Errorf("starting the service: %w", err)
		}
		fmt.Println("Done. The kita-connector service is running again.")
		return nil
	}
	if ask("Install as a service so it starts with the computer? [Y/n] ") {
		if err := service.Install(c); err != nil {
			fmt.Printf("Could not install the service: %v\n%s\n", err, adminHint("install-service"))
		} else if err := handOver(); err != nil {
			return fmt.Errorf("starting the service: %w", err)
		} else {
			fmt.Println("Installed. kita-connector now runs in the background; you can close this window.")
			return nil
		}
	}
	fmt.Println("Running in this window: keep it open (Ctrl+C stops it).")
	return <-done
}

// askKey returns the key from --key / KITA_KEY / <data>/key, or asks for it
// once and saves it.
func askKey(c *config.Config) (string, error) {
	k, err := c.LoadKey()
	if err != nil && c.Key != "" {
		return "", err
	}
	if err == nil && k != "" {
		return k, c.SaveKey(k)
	}
	for {
		fmt.Print("Paste the connector key from the kita dashboard: ")
		line, rerr := stdin.ReadString('\n')
		k = strings.TrimSpace(line)
		if err := config.CheckKey(k); err == nil {
			return k, c.SaveKey(k)
		} else if rerr != nil {
			return "", errors.New("no connector key")
		} else {
			fmt.Println(err)
		}
	}
}

func ask(q string) bool {
	fmt.Print(q)
	line, _ := stdin.ReadString('\n')
	a := strings.ToLower(strings.TrimSpace(line))
	return a == "" || a == "y" || a == "yes"
}

func adminHint(cmd string) string {
	if runtime.GOOS == "windows" {
		return "Run it from a terminal opened with \"Run as administrator\": kita-connector " + cmd
	}
	return "Run it with sudo: sudo kita-connector " + cmd
}

func run(args []string) error {
	c, err := parse("run", args, nil)
	if err != nil {
		return err
	}
	terminal := service.Interactive() && c.Mode != "hosted" // hosted: a process manager, no one at a terminal
	log, closeLog, err := logger(c, terminal)
	if err != nil {
		return err
	}
	defer closeLog.Close()
	return service.Run(func(ctx context.Context) error {
		key, err := c.LoadKey()
		if err == nil && key == "" {
			err = errors.New("no connector key: run `kita-connector login`")
		}
		if err != nil {
			log.Error(err.Error(), "event", "no_key")
			return err
		}
		a := &app.App{Cfg: c, Key: key, Version: version, Log: log, Pair: terminal}
		if terminal {
			a.Term = os.Stdout
		}
		return a.Run(ctx)
	})
}

func status(args []string) error {
	c, err := parse("status", args, nil)
	if err != nil {
		return err
	}
	s, err := app.ReadStatus(c)
	if err != nil {
		fmt.Println("Not set up yet: run kita-connector to start.")
		return nil
	}
	linked := "no: run `kita-connector login`"
	if s.WhatsApp == "open" || s.WhatsApp == "closed" {
		linked = "yes (" + s.WhatsApp + ")"
	}
	queue := "?"
	if ob, err := outbox.Open(c.Path("outbox.db")); err == nil {
		if n, err := ob.Len(); err == nil {
			queue = fmt.Sprint(n)
		}
		ob.Close()
	}
	fmt.Printf("WhatsApp linked:  %s\n", linked)
	fmt.Printf("Brain:            %s\n", s.Brain)
	fmt.Printf("Waiting to send:  %s message(s)\n", queue)
	fmt.Printf("Service:          %s\n", service.Status())
	fmt.Printf("Version:          %s\n", version)
	if s.Latest != "" && s.Latest != version {
		fmt.Printf("Update available: %s\n", s.Latest)
	}
	fmt.Printf("As of:            %s\n", s.Updated.Local().Format(time.DateTime))
	return nil
}

func logout(args []string) error {
	c, err := parse("logout", args, nil)
	if err != nil {
		return err
	}
	if _, err := os.Stat(c.Data); err != nil {
		fmt.Println("Nothing to log out from.")
		return nil
	}
	svc := service.Status()
	if svc == "running" {
		if err := service.Stop(); err != nil {
			return fmt.Errorf("stopping the service: %w (%s)", err, adminHint("logout"))
		}
	}
	if err := unlink(c); err != nil {
		fmt.Printf("Could not reach WhatsApp to unlink (%v).\nRemove \"kita-connector\" from WhatsApp → Linked devices on your phone.\n", err)
	}
	if svc == "running" || svc == "stopped" {
		if err := service.Uninstall(); err != nil {
			fmt.Printf("Could not remove the service: %v\n%s\n", err, adminHint("uninstall-service"))
		}
	}
	if err := os.RemoveAll(c.Data); err != nil {
		return err
	}
	fmt.Println("Logged out. Removed", c.Data)
	return nil
}

// unlink connects the saved session just long enough to remove this device.
func unlink(c config.Config) error {
	log, closeLog, err := logger(c, true)
	if err != nil {
		return err
	}
	defer closeLog.Close() // before the data dir is wiped
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	w, err := wa.Open(ctx, c.Path("session.db"), log)
	if err != nil {
		return err
	}
	defer w.Close()
	if !w.Linked() {
		return nil
	}
	open := make(chan struct{}, 1)
	w.OnState = func(s wa.State) {
		if s.State == "open" {
			select {
			case open <- struct{}{}:
			default:
			}
		}
	}
	w.OnMessage = func(filter.Out) {} // SetPartner never called: nothing passes
	if err := w.Connect(); err != nil {
		return err
	}
	select {
	case <-open:
	case <-ctx.Done():
		return ctx.Err()
	}
	return w.Logout(ctx)
}

func installService(args []string) error {
	c, err := parse("install-service", args, nil)
	if err == nil {
		err = selfOnly(c)
	}
	if err != nil {
		return err
	}
	if k, err := c.LoadKey(); err != nil || k == "" {
		return errors.New("not set up yet: run `kita-connector login` first")
	}
	if err := service.Install(c); err != nil {
		return fmt.Errorf("%w (%s)", err, adminHint("install-service"))
	}
	if err := service.Start(); err != nil {
		return err
	}
	fmt.Println("Installed and started. kita-connector now starts with the computer.")
	return nil
}
