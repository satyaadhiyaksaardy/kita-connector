//go:build !windows

package service

import (
	"errors"

	ks "github.com/kardianos/service"
)

func statusOf(name string) string {
	s, err := ks.New(&program{}, &ks.Config{Name: name})
	if err != nil {
		return "unknown"
	}
	st, err := s.Status()
	switch {
	case errors.Is(err, ks.ErrNotInstalled):
		return "not installed"
	case st == ks.StatusRunning:
		return "running"
	case st == ks.StatusStopped:
		return "stopped"
	}
	return "unknown"
}
