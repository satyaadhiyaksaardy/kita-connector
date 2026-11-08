package main

import (
	"os"
	"sync"
)

const (
	logMax  = 5 << 20 // bytes per file
	logKeep = 3       // connector.log, .1, .2
)

// logFile is an append-only file rotated at logMax, keeping logKeep files.
type logFile struct {
	mu   sync.Mutex
	path string
	f    *os.File
	size int64
}

func openLog(path string) (*logFile, error) {
	l := &logFile{path: path}
	return l, l.open()
}

func (l *logFile) open() error {
	f, err := os.OpenFile(l.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}
	l.f, l.size = f, st.Size()
	return nil
}

func (l *logFile) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.size > 0 && l.size+int64(len(p)) > logMax {
		l.f.Close()
		for i := logKeep - 1; i > 0; i-- {
			_ = os.Rename(l.name(i-1), l.name(i)) // .1 → .2 overwrites the oldest
		}
		if err := l.open(); err != nil {
			return 0, err
		}
	}
	n, err := l.f.Write(p)
	l.size += int64(n)
	return n, err
}

func (l *logFile) name(i int) string {
	if i == 0 {
		return l.path
	}
	return l.path + "." + string(rune('0'+i))
}

func (l *logFile) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.f.Close()
}
