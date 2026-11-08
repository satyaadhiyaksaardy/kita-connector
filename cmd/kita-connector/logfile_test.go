package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestLogRotation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "connector.log")
	l, err := openLog(path)
	if err != nil {
		t.Fatal(err)
	}
	line := append(bytes.Repeat([]byte("x"), 1023), '\n')
	for range 4 * logMax / len(line) { // 4 files' worth
		if _, err := l.Write(line); err != nil {
			t.Fatal(err)
		}
	}
	l.Close()
	for i, name := range []string{path, path + ".1", path + ".2"} {
		st, err := os.Stat(name)
		if err != nil || st.Size() > logMax || (i > 0 && st.Size() < logMax-int64(len(line))) {
			t.Fatalf("%s: %v %v", name, st, err)
		}
	}
	if _, err := os.Stat(path + ".3"); !os.IsNotExist(err) {
		t.Fatal("kept more than 3 files")
	}
	// reopening appends to the current file
	other := filepath.Join(t.TempDir(), "other.log")
	for range 2 {
		l, _ = openLog(other)
		l.Write(line)
		l.Close()
	}
	if st, _ := os.Stat(other); st.Size() != 2*int64(len(line)) {
		t.Fatal("reopen did not append")
	}
}
