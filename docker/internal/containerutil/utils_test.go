package containerutil

import (
	"os"
	"path/filepath"
	"testing"
)

func TestBuildDockerImageRunnerRejectsInvalidInput(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name      string
		command   []string
		imageType string
	}{
		{name: "empty command", imageType: "native"},
		{name: "empty image type", command: []string{"true"}},
		{name: "path traversal", command: []string{"true"}, imageType: "../native"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := BuildDockerImageRunner(test.command, t.TempDir(), test.imageType, ""); err == nil {
				t.Fatal("BuildDockerImageRunner() error = nil, want error")
			}
		})
	}
}

func TestCopyTreeCopiesModesAndRejectsSymbolicLinks(t *testing.T) {
	t.Parallel()

	sourceDir := t.TempDir()
	targetDir := filepath.Join(t.TempDir(), "target")
	executable := filepath.Join(sourceDir, "run")
	if err := os.WriteFile(executable, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	if err := copyTree(sourceDir, targetDir); err != nil {
		t.Fatalf("copyTree() error = %v", err)
	}
	info, err := os.Stat(filepath.Join(targetDir, "run"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o755 {
		t.Fatalf("copied mode = %o, want 755", info.Mode().Perm())
	}

	link := filepath.Join(sourceDir, "link")
	if err := os.Symlink(executable, link); err != nil {
		t.Fatal(err)
	}
	if err := copyTree(sourceDir, filepath.Join(t.TempDir(), "with-link")); err == nil {
		t.Fatal("copyTree() error = nil, want symbolic-link error")
	}
}

func TestCopyFileRejectsDirectory(t *testing.T) {
	t.Parallel()

	err := copyFile(t.TempDir(), filepath.Join(t.TempDir(), "target"))
	if err == nil {
		t.Fatalf("copyFile() error = %v, want non-nil error", err)
	}
}
