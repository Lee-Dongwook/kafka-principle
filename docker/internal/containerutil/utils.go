package containerutil

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

var supportedContainerRuntimes = [...]string{"docker", "podman"}

func Execute(command []string) error {
	if len(command) == 0 || command[0] == "" {
		return errors.New("command cannot be empty")
	}

	cmd := exec.Command(command[0], command[1:]...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to execute %q: %w", command, err)
	}

	return nil
}

func GetInput(reader *bufio.Reader, message string) (string, error) {
	if _, err := fmt.Fprint(os.Stdout, message); err != nil {
		return "", err
	}

	value, err := reader.ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return "", err
	}

	if errors.Is(err, io.EOF) && value == "" {
		return "", io.EOF
	}

	value = strings.TrimSuffix(value, "\n")
	value = strings.TrimSuffix(value, "\r")

	if value == "" {
		return "", errors.New("this field cannot be empty")
	}

	return value, nil
}

func BuildDockerImageRunner(
	command []string,
	sourceDir string,
	imageType string,
	kafkaArchive string,
) (err error) {
	tempDir, err := os.MkdirTemp("", "kafka-image-build-")
	if err != nil {
		return fmt.Errorf("create temporary directory: %w", err)
	}

	defer func() {
		if cleanupErr := os.RemoveAll(tempDir); cleanupErr != nil {
			err = errors.Join(
				err,
				fmt.Errorf("remove temporary directory: %w", cleanupErr),
			)
		}
	}()

	buildDir := filepath.Join(tempDir, imageType)

	if err := copyTree(
		filepath.Join(sourceDir, imageType),
		buildDir,
	); err != nil {
		return fmt.Errorf("copy image directory: %w", err)
	}

	if err := copyTree(
		filepath.Join(sourceDir, "resources"),
		filepath.Join(buildDir, "resources"),
	); err != nil {
		return fmt.Errorf("copy resources: %w", err)
	}

	if err := copyFile(
		filepath.Join(sourceDir, "server.properties"),
		filepath.Join(buildDir, "server.properties"),
	); err != nil {
		return fmt.Errorf("copy server.properties: %w", err)
	}

	archivePath := filepath.Join(buildDir, "kafka.tgz")

	if kafkaArchive != "" {
		if err := copyFile(kafkaArchive, archivePath); err != nil {
			return fmt.Errorf("copy Kafka archive: %w", err)
		}
	} else {
		file, err := os.OpenFile(
			archivePath,
			os.O_CREATE|os.O_WRONLY,
			0o666,
		)
		if err != nil {
			return fmt.Errorf("create Kafka archive placeholder: %w", err)
		}
		if err := file.Close(); err != nil {
			return fmt.Errorf("close Kafka archive placeholder: %w", err)
		}
	}

	replacer := strings.NewReplacer(
		"$DOCKER_FILE", filepath.Join(buildDir, "Dockerfile"),
		"$DOCKER_DIR", buildDir,
	)

	args := make([]string, len(command))
	for i, arg := range command {
		args[i] = replacer.Replace(arg)
	}

	if err := Execute(args); err != nil {
		return fmt.Errorf("container image build failed: %w", err)
	}

	return nil
}

func DetectContainerRuntime() (string, error) {
	configured := os.Getenv("CONTAINER_RUNTIME")

	if configured != "" {
		supported := false
		for _, runtime := range supportedContainerRuntimes {
			if configured == runtime {
				supported = true
				break
			}
		}

		if !supported {
			return "", fmt.Errorf(
				"unsupported container runtime: %s. Supported runtimes: %s",
				configured,
				strings.Join(supportedContainerRuntimes[:], ", "),
			)
		}

		if _, err := exec.LookPath(configured); err != nil {
			return "", fmt.Errorf(
				"container runtime %q was not found: %w",
				configured,
				err,
			)
		}

		return configured, nil
	}

	for _, runtime := range supportedContainerRuntimes {
		if _, err := exec.LookPath(runtime); err == nil {
			return runtime, nil
		}
	}

	return "", errors.New(
		"no supported container runtime found: " +
			"please install Docker or Podman, or set CONTAINER_RUNTIME",
	)
}

func copyTree(src, dst string) error {
	info, err := os.Stat(src)
	if err != nil {
		return err
	}

	if !info.IsDir() {
		return fmt.Errorf("%q is not a directory", src)
	}

	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(dst, 0o700); err != nil {
		return err
	}
	if err := os.Chmod(dst, 0o700); err != nil {
		return err
	}

	for _, entry := range entries {
		sourcePath := filepath.Join(src, entry.Name())
		targetPath := filepath.Join(dst, entry.Name())

		entryInfo, err := os.Stat(sourcePath)
		if err != nil {
			return err
		}

		if entryInfo.IsDir() {
			err = copyTree(sourcePath, targetPath)
		} else {
			err = copyFile(sourcePath, targetPath)
		}

		if err != nil {
			return fmt.Errorf("copy %q: %w", sourcePath, err)
		}
	}

	return os.Chmod(dst, info.Mode().Perm())
}

func copyFile(src, dst string) (err error) {
	source, err := os.Open(src)
	if err != nil {
		return err
	}
	defer source.Close()

	info, err := source.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("%q is not a regular file", src)
	}

	target, err := os.OpenFile(
		dst,
		os.O_CREATE|os.O_WRONLY|os.O_TRUNC,
		info.Mode().Perm(),
	)
	if err != nil {
		return err
	}

	defer func() {
		err = errors.Join(err, target.Close())
	}()

	if _, err := io.Copy(target, source); err != nil {
		return err
	}

	return target.Chmod(info.Mode().Perm())
}
