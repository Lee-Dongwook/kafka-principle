// docker-release builds and pushes a multi-architecture Kafka native image.
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/Lee-Dongwook/kafka-principle/docker/internal/containerutil"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(args []string) (err error) {
	flags := flag.NewFlagSet("docker-release", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)

	imageType := flags.String("image-type", "native", "image type (native)")
	kafkaURL := flags.String("kafka-url", "", "Kafka distribution tarball URL")
	sourceDir := flags.String("source-dir", ".", "path to the docker source directory")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 1 {
		return errors.New("exactly one destination image is required")
	}
	if *kafkaURL == "" {
		return errors.New("--kafka-url is required")
	}
	if *imageType != "native" {
		return fmt.Errorf("unsupported image type %q: this project currently provides native only", *imageType)
	}

	absoluteSourceDir, err := filepath.Abs(*sourceDir)
	if err != nil {
		return fmt.Errorf("resolve source directory: %w", err)
	}

	builderName := "kafka-builder"
	if err := containerutil.Execute([]string{"docker", "buildx", "create", "--name", builderName, "--use"}); err != nil {
		return fmt.Errorf("create buildx builder: %w", err)
	}
	defer func() {
		cleanupErr := containerutil.Execute([]string{"docker", "buildx", "rm", builderName})
		if cleanupErr != nil {
			err = errors.Join(err, fmt.Errorf("remove buildx builder: %w", cleanupErr))
		}
	}()

	command := []string{
		"docker", "buildx", "build", "-f", "$DOCKER_FILE",
		"--build-arg", "kafka_url=" + *kafkaURL,
		"--build-arg", "build_date=" + time.Now().Format("2006-01-02"),
		"--push", "--platform", "linux/amd64,linux/arm64", "--tag", flags.Arg(0), "$DOCKER_DIR",
	}
	if err := containerutil.BuildDockerImageRunner(command, absoluteSourceDir, *imageType, ""); err != nil {
		return fmt.Errorf("build and push image: %w", err)
	}

	return nil
}
