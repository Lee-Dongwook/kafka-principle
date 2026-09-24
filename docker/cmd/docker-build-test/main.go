// docker-build-test builds a Kafka container image from a Kafka distribution.
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

func run(args []string) error {
	flags := flag.NewFlagSet("docker-build-test", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)

	tag := flags.String("image-tag", "latest", "image tag")
	imageType := flags.String("image-type", "native", "image type (native)")
	kafkaURL := flags.String("kafka-url", "", "Kafka distribution tarball URL")
	kafkaArchive := flags.String("kafka-archive", "", "path to a local Kafka distribution tarball")
	sourceDir := flags.String("source-dir", ".", "path to the docker source directory")
	buildOnly := flags.Bool("build", false, "build only")
	testOnly := flags.Bool("test", false, "verify an existing image only")
	if err := flags.Parse(args); err != nil {
		return err
	}

	if flags.NArg() != 1 {
		return errors.New("exactly one image name is required")
	}
	if *kafkaURL == "" && *kafkaArchive == "" {
		return errors.New("one of --kafka-url or --kafka-archive is required")
	}
	if *kafkaURL != "" && *kafkaArchive != "" {
		return errors.New("--kafka-url and --kafka-archive cannot be used together")
	}
	if *imageType != "native" {
		return fmt.Errorf("unsupported image type %q: this project currently provides native only", *imageType)
	}

	absoluteSourceDir, err := filepath.Abs(*sourceDir)
	if err != nil {
		return fmt.Errorf("resolve source directory: %w", err)
	}

	runtime, err := containerutil.DetectContainerRuntime()
	if err != nil {
		return err
	}
	image := fmt.Sprintf("%s:%s", flags.Arg(0), *tag)

	if !*testOnly {
		command := []string{
			runtime, "build", "-f", "$DOCKER_FILE", "-t", image,
			"--build-arg", "build_date=" + time.Now().Format("2006-01-02"), "--no-cache",
		}
		if *kafkaURL != "" {
			command = append(command, "--build-arg", "kafka_url="+*kafkaURL)
		}
		command = append(command, "$DOCKER_DIR")

		if err := containerutil.BuildDockerImageRunner(command, absoluteSourceDir, *imageType, *kafkaArchive); err != nil {
			return err
		}
	}

	if !*buildOnly {
		if err := containerutil.Execute([]string{runtime, "image", "inspect", image}); err != nil {
			return fmt.Errorf("verify built image %q: %w", image, err)
		}
		fmt.Println("Basic image verification passed:", image)
	}

	return nil
}
