package main

import "testing"

func TestRunRejectsInvalidArgumentsBeforeUsingContainerRuntime(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		args []string
	}{
		{name: "missing image", args: []string{"--kafka-url", "https://example.test/kafka.tgz"}},
		{name: "missing source", args: []string{"image"}},
		{name: "both sources", args: []string{"image", "--kafka-url", "https://example.test/kafka.tgz", "--kafka-archive", "/tmp/kafka.tgz"}},
		{name: "unsupported image type", args: []string{"image", "--image-type", "jvm", "--kafka-url", "https://example.test/kafka.tgz"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := run(test.args); err == nil {
				t.Fatal("run() error = nil, want error")
			}
		})
	}
}
