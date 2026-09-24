package main

import "testing"

func TestRunRejectsInvalidArgumentsBeforeCreatingBuilder(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		args []string
	}{
		{name: "missing image", args: []string{"--kafka-url", "https://example.test/kafka.tgz"}},
		{name: "missing Kafka URL", args: []string{"registry.example/kafka:1"}},
		{name: "unsupported image type", args: []string{"registry.example/kafka:1", "--image-type", "jvm", "--kafka-url", "https://example.test/kafka.tgz"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := run(test.args); err == nil {
				t.Fatal("run() error = nil, want error")
			}
		})
	}
}
