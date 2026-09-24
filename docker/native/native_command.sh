#!/usr/bin/env bash

$1 --no-fallback \
  --enable-http \
  --enable-https \
  --allow-incomplete-classpath \
  --report-unsupported-elements-at-runtime \
  --install-exit-handlers \
  --enable-monitoring=jmxserver,jmxclient,heapdump,jvmstat \
  -H:+ReportExceptionStackTraces \
  -H:+EnableAllSecurityServices \
  -H:EnableURLProtocols=http,https \
  -H:AdditionalSecurityProviders=sun.security.jgss.SunProvider \
  -H:ReflectionConfigurationFiles="$2"/reflect-config.json \
  -H:JNIConfigurationFiles="$2"/jni-config.json \
  -H:ResourceConfigurationFiles="$2"/resource-config.json \
  -H:SerializationConfigurationFiles="$2"/serialization-config.json \
  -H:PredefinedClassesConfigurationFiles="$2"/predefined-classes-config.json \
  -H:DynamicProxyConfigurationFiles="$2"/proxy-config.json \
  --verbose \
  -march=compatibility \
  -cp "$3/*" kafka.docker.KafkaDockerWrapper \
  -o "$4"
