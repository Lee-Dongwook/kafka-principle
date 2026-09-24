FROM ghcr.io/graalvm/graalvm-community:25 AS build-native-image

ARG kafka_url=""

WORKDIR /app

ENV NATIVE_IMAGE_PATH="native-image"
ENV KAFKA_DIR="/app/kafka"
ENV NATIVE_CONFIGS_DIR="/app/native-image-configs"
ENV KAFKA_LIBS_DIR="$KAFKA_DIR/libs"
ENV TARGET_PATH="$KAFKA_DIR/kafka.Kafka"

COPY native-image-configs $NATIVE_CONFIGS_DIR
COPY --chmod=0755 native_command.sh native_command.sh

COPY *kafka.tgz /app

RUN set -eux; \
    if [ -n "$kafka_url" ]; then \
        microdnf install wget gnupg2; \
        wget -nv -O kafka.tgz "$kafka_url"; \
        wget -nv -O kafka.tgz.asc "$kafka_url.asc"; \
        wget -nv -O KEYS https://downloads.apache.org/kafka/KEYS; \
        gpg --import KEYS; \
        gpg --batch --verify kafka.tgz.asc kafka.tgz; \
    fi; \
    mkdir $KAFKA_DIR; \
    tar xfz kafka.tgz -C $KAFKA_DIR --strip-components 1; \
    /app/native_command.sh $NATIVE_IMAGE_PATH $NATIVE_CONFIGS_DIR $KAFKA_LIBS_DIR $TARGET_PATH


FROM alpine:latest

EXPOSE 9092

ARG build_date

LABEL org.label-schema.name="kafka" \
      org.label-schema.description="Apache Kafka" \
      org.label-schema.build-date="${build_date}" \
      org.label-schema.vcs-url="https://github.com/apache/kafka" \
      org.opencontainers.image.authors="Apache Kafka"

RUN apk update ; \
    apk upgrade --no-cache libcrypto3 libssl3 zlib; \
    apk add --no-cache gcompat ; \
    apk add --no-cache bash ; \
    mkdir -p /etc/kafka/docker /mnt/shared/config /opt/kafka/config /etc/kafka/secrets ; \
    adduser -h /home/appuser -D --shell /bin/bash appuser ; \
    chown appuser:root -R /etc/kafka /opt/kafka /mnt/shared/config ; \
    chmod -R ug+w /etc/kafka /opt/kafka /mnt/shared/config ;

COPY server.properties /etc/kafka/docker/server.properties
COPY --chown=appuser:root --from=build-native-image /app/kafka/kafka.Kafka /opt/kafka/
COPY --chown=appuser:root --from=build-native-image /app/kafka/config/log4j2.yaml /etc/kafka/docker/
COPY --chown=appuser:root --from=build-native-image /app/kafka/config/tools-log4j2.yaml /etc/kafka/docker/
COPY --chmod=0755 --chown=appuser:root resources/scripts /etc/kafka/docker/
COPY --chmod=0755 --chown=appuser:root launch /etc/kafka/docker/

USER appuser

VOLUME ["/etc/kafka/secrets", "/mnt/shared/config"]

CMD ["/etc/kafka/docker/run"]
