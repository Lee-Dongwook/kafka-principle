# Kafka Docker 도구

이 디렉터리는 Apache Kafka의 Docker 도구 구조를 참고해, 현재 프로젝트의 native 이미지 빌드와 릴리스를 Go로 실행합니다.

## 사전 조건

- Go 1.25 이상
- 로컬 이미지 빌드: Docker 또는 Podman
- multi-architecture 릴리스: Docker Buildx와 대상 레지스트리 로그인

## Native 이미지 빌드

Kafka 배포본 URL을 사용합니다.

```bash
cd docker
go run ./cmd/docker-build-test my-kafka --image-tag dev --kafka-url https://archive.apache.org/dist/kafka/<version>/kafka_2.13-<version>.tgz --build
```

로컬 tarball을 사용할 수도 있습니다.

```bash
go run ./cmd/docker-build-test my-kafka --image-tag dev --kafka-archive /absolute/path/kafka.tgz --build
```

`--build`를 생략하면 빌드 뒤 컨테이너 런타임의 이미지 조회로 생성 여부를 확인합니다. Compose 기반 Kafka 통합 테스트는 기준 저장소의 `test/fixtures`가 아직 이 프로젝트에 없으므로 별도 마이그레이션 대상입니다.

## Multi-architecture 릴리스

```bash
go run ./cmd/docker-release registry.example/my-kafka:1.0.0 --kafka-url https://archive.apache.org/dist/kafka/<version>/kafka_2.13-<version>.tgz
```

이 명령은 `linux/amd64`와 `linux/arm64` 이미지를 buildx로 빌드한 뒤 즉시 push합니다. 대상 레지스트리 접근 권한과 로그인 상태를 먼저 확인하세요.
