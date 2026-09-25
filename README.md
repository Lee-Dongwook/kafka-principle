# kafka-principle

Apache Kafka의 핵심 동작 원리를 학습하기 위한 Java 기반 클론코딩 프로젝트입니다.

## 프로젝트 구조

```text
kafka-principle/
├── broker/                         # 브로커 실행 및 브로커 설정 모델
│   └── src/main/java/.../broker/
│       └── config/                 # Log4j 설정 모델
├── common/                         # 여러 Java 모듈이 공유하는 기반 타입
│   └── src/main/java/.../common/
│       ├── annotation/             # 공개/내부 API 표시 annotation
│       ├── queue/                  # EventQueue와 이벤트 처리 기반
│       ├── serialization/          # 바이트 직렬화/역직렬화 계약
│       ├── DirectoryId.java        # 로그 디렉터리 식별자 관리
│       ├── ClientIdAndBroker.java  # 클라이언트·브로커 식별 정보
│       └── Uuid.java               # 128비트 식별자
├── network/                        # 소켓 연결과 요청 송수신 기반
├── connect-core/                   # Connect 공개 API의 시작점
│   └── src/main/java/.../connect/api/
│       └── health/                 # Connector 상태와 유형 모델
├── connect-plugin-file/            # 학습용 파일 Source/Sink Connector 플러그인
│   └── src/main/java/.../file/
├── image/                          # 메타데이터를 불변 이미지와 탐색 노드로 표현
│   └── src/main/java/.../image/
│       ├── node/                   # 셸·출력에서 순회하는 메타데이터 트리
│       └── writer/                 # 이미지 기록 계약
├── shell/                          # 메타데이터를 탐색하는 대화형 CLI
│   └── src/main/java/.../shell/
│       ├── command/                # 명령어 파싱·완성·핸들러
│       ├── node/                   # 셸 전용 메타데이터 루트
│       └── state/                  # 현재 루트와 작업 디렉터리 상태
├── docker/                         # Native Kafka 이미지 빌드·릴리스 도구 (Go)
│   ├── cmd/
│   │   ├── docker-build-test/      # 이미지 빌드와 기본 검증 CLI
│   │   └── docker-release/         # Buildx multi-architecture push CLI
│   ├── internal/containerutil/     # Docker/Podman 공통 실행·빌드 컨텍스트 처리
│   ├── native/                     # GraalVM native-image Dockerfile과 실행 스크립트
│   ├── resources/scripts/          # 컨테이너 초기 설정 스크립트
│   └── server.properties           # 단일 노드 KRaft 기본 설정
├── config/
│   └── broker.properties           # 로컬 브로커 실행 설정
├── build.gradle                    # 공통 Java/Gradle 설정
└── settings.gradle                 # Java 모듈 등록
```

현재 Gradle Java 모듈은 `broker`, `common`, `network`, `connect-core`, `connect-plugin-file`, `image`, `shell`입니다. 일부 `image`, `shell` 클래스는 후속 구현을 위한 `TODO` 골격이며, 현재는 모듈 연결과 컴파일 가능한 API 형태를 먼저 갖춥니다. 메시지 프로토콜과 디스크 로그 저장 기능은 구현을 시작할 때 각각 `protocol`, `storage` 모듈로 추가합니다.

## 모놀리식 아키텍처와 도메인 배선

이 프로젝트는 하나의 Git 저장소와 하나의 Gradle 빌드로 실행·검증하는 **모듈형 모놀리스**입니다. 마이크로서비스처럼 프로세스를 나누지는 않지만, 도메인 책임과 코드 의존성은 Gradle 모듈 경계로 분리합니다. 따라서 학습자는 한 번에 실행 가능한 프로젝트를 유지하면서도, 기능이 어디에 속하고 어떤 방향으로 연결되어야 하는지 추적할 수 있습니다.

```text
                  common
                 /   |   \
           network  image  connect-core
              |       |         |
           broker   shell  connect-plugin-file
```

- `common`은 식별자, 예외, 큐, 직렬화 계약처럼 여러 도메인이 함께 쓰는 가장 낮은 계층입니다. 이 모듈은 상위 도메인을 의존하지 않습니다.
- `network`는 통신 기반을 제공하고, `broker`는 이를 사용해 브로커 실행 도메인을 구성합니다.
- `image`는 브로커 메타데이터를 읽기 좋은 구조로 표현합니다. `shell`은 `image`만 참조해 메타데이터를 탐색하므로, CLI가 브로커 내부 구현에 직접 결합하지 않습니다.
- `connect-core`는 Connector 공통 계약을 정의하고, `connect-plugin-file` 같은 플러그인이 그 계약을 구현합니다. 플러그인끼리는 서로 의존하지 않습니다.

### 의존성을 정하는 기준

새 코드를 추가할 때는 먼저 “누가 이 코드를 알아야 하는가?”를 기준으로 모듈을 고릅니다.

- 두 개 이상 도메인이 재사용하는 작고 안정적인 타입이면 `common`에 둡니다.
- 특정 업무 개념(예: 메타데이터 이미지, Connector, 브로커 설정)을 표현하면 해당 도메인 모듈에 둡니다.
- 화면·CLI·플러그인 같은 입출력 계층은 도메인 모델을 사용하되, 반대로 도메인 모델이 입출력 계층을 참조하지 않게 합니다.
- 새로운 도메인이 독자적인 책임, 테스트, 의존성 집합을 갖게 되면 Gradle 하위 모듈로 만들고 `settings.gradle`에 등록합니다. 단순한 패키지 구분만 필요하면 기존 모듈 안의 패키지로 시작합니다.

이 규칙은 모놀리스가 커져도 순환 의존성을 피하고, 나중에 특정 도메인을 별도 서비스나 라이브러리로 분리할 선택지를 남깁니다.

## Docker 레이어의 Go 마이그레이션

Apache Kafka의 Docker 도구는 이미지 빌드·릴리스 자동화를 Python 스크립트로 제공합니다. 이 프로젝트에서는 native Docker 이미지에 필요한 도구를 Go로 옮겼습니다.

마이그레이션의 목적은 다음과 같습니다.

- 빌드 명령, 이미지 타입, Kafka 배포본 입력을 컴파일 단계와 실행 전 검증으로 명확히 관리합니다.
- Docker와 Podman을 공통 인터페이스로 감지하고, 실패한 외부 명령과 임시 빌드 컨텍스트 정리 오류를 함께 확인할 수 있게 합니다.
- Go 단위 테스트로 경로 순회, 심볼릭 링크, 잘못된 CLI 입력처럼 이미지 빌드 과정에서 놓치기 쉬운 경우를 검증합니다.
- 이후 Docker Official Image 생성과 통합 테스트도 같은 언어와 테스트 방식으로 확장할 기반을 마련합니다.

현재 범위는 `native` 이미지의 빌드와 multi-architecture 릴리스입니다. Compose 기반 통합 테스트와 JVM/Official Image 관련 도구는 필요한 fixture와 리소스를 추가한 뒤 순차적으로 마이그레이션합니다. 자세한 실행 방법은 [docker/README.md](docker/README.md)를 참고하세요.
