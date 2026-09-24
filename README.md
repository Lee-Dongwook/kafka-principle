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

현재 Gradle Java 모듈은 `broker`, `common`, `network`, `connect-core`, `connect-plugin-file`입니다. 메시지 프로토콜과 디스크 로그 저장 기능은 구현을 시작할 때 각각 `protocol`, `storage` 모듈로 추가합니다.

## Docker 레이어의 Go 마이그레이션

Apache Kafka의 Docker 도구는 이미지 빌드·릴리스 자동화를 Python 스크립트로 제공합니다. 이 프로젝트에서는 native Docker 이미지에 필요한 도구를 Go로 옮겼습니다.

마이그레이션의 목적은 다음과 같습니다.

- 빌드 명령, 이미지 타입, Kafka 배포본 입력을 컴파일 단계와 실행 전 검증으로 명확히 관리합니다.
- Docker와 Podman을 공통 인터페이스로 감지하고, 실패한 외부 명령과 임시 빌드 컨텍스트 정리 오류를 함께 확인할 수 있게 합니다.
- Go 단위 테스트로 경로 순회, 심볼릭 링크, 잘못된 CLI 입력처럼 이미지 빌드 과정에서 놓치기 쉬운 경우를 검증합니다.
- 이후 Docker Official Image 생성과 통합 테스트도 같은 언어와 테스트 방식으로 확장할 기반을 마련합니다.

현재 범위는 `native` 이미지의 빌드와 multi-architecture 릴리스입니다. Compose 기반 통합 테스트와 JVM/Official Image 관련 도구는 필요한 fixture와 리소스를 추가한 뒤 순차적으로 마이그레이션합니다. 자세한 실행 방법은 [docker/README.md](docker/README.md)를 참고하세요.
