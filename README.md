# kafka-principle

Apache Kafka의 핵심 동작 원리를 학습하기 위한 Java 기반 클론코딩 프로젝트입니다.

## 구조

```text
broker/   브로커 실행과 브로커 설정 모델
common/   공통 어노테이션과 직렬화 인터페이스
network/  소켓 연결과 요청 송수신 기능
config/   로컬 브로커 실행 설정
```

메시지 프로토콜과 디스크 로그 저장 기능은 구현을 시작할 때 각각 `protocol`, `storage` 모듈로 추가합니다.

## Docker 레이어의 Go 마이그레이션

Apache Kafka의 Docker 도구는 이미지 빌드·릴리스 자동화를 Python 스크립트로 제공합니다. 이 프로젝트에서는 native Docker 이미지에 필요한 도구를 Go로 옮겼습니다.

마이그레이션의 목적은 다음과 같습니다.

- 빌드 명령, 이미지 타입, Kafka 배포본 입력을 컴파일 단계와 실행 전 검증으로 명확히 관리합니다.
- Docker와 Podman을 공통 인터페이스로 감지하고, 실패한 외부 명령과 임시 빌드 컨텍스트 정리 오류를 함께 확인할 수 있게 합니다.
- Go 단위 테스트로 경로 순회, 심볼릭 링크, 잘못된 CLI 입력처럼 이미지 빌드 과정에서 놓치기 쉬운 경우를 검증합니다.
- 이후 Docker Official Image 생성과 통합 테스트도 같은 언어와 테스트 방식으로 확장할 기반을 마련합니다.

현재 범위는 `native` 이미지의 빌드와 multi-architecture 릴리스입니다. Compose 기반 통합 테스트와 JVM/Official Image 관련 도구는 필요한 fixture와 리소스를 추가한 뒤 순차적으로 마이그레이션합니다. 자세한 실행 방법은 [docker/README.md](docker/README.md)를 참고하세요.
