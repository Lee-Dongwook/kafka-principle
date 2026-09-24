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
