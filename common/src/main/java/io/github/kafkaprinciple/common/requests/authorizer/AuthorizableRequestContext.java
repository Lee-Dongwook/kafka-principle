package io.github.kafkaprinciple.common.requests.authorizer;

import io.github.kafkaprinciple.common.annotation.InterfaceAudience;
import io.github.kafkaprinciple.common.security.auth.KafkaPrincipal;
import io.github.kafkaprinciple.common.security.auth.SecurityProtocol;

import java.net.InetAddress;

@InterfaceAudience.Public
public interface AuthorizableRequestContext {
    String listenerName();

    SecurityProtocol securityProtocol();

    KafkaPrincipal principal();

    InetAddress clientAddress();

    int requestType();

    int requestVersion();

    String clientId();

    int correlationId();
}
