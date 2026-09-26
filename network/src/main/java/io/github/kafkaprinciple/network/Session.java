package io.github.kafkaprinciple.network;

import io.github.kafkaprinciple.common.security.auth.KafkaPrincipal;
import io.github.kafkaprinciple.common.utils.internals.Sanitizer;

import java.net.InetAddress;

public class Session {
    public final KafkaPrincipal principal;
    public final InetAddress clientAddress;
    public final String sanitiziedUser;
    
    public Session(KafkaPrincipal principal, InetAddress clientAddress) {
        this.principal = principal;
        this.clientAddress = clientAddress;
        this.sanitiziedUser = Sanitizer.sanitize(principal.getName());
    }
}
