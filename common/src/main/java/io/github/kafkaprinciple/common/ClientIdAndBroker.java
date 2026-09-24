package io.github.kafkaprinciple.common;

public class ClientIdAndBroker {
    public final String clientId;
    public final String brokerHost;
    public final int brokerPort;

    public ClientIdAndBroker(String clientId, String brokerHost, int brokerPort) {
        this.clientId = clientId;
        this.brokerHost = brokerHost;
        this.brokerPort = brokerPort;
    } 

    @Override
    public String toString() {
        return String.format("%s-%s-%d", clientId, brokerHost, brokerPort);
    }
}
