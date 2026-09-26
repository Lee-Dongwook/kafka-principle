package io.github.kafkaprinciple.common.requests;

import io.github.kafkaprinciple.common.requests.authorizer.AuthorizableRequestContext;
import io.github.kafkaprinciple.common.security.auth.KafkaPrincipal;
import io.github.kafkaprinciple.common.security.auth.SecurityProtocol;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Optional;

public class RequestContext implements AuthorizableRequestContext {
    public final RequestHeader header;
    public final String connectionId;
    public final InetAddress clientAddress;
    public final Optional<Integer> clientPort;
    public final KafkaPrincipal principal;
    public final ListenerName listenerName;
    public final SecurityProtocol securityProtocol;
    public final ClientInformation clientInformation;
    public final boolean fromPrivilegedListener;
    public final Optional<KafkaPrincipalSerde> principalSerde;

    public RequestContext(RequestHeader header,
                          String connectionId,
                          InetAddress clientAddress,
                          KafkaPrincipal principal,
                          ListenerName listenerName,
                          SecurityProtocol securityProtocol,
                          ClientInformation clientInformation,
                          boolean fromPrivilegedListener
    ) {
        this(
                header,
                connectionId,
                clientAddress,
                Optional.empty(),
                principal,
                listenerName,
                securityProtocol,
                clientInformation,
                fromPrivilegedListener,
                Optional.empty());
    }
    
    public RequestContext(RequestHeader header,
        String connectionId,
        InetAddress clientAddress,
        Optional<Integer> clientPort,
        KafkaPrincipal principal,
        ListenerName listenerName,
        SecurityProtocol securityProtocol,
        ClientInformation clientInformation,
            boolean fromPrivilegedListener) {
        this(header,
                connectionId,
                clientAddress,
                clientPort,
                principal,
                listenerName,
                securityProtocol,
                clientInformation,
                fromPrivilegedListener,
                Optional.empty());
    }
    
    public RequestContext(RequestHeader header,
                          String connectionId,
                          InetAddress clientAddress,
                          Optional<Integer> clientPort,
                          KafkaPrincipal principal,
                          ListenerName listenerName,
                          SecurityProtocol securityProtocol,
                          ClientInformation clientInformation,
                          boolean fromPrivilegedListener,
            Optional<KafkaPrincipalSerde> principalSerde) {
        this.header = header;
        this.connectionId = connectionId;
        this.clientAddress = clientAddress;
        this.clientPort = clientPort;
        this.principal = principal;
        this.listenerName = listenerName;
        this.securityProtocol = securityProtocol;
        this.clientInformation = clientInformation;
        this.fromPrivilegedListener = fromPrivilegedListener;
        this.principalSerde = principalSerde;
    }
    
    public RequestAndSize parseRequest(ByteBuffer buffer) {
        if (isUnsupportedApiVersionRequest()) {
            ApiVersionRequest apiVersionsRequest = new ApiVersionRequest(new ApiVersionsRequestData(), (short) 0,
                    header.apiVersion());
            return new RequestAndSize(apiVersionsRequest, 0);
        } else {
            ApiKeys apiKey = header.apiKey();
            try {
                short apiVersion = header.apiVersion();
                return AbstractRequest.parseRequest(apiKey, apiVersion, new ByteBufferAccessor(buffer));
            } catch (Throwable ex) {
                throw new InvalidRequestException("Error getting request for apiKey: " + apiKey +
                        ", apiVersion: " + header.apiVersion() +
                        ", connectionId: " + connectionId +
                        ", listenerName: " + listenerName +
                        ", principal: " + principal, ex);
            }
        }
    }

    public Send buildResponseSend(AbstractResponse body) {
        return body.toSend(header.toResponseHeader(), apiVersion());
    }

    public ByteBuffer buildResponseEnvelopePayload(AbstractResponse body) {
        return body.serializeWithHeader(header.toResponseHeader(), apiVersion());
    }

    private boolean isUnsupportedApiVersionsRequest() {
        return header.apiKey() == API_VERSIONS && !header.isApiVersionSupported();
    }

    public short apiVersion() {
        if (isUnsupportedApiVersionsRequest())
            return 0;
        return header.apiVersion();
    }

    public String connectionId() {
        return connectionId;
    }
    
    @Override
    public String listenerName() {
        return listenerName.value();
    }

    @Override
    public SecurityProtocol securityProtocol() {
        return securityProtocol;
    }

    @Override
    public KafkaPrincipal principal() {
        return principal;
    }

    @Override
    public InetAddress clientAddress() {
        return clientAddress;
    }

    @Override
    public int requestType() {
        return header.apiKey().id;
    }

    @Override
    public int requestVersion() {
        return header.apiVersion();
    }

    @Override
    public String clientId() {
        return header.clientId();
    }

    @Override
    public int correlationId() {
        return header.correlationId();
    }

    @Override
    public String toString() {
        return "RequestContext(" +
            "header=" + header +
            ", connectionId='" + connectionId + '\'' +
            ", clientAddress=" + clientAddress +
            ", principal=" + principal +
            ", listenerName=" + listenerName +
            ", securityProtocol=" + securityProtocol +
            ", clientInformation=" + clientInformation +
            ", fromPrivilegedListener=" + fromPrivilegedListener +
            ", principalSerde=" + principalSerde +
            ')';
    }
}
