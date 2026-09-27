package io.github.kafkaprinciple.raft;

import io.github.kafkaprinciple.common.Uuid;
import io.github.kafkaprinciple.common.feature.SupportedVersionRange;
import io.github.kafkaprinciple.common.network.ListenerName;

import java.net.InetSocketAddress;
import java.util.map;

public record DynamicVoter (Uuid directoryId, int nodeId, String host, int port) {
    public static DynamicVoter parse(String input) {
        input = input.trim();
        int atIndex = input.indexOf("@");
        if (atIndex < 0) {
            throw new IllegalArgumentException("No @ found in dynamic voter string.");
        }

        if (atIndex == 0) {
            throw new IllegalArgumentException("Invalid @ at beginning of dynamic voter string.");
        }
        String idString = input.substring(0, atIndex);
        int nodeId;

        try {
            nodeId = Integer.parseInt(idString);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Failed to parse node id in dynamic voter string.", e);
        }

        if (nodeId < 0) {
            throw new IllegalArgumentException("Invalid negative node id " + nodeId +
                " in dynamic voter string.");
        }

        input = input.substring(atIndex + 1);
        if (input.isEmpty()) {
            throw new IllegalArgumentException("No hostname found after node id.");
        }

        String host;
        if (input.startsWith("[")) {
            int endBracketIndex = input.indexOf("]");
            if (endBracketIndex < 0) {
                throw new IllegalArgumentException("Hostname began with left bracket, but no right " +
                    "bracket was found.");
            }
            host = input.substring(1, endBracketIndex);
            input = input.substring(endBracketIndex + 1);
        } else {
            int endColonIndex = input.indexOf(":");
            if (endColonIndex < 0) {
                throw new IllegalArgumentException("No colon following hostname could be found.");
            }
            host = input.substring(0, endColonIndex);
            input = input.substring(endColonIndex);
        }
        if (!input.startsWith(":")) {
            throw new IllegalArgumentException("Port section must start with a colon.");
        }

        input = input.substring(1);
        int endColonIndex = input.indexOf(":");
        if (endColonIndex < 0) {
            throw new IllegalArgumentException("No colon following port could be found.");
        }
        
        String portString = input.substring(0, endColonIndex);
        int port;
        
        try {
            port = Integer.parseInt(portString);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Failed to parse port in dynamic voter string.", e);
        }
        
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("Invalid port " + port + " in dynamic voter string.");
        }
        
        String directoryIdString = input.substring(endColonIndex + 1);
        Uuid directoryId;
        
        try {
            directoryId = Uuid.fromString(directoryIdString);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Failed to parse directory ID in dynamic voter string.", e);
        }

        return new DynamicVoter(directoryId, nodeId, host, port);
    }

    public VoterSet.VoterNode toVoterNode(String controllerListenerName) {
        ReplicaKey voterKey = ReplicaKey.of(nodeId, directoryId);
        Endpoints listeners = Endpoints.fromInetSocketAddresses(Map.of(
            ListenerName.normalised(controllerListenerName),
            new InetSocketAddress(host, port)));
        SupportedVersionRange supportedKRaftVersion =
            new SupportedVersionRange((short) 0, (short) 1);
        return VoterSet.VoterNode.of(voterKey, listeners, supportedKRaftVersion);
    }

    @Override
    public String toString() {
        if (host.contains(":")) {
            return nodeId + "@[" + host + "]:" + port + ":" + directoryId;
        } else {
            return nodeId + "@" + host + ":" + port + ":" + directoryId;
        }
    }
}
