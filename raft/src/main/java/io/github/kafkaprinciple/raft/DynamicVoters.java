package io.github.kafkaprinciple.raft;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

public final class DynamicVoters {
    public static DynamicVoters parse(String input) {
        input = input.trim();
        List<DynamicVoter> voters = new ArrayList<>();
        for (String voterString : input.split(",")) {
            if(!voterString.isEmpty()) {
                voters.add(DynamicVoter.parse(voterString));
            }
        }

        return new DynamicVoters(voters);
    }

    private final NavigableMap<Integer, DynamicVoter> voters;

    public DynamicVoters(Collection<DynamicVoter> voters) {
        if (voters.isEmpty()) {
            throw new IllegalArgumentException("No voters given.");
        }
        TreeMap<Integer, DynamicVoter> votersMap = new TreeMap<>();
        for (DynamicVoter voter : voters) {
            if (votersMap.put(voter.nodeId(), voter) != null) {
                throw new IllegalArgumentException("Node id " + voter.nodeId() +
                        " was specified more than once.");
            }
        }
        this.voters = Collections.unmodifiableNavigableMap(votersMap);
    }

    public NavigableMap<Integer, DynamicVoter> voters() {
        return voters;
    }

    public VoterSet toVoterSet(String controllerListenerName) {
        Map<Integer, VoterSet.VoterNode> voterSetMap = new HashMap<>();
        for (DynamicVoter voter : voters.values()) {
            voterSetMap.put(voter.nodeId(), voter.toVoterNode(controllerListenerName));
        }
        return VoterSet.fromMap(voterSetMap);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || (!(o.getClass().equals(DynamicVoters.class)))) return false;
        DynamicVoters other = (DynamicVoters) o;
        return voters.equals(other.voters);
    }

    @Override
    public int hashCode() {
        return voters.hashCode();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        String prefix = "";
        for (DynamicVoter voter : voters.values()) {
            builder.append(prefix);
            prefix = ",";
            builder.append(voter.toString());
        }
        return builder.toString();
    }
}


