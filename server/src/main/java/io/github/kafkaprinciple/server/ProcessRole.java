package io.github.kafkaprinciple.server;

public enum ProcessRole {
    BrokerRole("broker");
    ControllerRole("controller");

    private final String roleName;

    ProcessRole(String roleName) {
        this.roleName = roleName;
    }

    @Override
    public String toString() {
        return roleName;
    }
}
