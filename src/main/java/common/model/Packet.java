package common.model;

public record Packet(
    Command command,
    byte[] payload
) {
}
