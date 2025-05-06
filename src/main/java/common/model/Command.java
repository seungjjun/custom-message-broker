package common.model;

public enum Command {
    CONNECT((byte) 0x01),
    CONNACK((byte) 0x02),
    DISCONNECT((byte) 0x03),
    SUBSCRIBE((byte) 0x04),
    SUBACK((byte) 0x05),
    UNSUBSCRIBE((byte) 0x06),
    UNSUBACK((byte) 0x07),
    PUBLISH((byte) 0x08),
    EVENT((byte) 0x09),
    ERROR((byte) 0x0F);

    public final byte opcode;

    Command(byte opcode) {
        this.opcode = opcode;
    }

    public byte getOpcode() {
        return opcode;
    }

    public static Command fromOpcode(byte opcode) {
        for (Command command : Command.values()) {
            if (command.opcode == opcode) {
                return command;
            }
        }
        throw new IllegalArgumentException("Unknown opcode: " + opcode);
    }
}
