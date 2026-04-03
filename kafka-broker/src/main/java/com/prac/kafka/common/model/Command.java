package com.prac.kafka.common.model;

import java.util.HashMap;
import java.util.Map;

public enum Command {
    PRODUCE((byte) 0x01),
    PRODUCE_ACK((byte) 0x02),
    FETCH((byte) 0x03),
    FETCH_RESPONSE((byte) 0x04),
    CREATE_TOPIC((byte) 0x05),
    TOPIC_ACK((byte) 0x06),
    COMMIT_OFFSET((byte) 0x07),
    COMMIT_ACK((byte) 0x08),
    ERROR((byte) 0x0F);

    private final byte opcode;

    private static final Map<Byte, Command> BY_OPCODE = new HashMap<>();

    static {
        for (Command cmd : values()) {
            BY_OPCODE.put(cmd.opcode, cmd);
        }
    }

    Command(byte opcode) {
        this.opcode = opcode;
    }

    public byte getOpcode() {
        return opcode;
    }

    public static Command fromOpcode(byte opcode) {
        Command cmd = BY_OPCODE.get(opcode);
        if (cmd == null) {
            throw new IllegalArgumentException("Unknown opcode: 0x%02X".formatted(opcode));
        }
        return cmd;
    }
}
