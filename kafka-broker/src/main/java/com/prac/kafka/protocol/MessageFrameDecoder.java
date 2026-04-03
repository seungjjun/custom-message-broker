package com.prac.kafka.protocol;

import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

public class MessageFrameDecoder extends LengthFieldBasedFrameDecoder {

    public MessageFrameDecoder(int maxFrameLength, int lengthFieldOffset, int lengthFieldLength, int lengthAdjustment,
                               int initialBytesToStrip) {
        super(maxFrameLength, lengthFieldOffset, lengthFieldLength, lengthAdjustment, initialBytesToStrip);
    }
}
