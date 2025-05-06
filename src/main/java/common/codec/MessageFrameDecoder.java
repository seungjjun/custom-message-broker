package common.codec;

import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

public class MessageFrameDecoder extends LengthFieldBasedFrameDecoder {

    public MessageFrameDecoder() {
        // 최대 메시지 크기: 1MB
        super(1_048_576, 0, 4, 0, 4);
    }
}
