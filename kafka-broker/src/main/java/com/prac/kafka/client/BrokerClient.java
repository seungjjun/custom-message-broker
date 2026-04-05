package com.prac.kafka.client;

import com.prac.kafka.common.model.Command;
import com.prac.kafka.common.model.Packet;
import com.prac.kafka.protocol.MessageDecoder;
import com.prac.kafka.protocol.MessageEncoder;
import com.prac.kafka.protocol.MessageFrameDecoder;
import com.prac.kafka.protocol.request.CreateTopicRequest;
import com.prac.kafka.protocol.request.FetchRequest;
import com.prac.kafka.protocol.request.ProduceRequest;
import com.prac.kafka.protocol.response.CreateTopicResponse;
import com.prac.kafka.protocol.response.FetchResponse;
import com.prac.kafka.protocol.response.ProduceResponse;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.fasterxml.jackson.databind.ObjectMapper;

public class BrokerClient {

    private static final int MAX_FRAME_LENGTH = 1_048_576;

    private final String host;
    private final int port;
    private BrokerClientHandler brokerClientHandler;
    private Channel channel;
    private EventLoopGroup group;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public BrokerClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void connect() throws InterruptedException {
        group = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());
        brokerClientHandler = new BrokerClientHandler();

        Bootstrap bootstrap = new Bootstrap()
            .group(group)
            .channel(NioSocketChannel.class)
            .handler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) {
                    ChannelPipeline pipeline = ch.pipeline();

                    pipeline.addLast(new MessageFrameDecoder(MAX_FRAME_LENGTH, 0, 4, 0, 4));
                    pipeline.addLast(new MessageDecoder());
                    pipeline.addLast(new MessageEncoder());
                    pipeline.addLast(brokerClientHandler);
                }
            });

        channel = bootstrap.connect(host, port).sync().channel();
    }

    public Packet send(Packet request) throws ExecutionException, InterruptedException, TimeoutException {
        brokerClientHandler.setResponseFuture(new CompletableFuture<>());
        channel.writeAndFlush(request);
        return brokerClientHandler.getResponseFuture().get(5, TimeUnit.SECONDS);
    }

    public void close() {
        channel.close();
        group.shutdownGracefully();
    }

    public CreateTopicResponse createTopic(String topic) throws Exception {
        CreateTopicRequest request = new CreateTopicRequest(topic);
        Packet packet = new Packet(Command.CREATE_TOPIC, objectMapper.writeValueAsBytes(request));
        Packet response = send(packet);
        return objectMapper.readValue(response.payload(), CreateTopicResponse.class);
    }

    public ProduceResponse produce(String topic, String key, String value) throws Exception {
        ProduceRequest request = new ProduceRequest(topic, key, value);
        Packet packet = new Packet(Command.PRODUCE, objectMapper.writeValueAsBytes(request));
        Packet response = send(packet);
        return objectMapper.readValue(response.payload(), ProduceResponse.class);
    }

    public FetchResponse fetch(String topic, long offset, long maxRecords) throws Exception {
        FetchRequest request = new FetchRequest(topic, offset, maxRecords);
        Packet packet = new Packet(Command.FETCH, objectMapper.writeValueAsBytes(request));
        Packet response = send(packet);
        return objectMapper.readValue(response.payload(), FetchResponse.class);
    }
}
