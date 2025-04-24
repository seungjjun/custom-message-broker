import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public class NioServer {

    private static final int PORT = 8080;
    private static final int BUFFER_SIZE = 1024;

    public static void main(String[] args) {
        Selector selector = null;
        ServerSocketChannel serverSocketChannel = null;

        try {
            selector = Selector.open();

            serverSocketChannel = ServerSocketChannel.open(); // 서버 소켓 생성
            initServerSocketChannel(serverSocketChannel, selector);

            System.out.println("서버가 포트 " + PORT + "에서 시작되었습니다.");

            while (true) {
                // select() 호출 하여 이벤트가 발생할 때까지 대기 (blocking) -> 클라이언트가 연결을 요청하면 이벤트 발생
                int readyChannels = selector.select();

                if (readyChannels == 0) {
                    continue; // 발생한 이벤트가 없으면 다시 대기
                }

                Iterator<SelectionKey> keyIterator = selector.selectedKeys().iterator();
                while (keyIterator.hasNext()) {
                    SelectionKey key = keyIterator.next();

                    // 중복 처리 방지를 위한 현재 key 제거
                    keyIterator.remove();

                    if (key.isAcceptable()) {
                        // 클라이언트 연결 요청 처리
                        handleAccept(key, selector);
                    } else if (key.isReadable()) {
                        // 클라이언트로부터 데이터 읽기 처리
                        handleRead(key);
                    }
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (serverSocketChannel != null) {
                    serverSocketChannel.close();
                }
                if (selector != null) {
                    selector.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static void initServerSocketChannel(ServerSocketChannel serverSocketChannel, Selector selector) throws IOException {
        serverSocketChannel.bind(new InetSocketAddress(PORT));
        serverSocketChannel.configureBlocking(false); // non-blocking mode

        // ServerSocketChannel을 Selector에 등록 (Accept 이벤트 감지)
        serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
    }

    private static void handleAccept(SelectionKey key, Selector selector) throws IOException {
        ServerSocketChannel serverChannel = (ServerSocketChannel) key.channel();
        SocketChannel clientChannel = serverChannel.accept();

        clientChannel.configureBlocking(false); // non-blocking mode

        // 클라이언트 소켓 채널을 Selector에 등록 (Read 이벤트 감지)
        clientChannel.register(selector, SelectionKey.OP_READ, ByteBuffer.allocate(BUFFER_SIZE)); // 버퍼를 첨부하여 등록
        System.out.println("클라이언트 연결됨: " + clientChannel.getRemoteAddress());

        String welcomeMessage = "Welcome to the NIO Playground Server!\n";
        clientChannel.write(ByteBuffer.wrap(welcomeMessage.getBytes()));
    }

    private static void handleRead(SelectionKey key) throws IOException {
        SocketChannel clientChannel = (SocketChannel) key.channel();
        ByteBuffer buffer = (ByteBuffer) key.attachment(); // 등록 시 첨부했던 버퍼 가져오기

        int bytesRead;
        try {
            bytesRead = clientChannel.read(buffer);
        } catch (IOException e) {
            System.out.println("클라이언트 연결 오류: " + clientChannel.getRemoteAddress() + ", " + e.getMessage());
            key.cancel(); // 키 취소
            clientChannel.close(); // 클라이언트 채널 닫기
            return;
        }

        if (bytesRead == -1) {
            // 클라이언트 연결 정상 종료
            System.out.println("클라이언트 연결 종료됨: " + clientChannel.getRemoteAddress());
            key.cancel(); // 셀렉터에서 키 제거
            clientChannel.close(); // 채널 닫기
            return;
        }

        // 버퍼를 읽기 모드로 전환
        buffer.flip();

        // 버퍼에서 데이터 읽기 및 처리
        byte[] data = new byte[buffer.remaining()];
        buffer.get(data); // 버퍼의 남은 데이터를 바이트 배열로 복사

        String message = new String(data, StandardCharsets.UTF_8); // 바이트 배열을 문자열로 변환
        System.out.println("메시지 수신 (" + clientChannel.getRemoteAddress() + "): " + message.trim());

        buffer.clear(); // 버퍼 초기화 (다음 읽기를 위해)
    }
}
