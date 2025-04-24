import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;

public class NioClient {

    private static final String HOST = "localhost";
    private static final int PORT = 8080;
    private static final int BUFFER_SIZE = 1024;

    private static final String QUIT_COMMAND = "quit";

    public static void main(String[] args) {
        SocketChannel clientChannel = null;
        BufferedReader consoleReader = null;

        try {
            clientChannel = SocketChannel.open();
            clientChannel.connect(new InetSocketAddress(HOST, PORT)); // 서버에 연결 요청 (3-way handshake 시작)

            System.out.println("서버에 연결되었습니다.");

            consoleReader = new BufferedReader(new InputStreamReader(System.in));
            String message;

            printWelcomeMessage(clientChannel);

            while ((message = consoleReader.readLine()) != null) {
                if (QUIT_COMMAND.equalsIgnoreCase(message)) {
                    break;
                }

                ByteBuffer buffer = ByteBuffer.wrap(message.getBytes(StandardCharsets.UTF_8));

                // ByteBuffer의 내용을 SocketChannel로 쓰기
                clientChannel.write(buffer);

                System.out.println("메시지 전송: " + message);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (clientChannel != null) {
                    clientChannel.close();
                }
                if (consoleReader != null) {
                    consoleReader.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static void printWelcomeMessage(SocketChannel channel) throws IOException {
        ByteBuffer welcomeBuffer = ByteBuffer.allocate(BUFFER_SIZE);
        channel.read(welcomeBuffer);
        welcomeBuffer.flip();
        String welcomeMessage = StandardCharsets.UTF_8.decode(welcomeBuffer).toString();
        System.out.println("서버로부터 받은 메시지: " + welcomeMessage);
        System.out.println("메시지를 입력하세요 (종료하려면 'quit' 입력):");
    }
}
