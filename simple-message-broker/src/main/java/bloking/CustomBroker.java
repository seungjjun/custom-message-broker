package bloking;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CustomBroker {

    private static final int BROKER_PORT = 8080;
    private static final String MESSAGE_LOG_FILE = "messages.log";
    private static final String PRODUCER_MESSAGE_PROTOCOL = "SEND ";

    public static void main(String[] args) {
        try (ServerSocket serverSocket = new ServerSocket(BROKER_PORT)) {
            System.out.println("브로커가 포트 " + BROKER_PORT + "에서 실행 중입니다.");

            ExecutorService executorService = Executors.newFixedThreadPool(10);

            while (true) {
                try {
                    // 블로킹 상태
                    Socket clientSocket = serverSocket.accept();
                    System.out.println("클라이언트 연결되었습니다: " + clientSocket.getInetAddress());

                    executorService.submit(() -> handleClient(clientSocket));
                } catch (IOException e) {
                    System.out.println("클라이언트 연결 처리 중 오류 발생: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.println("서버 소켓 생성 중 오류 발생: " + e.getMessage());
        }
    }

    private static void handleClient(Socket clientSocket) {
        try(
            InputStream input = clientSocket.getInputStream();
            OutputStream output = clientSocket.getOutputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(input))
        ) {
            String initialCommand = reader.readLine();

            if (initialCommand.startsWith(ClientType.PRODUCER.name())) {
                handleProducer(clientSocket, reader, output);
            } else if (initialCommand.startsWith(ClientType.CONSUMER.name())) {
                handleConsumer(clientSocket, reader, output);
            } else {
                output.write("ERROR 유효하지 않은 클라이언트 타입\n".getBytes(StandardCharsets.UTF_8));
            }
        } catch (IOException e) {
            System.err.println("클라이언트 처리 중 오류 발생: " + e.getMessage());
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                System.err.println("소켓 닫기 중 오류 발생: " + e.getMessage());
            }
        }
    }

    private static void handleProducer(Socket clientSocket, BufferedReader reader, OutputStream output) throws IOException {
        String line = reader.readLine();
        while (isContinueReadMessage(line)) {
            if (line.startsWith(Command.SEND.name())) {
                String message = line.substring(PRODUCER_MESSAGE_PROTOCOL.length());

                File logFile = getLogFile();
                try (RandomAccessFile file = new RandomAccessFile(logFile, "rw")) {
                    long currentOffset = writeMessage(file, message);
                    output.write(("SENT: 메시지가 오프셋 " + currentOffset + "에 저장되었습니다.\n").getBytes(StandardCharsets.UTF_8));
                }
            } else {
                output.write("ERROR: 알 수 없는 명령\n".getBytes());
            }
            line = reader.readLine();
        }
    }

    private static File getLogFile() {
        File logsDir = new File("simple-message-broker/logs");
        if (!logsDir.exists()) {
            logsDir.mkdirs();
        }
        return new File(logsDir, MESSAGE_LOG_FILE);
    }

    private static long writeMessage(RandomAccessFile file, String message) throws IOException {
        long currentOffset = file.length();
        file.seek(currentOffset);

        file.write((message + "\n").getBytes(StandardCharsets.UTF_8));
        return currentOffset;
    }

    private static void handleConsumer(Socket clientSocket, BufferedReader reader, OutputStream output) throws IOException {
        String line = reader.readLine();
        while (isContinueReadMessage(line)) {
            if (line.startsWith(Command.GET.name())) {

            }

            line = reader.readLine();
        }
    }

    private static boolean isContinueReadMessage(String line) {
        return line != null && !line.equalsIgnoreCase(Command.QUIT.name());
    }
}
