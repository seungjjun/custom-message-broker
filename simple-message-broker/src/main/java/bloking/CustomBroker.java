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
    private static final String OFFSET_LOG_FILE = "offsets.log";
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

                File logFile = getFile("simple-message-broker/logs", MESSAGE_LOG_FILE);
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

    private static File getFile(String filePath, String fileName) {
        File logsDir = new File(filePath);
        if (!logsDir.exists()) {
            logsDir.mkdirs();
        }
        return new File(logsDir, fileName);
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
                File logFile = getFile("simple-message-broker/logs", MESSAGE_LOG_FILE);
                File offsetFile = getFile("simple-message-broker/offsets", OFFSET_LOG_FILE);

                try (RandomAccessFile file = new RandomAccessFile(logFile, "r")) {
                    long lastReadOffset =  getCurrentOffset(offsetFile);

                    if (lastReadOffset >= file.length()) {
                        output.write("ERROR: 읽을 메시지가 없습니다.\n".getBytes(StandardCharsets.UTF_8));
                        return;
                    }

                    String payload = readMessages(file, lastReadOffset);
                    output.write(("READ: [" + lastReadOffset + "] " + payload + "\n").getBytes(StandardCharsets.UTF_8));

                    saveNewOffset(offsetFile, file.getFilePointer());
                }
            }

            line = reader.readLine();
        }
    }

    private static boolean isContinueReadMessage(String line) {
        return line != null && !line.equalsIgnoreCase(Command.QUIT.name());
    }

    private static long getCurrentOffset(File offsetFile) {
        if (!offsetFile.exists()) {
            return 0L;
        }
        try (RandomAccessFile file = new RandomAccessFile(offsetFile, "r")) {
            String line = file.readLine();
            return line != null ? Long.parseLong(line.trim()) : 0;
        } catch (IOException | NumberFormatException e) {
            System.err.println("오프셋 파일 읽기 중 오류 발생: " + e.getMessage());
            return 0L;
        }
    }

    private static String readMessages(RandomAccessFile file, long offset) throws IOException {
        file.seek(offset);

        StringBuilder builder = new StringBuilder();
        byte[] buffer = new byte[8 * 1024]; // 8kb 버퍼

        int bytesRead;
        while ((bytesRead = file.read(buffer)) != -1) {
            builder.append(new String(buffer, 0, bytesRead, StandardCharsets.UTF_8));
        }

        return builder.toString();
    }

    private static void saveNewOffset(File offsetFile, long newOffset) {
        try (RandomAccessFile file = new RandomAccessFile(offsetFile, "rw")) {
            file.setLength(0); // 파일 초기화
            file.write(String.valueOf(newOffset).getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            System.err.println("오프셋 파일 저장 중 오류 발생: " + e.getMessage());
        }
    }
}
