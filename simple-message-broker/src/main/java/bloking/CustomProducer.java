package bloking;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class CustomProducer {

    private static final String BROKER_SERVER_ADDRESS = "localhost";
    private static final int BROKER_SERVER_PORT = 8080;

    public static void main(String[] args) {
        try (Socket socket = new Socket(BROKER_SERVER_ADDRESS, BROKER_SERVER_PORT)) {
            PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader userInput = new BufferedReader(new InputStreamReader(System.in));
            BufferedReader brokerReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            System.out.println("프로듀서가 브로커에 연결되었습니다.");

            writer.println(ClientType.PRODUCER.name());
            System.out.println("메시지를 입력하세요 (종료하려면 'QUIT' 입력):");

            String message = userInput.readLine();
            while (message != null && !message.equalsIgnoreCase(Command.QUIT.name())) {
                String sendMessage = Command.SEND.name() + " " + message;
                writer.println(sendMessage);

                String response = brokerReader.readLine();
                System.out.println("[서버 응답] " + response);

                message = userInput.readLine();
            }

            System.out.println("프로듀서를 종료합니다.");

        } catch (IOException e) {
            System.err.println("연결 오류: " + e.getMessage());
        }
    }
}
