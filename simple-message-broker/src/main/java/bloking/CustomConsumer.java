package bloking;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

import static bloking.BrokerServerConstants.*;

public class CustomConsumer {

    public static void main(String[] args) {

        try (Socket socket = new Socket(BROKER_SERVER_ADDRESS, BROKER_SERVER_PORT)) {
            PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader userInput = new BufferedReader(new InputStreamReader(System.in));
            BufferedReader brokerReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            System.out.println("컨슈머가 브로커에 연결되었습니다.");

            writer.println(ClientType.CONSUMER.name());
            System.out.println("마지막으로 읽은 오프셋 부터 메시지를 읽으려면 'CONTINUE'를 입력하세요. (종료하려면 'QUIT' 입력):");

            String message = userInput.readLine();
            while (message != null && !message.equalsIgnoreCase(Command.QUIT.name())) {
                String sendMessage = Command.GET.name();
                writer.println(sendMessage);

                printReadMessage(brokerReader);

                message = userInput.readLine();
            }

            System.out.println("컨슈머를 종료합니다.");

        } catch (IOException e) {
            System.err.println("연결 오류: " + e.getMessage());
        }
    }

    private static void printReadMessage(BufferedReader brokerReader) throws IOException {
        boolean firstLine = true;
        String  response;
        while ((response = brokerReader.readLine()) != null) {
            if (response.isBlank()) break;

            if (firstLine) {
                System.out.println("[서버 응답] " + response);
                firstLine = false;
            } else {
                System.out.println(response);
            }
        }
    }
}
