public class ClientApplication {

    private static final String SERVER_HOST = "localhost";
    private static final int SERVER_PORT = 8080;

    public static void main(String[] args) throws Exception {
        ChatClient chatClient = new ChatClient(SERVER_HOST, SERVER_PORT);
        chatClient.run();
    }
}
