public class ServerApplication {

    private static final int SERVER_PORT = 8080;

    public static void main(String[] args) throws Exception {
        ChatServer chatServer = new ChatServer(SERVER_PORT);
        chatServer.run();
    }
}
