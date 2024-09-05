package ca.yorku.eecs;

import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;

public class App {
    static int PORT = 8080;

    public static void main(String[] args) throws IOException {

        HttpServer server = HttpServer.create(new InetSocketAddress("0.0.0.0", PORT), 0);


        // Handling  context for  REST API endpoint requests
        // The context will be handled by an instance of SixDegreesOfKevinBacon
        server.createContext("/api/v1", new SixDegreesOfKevinBacon());


        server.start();
        System.out.printf("Server started on port %d...\n", PORT);
    }
}
