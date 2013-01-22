package handlers;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

public class FooHandler implements HttpHandler {
    @Override
    public void handle(HttpExchange exchange) throws IOException {
        String requestMethod = exchange.getRequestMethod();
        if(requestMethod.equalsIgnoreCase("GET")) {
          Headers responseHeaders = exchange.getResponseHeaders();
          responseHeaders.set("Content-Type", "text/plain");
          exchange.sendResponseHeaders(200, 0);

          OutputStream responseBody = exchange.getResponseBody();
          BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(responseBody));
          writer.write("Welcome to foo :D");
          writer.flush();
          responseBody.close();
        }
    }
}
