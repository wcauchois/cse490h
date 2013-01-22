import java.io.*;
import java.util.*;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.util.concurrent.Executors;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import org.antlr.stringtemplate.*;
import org.antlr.stringtemplate.language.*;


// lol, what a class name :P
public class StringHttpServerTemplate {
    public static void main(String[] args) throws IOException {
        InetSocketAddress addr = new InetSocketAddress(8080);
        HttpServer server = HttpServer.create(addr, 0);

        server.createContext("/", new MyNewHandler());
        server.setExecutor(Executors.newCachedThreadPool());
        server.start();
        System.out.println("Server is listening on port 8080" );
    }
}

class MyNewHandler implements HttpHandler {
    // courtesy of BalusC on StackOverflow
    // http://stackoverflow.com/questions/1667278/parsing-query-strings-in-java
    private Map<String, List<String>> parseQuery(String query) {
        Map<String, List<String>> params = new HashMap<String, List<String>>();
        if(query != null && query.length() > 0) {
            for (String param : query.split("&")) {
                String[] pair = param.split("=");
                
                String key = "", value = "";
                try {
                    key = URLDecoder.decode(pair[0], "UTF-8");
                    value = URLDecoder.decode(pair[1], "UTF-8");
                } catch(UnsupportedEncodingException e) { /* balls */ }
                List<String> values = params.get(key);
                if (values == null) {
                    values = new ArrayList<String>();
                    params.put(key, values);
                }
                values.add(value);
            }
        }
        return params;
    }
    public void handle(HttpExchange exchange) throws IOException {
      String requestMethod = exchange.getRequestMethod();
      if(requestMethod.equalsIgnoreCase("GET")) {
        Headers responseHeaders = exchange.getResponseHeaders();
        responseHeaders.set("Content-Type", "text/plain");
        exchange.sendResponseHeaders(200, 0);

        OutputStream responseBody = exchange.getResponseBody();
        // lol @ this line right here
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(responseBody));
        
        String query = exchange.getRequestURI().getQuery();
        List<String> names = parseQuery(query).get("name");
        String name = "";
        if(names.size() > 0)
            name = names.get(0);
        
        
        StringTemplate hello = new StringTemplate("Hello, $name$", DefaultTemplateLexer.class);
        hello.setAttribute("name", name);
        writer.write(hello.toString());
        
        writer.flush(); // gotta flush!!!
        responseBody.close();
      }
    }
  }