package webserver;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import org.antlr.stringtemplate.StringTemplate;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

public class HandlerBase implements HttpHandler {
    private ServiceProvider services;
    
    public HandlerBase(ServiceProvider services) {
        this.services = services;
    }
    
    protected StringTemplate getTemplate(String name) {
        return services.getTemplate(name);
    }
    
    @Override
    public void handle(HttpExchange exchange) throws IOException {
        String requestMethod = exchange.getRequestMethod();
        if(requestMethod.equalsIgnoreCase("GET")) {
            Headers responseHeaders = exchange.getResponseHeaders();
            responseHeaders.set("Content-Type", getContentType());
            exchange.sendResponseHeaders(200, 0);
            
            OutputStream outStream = exchange.getResponseBody();
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outStream));
            writer.write(getResponseBody(exchange));
            writer.flush();
            outStream.close();
        }
    }
    
    protected String getResponseBody(HttpExchange exchange) {
        // default implementation: use the template
        StringTemplate template = getTemplate(getTemplateName());
        prepareTemplate(template, exchange);
        return Utility.evalTemplate(services, template);
    }
    
    protected void prepareTemplate(StringTemplate template, HttpExchange exchange) { }

    protected String getContentType() {
        return "text/html";
    }
    
    protected String getTemplateName() {
        return null;
    }
}
