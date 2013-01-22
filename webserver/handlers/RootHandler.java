package handlers;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import org.antlr.stringtemplate.StringTemplate;

import webserver.HandlerBase;
import webserver.ServiceProvider;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

public class RootHandler extends HandlerBase {
    public RootHandler(ServiceProvider s) {
        super(s);
    }
    
    protected void prepareTemplate(StringTemplate template, HttpExchange exchange) {
    }
    
    protected String getTemplateName() {
        return "homepage";
    }
}
