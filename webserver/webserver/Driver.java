package webserver;

import java.io.*;
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.antlr.stringtemplate.StringTemplate;
import org.antlr.stringtemplate.StringTemplateGroup;
import org.antlr.stringtemplate.language.DefaultTemplateLexer;

import com.google.gson.Gson;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

public class Driver {
    public static final String HANDLERS_FILE = "handlers.json";
    public static final String TEMPLATE_DIR = "templates";
    
    private static class Services implements ServiceProvider {
        private StringTemplateGroup templateGroup;
        
        @Override
        public StringTemplate getTemplate(String name) {
            return templateGroup.getInstanceOf(name);
        }

        public Services(StringTemplateGroup templateGroup) {
            this.templateGroup = templateGroup;
        }
    }
    
    public static void main(String[] args) throws IOException {
        InetSocketAddress addr = new InetSocketAddress(8080);
        HttpServer server = HttpServer.create(addr, 0);

        StringTemplateGroup templateGroup = new StringTemplateGroup(
                "templates", TEMPLATE_DIR, DefaultTemplateLexer.class);
        Services services = new Services(templateGroup);
        
        // initialize the handlers
        Gson gson = new Gson();
        FileReader reader = new FileReader(HANDLERS_FILE);
        HandlerEntry[] handlers = gson.fromJson(reader, HandlerEntry[].class);
        try {
            for(HandlerEntry entry : handlers) {
                Constructor<?> cons = Class.forName(entry.handler).getConstructor(ServiceProvider.class);
                HandlerBase handler = (HandlerBase)cons.newInstance(services);
                server.createContext(entry.path, handler);
            }
        } catch(Exception e) {
            System.err.println("Failed to initialize");
            e.printStackTrace();
            System.exit(1);
        }
        
        server.setExecutor(Executors.newCachedThreadPool());
        server.start();
        System.out.println("Server is listening on port 8080" );
    }
}

class HandlerEntry {
    public String path;
    public String handler;
}