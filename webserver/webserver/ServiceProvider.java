package webserver;

import org.antlr.stringtemplate.StringTemplate;

public interface ServiceProvider {
    public StringTemplate getTemplate(String name);
}
