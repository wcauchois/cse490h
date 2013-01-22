package webserver;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.antlr.stringtemplate.StringTemplate;
import org.antlr.stringtemplate.StringTemplateGroup;

public class Utility {
    public static String evalTemplate(ServiceProvider services, StringTemplate template) {
        // TODO: evaluate includes?
        return template.toString();
    }
    
    // courtesy of BalusC on StackOverflow
    // http://stackoverflow.com/questions/1667278/parsing-query-strings-in-java
    public static Map<String, List<String>> parseQuery(String query) {
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
}
