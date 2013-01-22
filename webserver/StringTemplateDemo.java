import org.antlr.stringtemplate.*;
import org.antlr.stringtemplate.language.*;

public class StringTemplateDemo {
    public static void main(String[] args) {
        StringTemplate hello = new StringTemplate("Hello, $name$", DefaultTemplateLexer.class);
        hello.setAttribute("name", "World");
        System.out.println(hello.toString());
        // wow, that was easy!!
    }
}
