package main;

public class AppUtil {
    // Whether to actually use colors (set this to false if you're piping to a log or
    // somewhere where the color commands are annoying to look at).
    public static boolean USE_COLORS = true;
    
    /**
     * Parse a string of the form "command arg1 arg2... argN" where argN may contain
     * additional spaces that are included as part of the argument.
     * @param the number of args to parse
     * @return [arg1, arg2, ..., argN]
     * @throws IllegalArgumentException if s contains too few args
     */
    public static String[] splitArgs(String s, int nargs) {
        if(nargs == 1)
            return new String[] { s };
        
        String[] result = new String[nargs];
        for(int i = 0; i < nargs - 1; i++) {
            int afterChunk = s.indexOf(' ');
            if(afterChunk < 0)
                throw new IllegalArgumentException(); // error
            result[i] = s.substring(0, afterChunk);
            s = s.substring(afterChunk + 1);
        }
        result[nargs - 1] = s;
        return result;
    }
    
    public static final String COLOR_PURPLE = "1;35";
    public static final String COLOR_BLUE = "0;34";
    public static final String COLOR_RED = "0;31";
    public static final String COLOR_CYAN = "0;36";
    public static final String COLOR_GREEN = "0;32";
    
    public static void printColored(String s, String colorCommand) {
        if(USE_COLORS)
            System.out.println((char)27 + "[" + colorCommand + "m" + s + (char)27 + "[m");
        else
            System.out.println(s);
    }
}
