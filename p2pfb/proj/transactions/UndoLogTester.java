package transactions;

import java.util.*;
import java.io.*;

public class UndoLogTester {
    private static void catFile(String filename) {
        try {
            Scanner sc = new Scanner(new File(filename));
            while(sc.hasNext())
                System.out.println(sc.nextLine());
        } catch(IOException e) { /* don't care */ }
    }
    public static final String LOG_FNAME = "/tmp/undo.log";
    public static void main(String[] args) {
        MockUndoLog undoLog = new MockUndoLog(LOG_FNAME);
        undoLog.logTxStart(1);
        undoLog.logWrite("foo.txt", 0, "hello\n_world");
        undoLog.logTxCommit();
        
        undoLog.logTxStart(2);
        undoLog.logWrite("bar.txt", 2, "whee");
        undoLog.logCreate("foo.txt");
        undoLog.logDelete("foo.txt", 1, "bananas");
        
        catFile(LOG_FNAME);
        undoLog.dumpLogFile();
        undoLog.recover();
    }
}
