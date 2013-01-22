package transactions;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.washington.cs.cse490h.lib.Node;
import edu.washington.cs.cse490h.lib.PersistentStorageWriter;
import edu.washington.cs.cse490h.lib.Utility;

public class UndoRedoLog {
    /*
     * HOW TO ADD OR MODIFY LOG ENTRY TYPES
     * a simple stupid guide by bill!
     * 
     * there are n things you need to modify
     * (1) add (or modify) the appropriate data-model class that's a descendant of Entry
     *     - you want the members of this class to correspond to the parts of your log entry
     *     - note that every entry, by default, has an EntryType
     *     - and if you're descended from FileModEntry you have a filename, version, and contents
     *     - make sure to overload getName() to return a string identifying your log entry type
     * (2) add an appropriate field to the EntryType enum
     * (3) you're going to need to add a branch to the giant if-statement in parseLogFile
     *     - else if(line.startsWith("YOUR LOG ENTRY TYPE HERE IN ALL CAPS")) {
     *     -     [parse from the string, line]
     *     -     [then, add an instance of the data-model class to the entries list]
     * (4) now you need to modify recover()
     *     - this is pretty dumb, you just need to go through and invoke the appropriate method on the Undoer
     * (5) oh by the way you should probably add a method to the Undoer
     * (6) now you'll need to add a method like logTxStart() or logWrite(), except it's called logYourThing()
     *     - you want to use appendLine(str) to append to the log file
     *     - this is where you do serialization (the opposite of what parseLogFile does)
     * (7) [optional] modify MockUndoLog, so you can actually test your shit
     * 
     * i think from writing this it's clear that the design of this class could be better in a lot of ways.
     * i designed it to be simple, and then it needed to be complex. ah, such is software!!
     */
    
    private Node n;
    protected List<Entry> logEntries = new ArrayList<Entry>();
    protected String logFilename;
    
    /**
     * Initialize the UndoLog. Note that this does NOT recover previous state! You
     * should be sure to explicitly call recover() immediately after initializing the
     * UndoLog, to ensure that you recover from failures correctly.
     * @param n
     * @param logFilename
     */
    public UndoRedoLog(Node n, String logFilename) {
        this.n = n;
        this.logFilename = logFilename;
        if(doesLogFileExist())
            logEntries.addAll(parseLogFile());
    }
    
    protected boolean doesLogFileExist() {
        return Utility.fileExists(n, logFilename);
    }
    
    protected boolean deleteLogFile() {
        try {
            return n.getWriter(logFilename, false).delete();
        } catch(IOException e) { /* don't care */ }
        return false;
    }
    
    // protected so we can override this for when we're testing
    protected void appendLine(String line) {
        try {
            // create the log file if it does not already exist
            boolean appending = Utility.fileExists(n, logFilename);
            PersistentStorageWriter writer = n.getWriter(logFilename, appending);
            writer.append(line + "\n");
            writer.flush();
            writer.close();
        } catch(IOException e) {
            // TODO: what do?
        }
    }
    
    protected enum EntryType {
        TXSTART,
        TXCOMMIT,
        WRITE,
        CREATE,
        DELETE;
    }
    
    protected abstract class Entry {
        public abstract EntryType getType();
    }
    
    protected class TxStartEntry extends Entry {
        private int pxId;
        
        @Override
        public EntryType getType() {
            return EntryType.TXSTART;
        }
        public String toString() {
            return "TxStart()";
        }
        
        public TxStartEntry(int pxId) {
            this.pxId = pxId;
        }
        public int getPaxosId() {
            return pxId;
        }
    }
    
    protected class TxCommitEntry extends Entry {
        @Override
        public EntryType getType() {
            return EntryType.TXCOMMIT;
        }
        public String toString() {
            return "TxCommit()";
        }
    }
    private TxCommitEntry txCommit = new TxCommitEntry(); // singleton, don't need any more
    
    protected abstract class FileModEntry extends Entry { 
        private String filename;
        private int version;
        private String contents;
        
        public String getFilename() {
            return filename;
        }
        public int getVersion() {
            return version;
        }
        public String getContents() { 
            return contents;
        }
        
        public FileModEntry(String filename, int version, String contents) {
            this.filename = filename;
            this.version = version;
            this.contents = contents;
        }
        @Override
        public String toString() {
            return getName() + "(" + filename + ", " + version + ", " + contents + ")";
        }
        protected abstract String getName();
    }
    
    protected class WriteEntry extends FileModEntry {
        public WriteEntry(String filename, int version, String contents) {
            super(filename, version, contents);
        }
        @Override
        public EntryType getType() {
            return EntryType.WRITE;
        }
        @Override
        protected String getName() {
            return "Write";
        }
    }
    
    protected class CreateEntry extends Entry {
        private String filename;
        
        public String getFilename() {
            return filename;
        }
        
        @Override
        public EntryType getType() {
            return EntryType.CREATE;
        }
        public CreateEntry(String filename) {
            this.filename = filename;
        }
        public String toString() {
            return "Create(" + filename + ")";
        }
    }
    
    protected class DeleteEntry extends FileModEntry {
        public DeleteEntry(String filename, int version, String contents) {
            super(filename, version, contents);
        }
        @Override
        public EntryType getType() {
            return EntryType.DELETE;
        }
        @Override
        protected String getName() {
            return "Delete";
        }
    }
    
    private Pattern filenameVersionPattern = Pattern.compile("(.+?)\\((\\d+)\\)");
    
    protected List<Entry> parseLogFile() {
        try {
            return parseLogFile(n.getReader(logFilename));
        } catch(FileNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }
    protected List<Entry> parseLogFile(BufferedReader reader) {
        List<Entry> entries = new ArrayList<Entry>();
        try {
            while(true) {
                reader.mark(512); // mark the current position in the stream
                String line = reader.readLine();
                if(line == null)
                    break; // EOF
                
                if(line.startsWith("TXSTART")) {
                    int pxId = Integer.parseInt(line.split(" ")[1]);
                    entries.add(new TxStartEntry(pxId));
                } else if(line.startsWith("TXCOMMIT")) {
                    entries.add(txCommit);
                } else if(line.startsWith("WRITE") || line.startsWith("DELETE")) {
                    String[] parts = line.split(":");
                    String beforeColon = parts[0];
                    String[] subParts = beforeColon.split(" ");
                    Matcher filenameVersionMatcher = filenameVersionPattern.matcher(subParts[1]);
                    filenameVersionMatcher.matches();
                    String filename = filenameVersionMatcher.group(1);
                    int version = Integer.parseInt(filenameVersionMatcher.group(2));
                    int contentsLength = Integer.parseInt(subParts[2]);
                    reader.reset();
                    reader.skip(parts[0].length() + 1); // XXX should this be 1 or 2??
                    StringBuffer contents = new StringBuffer();
                    for(int i = 0; i < contentsLength; i++) {
                        int c = reader.read();
                        if(c == -1) {
                            throw new RuntimeException("unexpected end of file");
                        }
                        contents.append((char)c);
                    }
                    if(line.startsWith("WRITE")) {
                        entries.add(new WriteEntry(filename, version, contents.toString()));
                    } else { // line.startsWith("DELETE")
                        entries.add(new DeleteEntry(filename, version, contents.toString()));
                    }
                } else if(line.startsWith("CREATE")) {
                    String filename = line.split(" ")[1];
                    entries.add(new CreateEntry(filename));
                }
            }
        } catch(IOException e) {
            // XXX: what do?
        }
        return entries;
    }
    
    public interface UndoRedoer {
        void undoWrite(String filename, int oldVersion, String oldContents);
        void undoDelete(String filename, int oldVersion, String oldContents);
        void undoCreate(String filename);
        
        void beginRedo(int pxId);
        void redoWrite(String filename, int oldVersion, String oldContents);
        void redoDelete(String filename, int oldVersion, String oldContents);
        void redoCreate(String filename);
        void endRedo(int pxId);
        
        void onRecoveryComplete();
    }
    
    public void recover(UndoRedoer u) {
        boolean insideTransaction = false;
        Queue<Entry> undoRedoEntries = new LinkedList<Entry>();
        // read through the log, starting from the bottom
        // remember that everything is reversed!!
        for(int i = logEntries.size() - 1; i >= 0; i--) {
            Entry entry = logEntries.get(i);
            switch(entry.getType()) {
            case TXCOMMIT:
                if(insideTransaction)
                    throw new RuntimeException("detected nested transaction");
                insideTransaction = true;
                break;
            case TXSTART:
                if(!insideTransaction) {
                    // we found a TXSTART with no corresponding TXCOMMIT; undo the changes
                    while(!undoRedoEntries.isEmpty()) {
                        Entry undoEntry = undoRedoEntries.poll();
                        switch(undoEntry.getType()) {
                        case WRITE:
                            WriteEntry w = (WriteEntry)undoEntry;
                            u.undoWrite(w.getFilename(), w.getVersion(), w.getContents());
                            break;
                        case CREATE:
                            CreateEntry c = (CreateEntry)undoEntry;
                            u.undoCreate(c.getFilename());
                            break;
                        case DELETE:
                            DeleteEntry d = (DeleteEntry)undoEntry;
                            u.undoDelete(d.getFilename(), d.getVersion(), d.getContents());
                            break;
                        }
                    }
                } else {
                    // this has been a valid transaction. yay!
                    insideTransaction = false;
                    
                    TxStartEntry startEntry = (TxStartEntry)entry;
                    u.beginRedo(startEntry.getPaxosId());
                    while(!undoRedoEntries.isEmpty()) { // wish i could factor this
                        Entry redoEntry = undoRedoEntries.poll();
                        switch(redoEntry.getType()) {
                        case WRITE:
                            WriteEntry w = (WriteEntry)redoEntry;
                            u.redoWrite(w.getFilename(), w.getVersion(), w.getContents());
                            break;
                        case CREATE:
                            CreateEntry c = (CreateEntry)redoEntry;
                            u.redoCreate(c.getFilename());
                            break;
                        case DELETE:
                            DeleteEntry d = (DeleteEntry)redoEntry;
                            u.redoDelete(d.getFilename(), d.getVersion(), d.getContents());
                            break;
                        }
                    }
                    u.endRedo(startEntry.getPaxosId());
                    
                    undoRedoEntries.clear();
                }
                // since we're assuming transactions aren't interleaved, we can
                // return straightaway
                return;
            case WRITE:
            case CREATE:
            case DELETE:
                undoRedoEntries.add(entry);
                break;
            }
        }
        deleteLogFile();
        logEntries.clear();
        u.onRecoveryComplete();
    }
    
    private void appendWriteOrDelete(String key, String filename, int oldVersion, String oldContents) {
        StringBuffer buffer = new StringBuffer();
        buffer.append(key + " ");
        buffer.append(filename);
        buffer.append("(" + oldVersion + ") ");
        buffer.append(oldContents.length() + ":" + oldContents); // "bencoded" contents string thingy
        appendLine(buffer.toString());
    }
    
    /**
     * @param filename the file that's going to be written to
     * @param oldVersion it's version, prior to the update
     * @param oldContents it's contents, prior to the update
     */
    public void logWrite(String filename, int oldVersion, String oldContents) {
        appendWriteOrDelete("WRITE", filename, oldVersion, oldContents);
        logEntries.add(new WriteEntry(filename, oldVersion, oldContents));
    }
    
    public void logCreate(String filename) {
        appendLine("CREATE " + filename);
        logEntries.add(new CreateEntry(filename));
    }
    
    public void logDelete(String filename, int oldVersion, String oldContents) {
        appendWriteOrDelete("DELETE", filename, oldVersion, oldContents);
        logEntries.add(new DeleteEntry(filename, oldVersion, oldContents));
    }
    
    public void logTxStart(int pxId) {
        appendLine("TXSTART " + pxId);
        logEntries.add(new TxStartEntry(pxId));
    }
    public void logTxCommit() {
        appendLine("TXCOMMIT");
        logEntries.add(txCommit);
    }
    
    /**
     * alias for recover()
     * @param u the Undoer to use
     */
    public void txAbort(UndoRedoer u) {
        recover(u);
    }
}

class MockUndoLog extends UndoRedoLog {
    private class MockUndoer implements UndoRedoer {
        @Override
        public void undoWrite(String filename, int oldVersion, String oldContents) {
            System.out.println("\tundoing WRITE " + filename + "(" + oldVersion + "): " + oldContents);
        }
        @Override
        public void undoDelete(String filename, int oldVersion, String oldContents) {
            System.out.println("\tundoing DELETE " + filename + "(" + oldVersion + "): " + oldContents);
        }
        @Override
        public void undoCreate(String filename) {
            System.out.println("\tundoing CREATE " + filename);
        }
        @Override
        public void beginRedo(int pxId) {
        }
        @Override
        public void endRedo(int pxId) {
        }
        @Override
        public void onRecoveryComplete() {
        }
        @Override
        public void redoCreate(String filename) {
        }
        @Override
        public void redoDelete(String filename, int oldVersion,
                String oldContents) {
        }
        @Override
        public void redoWrite(String filename, int oldVersion,
                String oldContents) {
        }
        
    }
    public MockUndoLog(String logFilename) {
        super(null, logFilename);
        // we want to start from scratch
        new File(logFilename).delete();
    }
    @Override
    protected void appendLine(String line) {
        try {
            new File(logFilename).createNewFile(); // creates the file if it doesn't exist
            FileWriter fwriter = new FileWriter(logFilename, true);
            BufferedWriter bwriter = new BufferedWriter(fwriter);
            bwriter.write(line + "\n");
            bwriter.close();
        } catch(IOException e) {
            e.printStackTrace();
        }
    }
    @Override
    protected boolean doesLogFileExist() {
        // return false, so we don't load it...
        return false;
    }
    @Override
    protected boolean deleteLogFile() {
        return true; // do nothing
    }
    @Override
    protected List<Entry> parseLogFile() {
        try {
            return parseLogFile(new BufferedReader(new FileReader(logFilename)));
        } catch(FileNotFoundException e) { /* don't care */ }
        return null;
    }
    public void dumpLogFile() {
        List<Entry> entries = parseLogFile();
        
        System.out.println("parsed " + logFilename + ", here's what it looks like:");
        for(Entry e : entries) {
            System.out.println("\t" + e);
        }
    }
    /**
     * Recover using the mock Undoer, that just prints out all the undos.
     */
    public void recover() {
        System.out.println("recovering using " + logFilename);
        super.recover(new MockUndoer());
    }
}