package main;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import edu.washington.cs.cse490h.lib.Node;
import edu.washington.cs.cse490h.lib.PersistentStorageInputStream;
import edu.washington.cs.cse490h.lib.PersistentStorageOutputStream;
import edu.washington.cs.cse490h.lib.PersistentStorageReader;
import edu.washington.cs.cse490h.lib.PersistentStorageWriter;
import edu.washington.cs.cse490h.lib.Utility;

public class FSUtil {
    private Node n;
    public FSUtil(Node n) {
        this.n = n;
        if(Utility.fileExists(n, ".temp")) {
            try {
                PersistentStorageReader tempReader = n.getReader(".temp");
                if(tempReader.ready()) {
                    String filename = tempReader.readLine();
                    String oldContents = getContents(tempReader);
                    PersistentStorageWriter revertFile = n.getWriter(filename, false);
                    revertFile.write(oldContents);
                    revertFile.close();
                }
                tempReader.close();
                n.getWriter(".temp", false).delete();
            } catch(IOException e) { }
        }
        
        // (we need to name the temp file differently so that we know how to interpret it...)
        if(Utility.fileExists(n, ".bin.temp")) {
            try {
                PersistentStorageInputStream tempReader = n.getInputStream(".bin.temp");
                if(tempReader.available() != 0) {
                    ByteArrayOutputStream filenameBytes = new ByteArrayOutputStream();
                    // tempReader.read() reads 4 bytes, not a single byte... so we have to use a byte[1]
                    byte[] buffer = new byte[1];
                    while(buffer[0] != '\n') {
                        if(tempReader.read(buffer, 0, 1) != 1)
                            throw new IllegalStateException("filename was not written properly to .bin.temp!");
                        filenameBytes.write(buffer);
                    }
                    
                    byte[] oldContents = getBinaryContents(tempReader);
                    PersistentStorageOutputStream revertFile = n.getOutputStream(filenameBytes.toString().replace("\n", ""), false);
                    revertFile.write(oldContents);
                    revertFile.flush();
                    revertFile.close();
                }
                tempReader.close();
                n.getWriter(".bin.temp", false).delete();
            } catch(IOException e) { }
        }
    }
    
    public String getContents(PersistentStorageReader reader) throws IOException {
        StringBuffer contents = new StringBuffer();
        char[] buffer = new char[1024];
        int bytesRead = 0;
        while(bytesRead >= 0) {
            bytesRead = reader.read(buffer, 0, 1024);
            if(bytesRead > 0)
                contents.append(buffer, 0, bytesRead);
        }
        return contents.toString();
    }
    
    // TODO: throw an exception if getInfo(filename).access is INVALID ???
    // Read everything from a stream and put it into a buffer.
    public String getContents(String filename) throws IOException {
        PersistentStorageReader reader = n.getReader(filename);
        try {
            return getContents(reader);
        } finally {
            reader.close();
        }
    }
    
    public byte[] getBinaryContents(PersistentStorageInputStream reader) throws IOException {
        // java doesn't have an equivalent ByteBuffer class for appending... 
        // ... redundant with getContents()...
        ByteArrayOutputStream contents = new ByteArrayOutputStream(); 
        byte[] buffer = new byte[1024];
        int bytesRead = 0;
        while(bytesRead >= 0) {
            bytesRead = reader.read(buffer, 0, 1024);
            if(bytesRead > 0)
                contents.write(buffer, 0, bytesRead);
        }
        return contents.toByteArray();
    }
    
    // TODO: throw an exception if getInfo(filename).access is INVALID ???
    // Read everything from a stream and put it into a buffer.
    public byte[] getBinaryContents(String filename) throws IOException {
        PersistentStorageInputStream reader = n.getInputStream(filename);
        try {
            return getBinaryContents(reader);
        } finally {
            reader.close();
        }
    }
    
    // returns whether the file was actually deleted (i think?)
    public boolean deleteFile(String filename) throws IOException {
        return n.getWriter(filename, false).delete();
    }
    
    public void createFile(String filename) throws IOException {
        n.getWriter(filename, false).close();
    }
    
    // Create the file if it does not exist, and write the given the contents
    public void writeContents(String filename, String contents) throws IOException {
        ensureFileExists(filename);
        
        // Vincent's magical error-proof put method
        String oldContents = getContents(n.getReader(filename));
        PersistentStorageWriter tempWriter = n.getWriter(".temp", false);
        tempWriter.write(filename + "\n" + oldContents);
        PersistentStorageWriter newFile = n.getWriter(filename, false);
        newFile.write(contents);
        tempWriter.delete();
        newFile.close();
    }
    
    public void writeBinaryContents(String filename, byte[] contents) throws IOException {
        // XXX: what if contents has a midstream byte == EOF?
        ensureFileExists(filename);
        
        // Vincent's magical error-proof put method
        byte[] oldContents = getBinaryContents(filename);
        PersistentStorageOutputStream tempWriter = n.getOutputStream(".bin.temp", false);
        // we must put both the filename and the contents to .bin.tmp in a single write...
        ByteArrayOutputStream tempContents = new ByteArrayOutputStream();
        tempContents.write((filename + "\n").getBytes());
        tempContents.write(oldContents);
        tempWriter.write(tempContents.toByteArray());
        PersistentStorageOutputStream newFile = n.getOutputStream(filename, false);
        newFile.write(contents);
        tempWriter.delete();
        newFile.close();
    }
    
    private void ensureFileExists(String filename) throws IOException {
        if(!Utility.fileExists(n, filename)) {
            n.getWriter(filename, false).close();
        }
    }
    
    // Called for the APPEND command to append stuff to the end of a file.
    public void appendContents(String filename, String contents) throws IOException {
        // We don't have to worry about Vincent's magic here, since fetching an append writer doesn't overwrite any contents
        PersistentStorageWriter writer = n.getWriter(filename, true);
        writer.write(contents);
        writer.close();
    }
    
    /**
     * @param filename
     * @return whether the file exists on disc
     */
    public boolean fileExists(String filename) {
        return Utility.fileExists(n, filename);
    }
    
    /**
     * Get the id from idFile
     * 
     * @param idFile
     * @return the id stored in idFile, or null if no id has been written yet
     */
    public Integer getID(String idFile) {
        if (fileExists(idFile)) {
            try {
                return Integer.parseInt(getContents(idFile));
            } catch (NumberFormatException e) {
                return null; // it's safe to return null here -- we must have crashed before writing to the file, 
                //  but after ensuring the file exists (empty file)
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(-1);
            }
        } 
        
        return null;  // no ID file has been written yet
    }
    
    /**
     * persist nextVal to disc
     * 
     * @param idFile where to write the ID
     * @param nextVal the ID to write
     */
    public void updateID(String idFile, int nextVal) {
        try {
            writeContents(idFile, "" + nextVal);
        } catch (IOException e) {
            e.printStackTrace(); // FUCKING CHECKED EXCEPTIONS MAKE ME WANT TO KILL MYSELF
            System.exit(-1);
        }
    }
}
