package main;

import primitives.Handle;

public interface CacheClient {
    /**
     * Get a file. The handle passes back the file in its entirety if the call was
     * successful. If the file is already in our cache, returns immediately,
     * else asks the manager for read access to the file. 
     *
     * @param filename the file to get
     * @return a handle for the result, returning the contents of the file on
     * success or ERR_FILE_NOT_FOUND_EXCEPTION if the file did not exist.
     */
    Handle<String> get(String filename);
    
    /**
     * Write the contents to the given file. If the file is already R/W in
     * our cache, will return immediately, else asks the manager for write
     * access to the file and then performs the put in our cache.
     *
     * @param filename the file to write to
     * @param contents what to write
     * @return a handle for the results of the call (NULL if the call was
     * successful, or an error if the file does not exist)
     */
    Handle<Object> put(String filename, String contents);
    
    /**
     * Append the contents to the given file. If the file is already R/W in
     * our cache, will return immediately, else asks the manager for write
     * access to the file and then performs the append in our cache.
     *
     * @param filename the file to write to
     * @param contents what to write
     * @return a handle for the results of the call (NULL if the call was
     * successful, or an error if the file does not exist)
     */
    Handle<Object> append(String filename, String contents);
    
    /**
     * This command creates an empty file. When the create() call returns
     * succesfully, a R/W copy of the file is placed in this node's cache.
     * When the given file already exists, the handle will return ERR_FILE_ALREADY_EXISTS
     * 
     * @return a handle for the eventual result of the create() call
     */
    Handle<Object> create(String filename);
    
    /**
     * This command deletes the file called filename. 
     * 
     * @param filename the file to delete
     * @return a handle for the result of the call. The handle returns
     * NULL on success, or ERR_FILE_NOT_FOUND if the file did not
     * exist.
     */
    Handle<Object> delete(String filename);
}
