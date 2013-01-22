package main;

public class ErrorCode {
    public static final int ERR_NONE = 0;
    public static final int ERR_OUTDATED_FILE_VERSION = 5;
    public static final int ERR_ILLEGAL_OPERATION = 6;
    public static final int ERR_FILE_NOT_FOUND = 10; 
    public static final int ERR_FILE_EXISTS = 11;
    public static final int ERR_IO_EXCEPTION = 12;
    public static final int ERR_TIMEOUT = 20;
    public static final int ERR_FILE_TOO_LARGE = 30;
    public static final int ERR_TRANSACTION_FAILED = 40;
    
    // Error code associated with RPC calls -- indicates that the RPC service [server]
    // has failed, meaning your call may or may not have completed (but either
    // way, you're definitely not going to get the result :).
    public static final int ERR_SERVICE_FAILURE = 55;
    
    public static String codeToString(int errorCode) {
        // TODO: use reflection?
        switch(errorCode) {
        case ERR_NONE:                      return "ERR_NONE";
        case ERR_OUTDATED_FILE_VERSION:     return "ERR_OUTDATED_FILE_VERSION";
        case ERR_ILLEGAL_OPERATION:         return "ERR_ILLEGAL_OPERATION";
        case ERR_FILE_NOT_FOUND:            return "ERR_FILE_NOT_FOUND"; 
        case ERR_FILE_EXISTS:               return "ERR_FILE_EXISTS";
        case ERR_IO_EXCEPTION:              return "ERR_IO_EXCEPTION";
        case ERR_TIMEOUT:                   return "ERR_TIMEOUT";
        case ERR_FILE_TOO_LARGE:            return "ERR_FILE_TOO_LARGE";
        case ERR_TRANSACTION_FAILED:        return "ERR_TRANSACTION_FAILED";
        default:                            return "Unknown";
        }
    }
}
