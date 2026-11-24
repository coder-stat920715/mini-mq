package io.mq.client;

public class ClientException extends RuntimeException {
    private final short code;
    public ClientException(short code, String msg) { super(msg + " (error " + code + ")"); this.code = code; }
    public ClientException(String msg, Throwable cause) { super(msg, cause); this.code = -1; }
    public short code() { return code; }
}
