package io.mq.domain;

/** Wire-level error codes. 0 = success. */
public final class Errors {
    private Errors() {}
    public static final short NONE = 0;
    public static final short OFFSET_OUT_OF_RANGE = 1;
    public static final short UNKNOWN_TOPIC_OR_PARTITION = 3;
    public static final short NOT_ENOUGH_REPLICAS = 5;
    public static final short OUT_OF_ORDER_SEQUENCE = 6;
    public static final short PRODUCER_FENCED = 7;
    public static final short ILLEGAL_GENERATION = 8;
    public static final short REBALANCE_IN_PROGRESS = 9;
    public static final short UNKNOWN_MEMBER_ID = 10;
    public static final short INVALID_REQUEST = 11;
    public static final short TOPIC_EXISTS = 12;
    public static final short INVALID_TOPIC = 13;
    public static final short UNKNOWN_SERVER_ERROR = 99;

    public static boolean retriable(short code) { return code == NOT_ENOUGH_REPLICAS; }
}
