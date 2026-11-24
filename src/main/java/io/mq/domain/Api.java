package io.mq.domain;

/** Request api keys. Frame: [len:int32][api:int16][correlationId:int32][body]. */
public final class Api {
    private Api() {}
    public static final short CREATE_TOPIC = 1;
    public static final short PRODUCE = 2;
    public static final short FETCH = 3;
    public static final short COMMIT_OFFSET = 4;
    public static final short FETCH_OFFSET = 5;
    public static final short JOIN_GROUP = 6;
    public static final short HEARTBEAT = 7;
    public static final short LEAVE_GROUP = 8;
    public static final short METADATA = 9;
    public static final short INIT_PRODUCER_ID = 10;

    /** Special fetch offsets. */
    public static final long EARLIEST = -2L;
    public static final long LATEST = -1L;
}
