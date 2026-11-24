package io.mq.domain;

public enum Acks {
    NONE((byte) 0), LEADER((byte) 1), ALL((byte) -1);
    public final byte code;
    Acks(byte c) { code = c; }
    public static Acks of(byte c) {
        return switch (c) { case 0 -> NONE; case 1 -> LEADER; default -> ALL; };
    }
}
