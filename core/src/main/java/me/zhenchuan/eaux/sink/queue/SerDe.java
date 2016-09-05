package me.zhenchuan.eaux.sink.queue;

/**
 * Created by liuzhenchuan@foxmail.com on 5/18/15.
 */
public interface SerDe<T> {
    T deserialize(byte[] payload);

    byte[] serialize(T payload);
}
