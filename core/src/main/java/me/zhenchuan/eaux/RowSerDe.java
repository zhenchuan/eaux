package me.zhenchuan.eaux;

/**
 * Created by liuzhenchuan@foxmail.com on 8/4/15.
 */
public interface RowSerDe<T extends Row> {

    T deserialize(byte[] payload);

    byte[] serialize(T payload);

}
