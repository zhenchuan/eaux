package me.zhenchuan.eaux.ext.kafka;

/**
 * Created by liuzhenchuan@foxmail.com on 12/23/15.
 */
public interface MessageProcessor {

    boolean process(String topic,byte[] message);

}
