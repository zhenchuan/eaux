package me.zhenchuan.eaux.writer;

import me.zhenchuan.eaux.Row;

import java.io.IOException;

/**
 * Created by liuzhenchuan@foxmail.com on 7/19/15.
 */
public interface Writer {

    //boolean add(String[] items);

//    boolean add(Object[] fieldValues);

    boolean add(Row message);

    boolean rotateTo(String newFileName) throws IOException;

    void close();

}
