package me.zhenchuan.eaux;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;

import java.io.Serializable;

/**
 * Created by liuzhenchuan@foxmail.com on 8/4/15.
 */
public class Row implements Serializable{
    /****
     *
     */
    protected Object[] fields;
    /****
     *
     */
    protected String group;
    /****
     * the estimated size of {fields} , the max size will be preferred ~
     */
    protected int estimateSize;


    public Row(Object[] fields,int estimateSize) {
        this.fields = fields;
        this.group = "";
        this.estimateSize = estimateSize;
    }

    public Row(Object[] fields, String group, int estimateSize) {
        this.fields = fields;
        this.group = group;
        this.estimateSize = estimateSize;
    }

    public Object[] fields() {
        return fields;
    }

    public String group() {
        return group;
    }

    public int estimateSize() {
        return estimateSize;
    }


}
