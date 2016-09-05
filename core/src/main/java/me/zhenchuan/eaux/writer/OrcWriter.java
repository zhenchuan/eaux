package me.zhenchuan.eaux.writer;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.CompressionKind;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

/**
 * 用于直接生产orc格式的文件,减少转换延时.
 * Created by liuzhenchuan@foxmail.com on 3/16/15.
 */
class OrcWriter extends OrcSupport {

    private static final Logger log = LoggerFactory.getLogger(FileWriter.class);

    static final long stripeSize = 256L * 1024 * 1024;
    static final CompressionKind compress = CompressionKind.ZLIB;
    static final int bufferSize = 256 * 1024;
    static final int rowIndexStride = 10000;

    private FileSystem fs ;

    public OrcWriter(String filename ,String[] fieldNames,String[] fieldTypes) throws IOException {
        this.fieldTypes = fieldTypes;
        this.fieldNames = fieldNames ;

        this.fs =  FileSystem.getLocal(new Configuration());

        resetWriter(filename);
    }

    private void resetWriter(String filename) throws IOException {
        Path path = new Path(filename);
        ObjectInspector inspector = createInspector(fieldNames,fieldTypes);
        this.writer = OrcFile.createWriter(fs, path, new Configuration(), inspector,
                stripeSize, compress, bufferSize, rowIndexStride);
    }

    @Deprecated
    public boolean add(String row){
        String[] cols = StringUtils.splitByWholeSeparatorPreserveAllTokens(
                row, "\t");
        if(cols.length == fieldNames.length && cols.length == fieldTypes.length){
            Object[] fieldValues = convert(cols);
            try{
                this.writer.addRow(fieldValues);
            }catch (IOException e){
                 e.printStackTrace();
                return false;
            }
        }else{
            log.warn("error: cols.length:"+cols.length+" ; names.length:"+fieldNames.length+" ;types.length:"+fieldTypes.length+"\n" + row);
        }
        return true;
    }

    public boolean add(Object[] fieldValues) {
        if(fieldValues.length == fieldNames.length && fieldValues.length == fieldTypes.length){
            try{
                this.writer.addRow(fieldValues);
            }catch (IOException e){
                e.printStackTrace();
                return false;
            }
        }else{
            log.warn("error: cols.length:"+fieldValues.length+" ; names.length:"+fieldNames.length+" ;types.length:"+fieldTypes.length+"\n" + Arrays.asList(fieldValues));
        }
        return true;
    }

    public boolean rotateTo(String newFileName) throws IOException {
        close();
        resetWriter(newFileName);
        return true;
    }


    public void close(){
        try {
            this.writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



}
