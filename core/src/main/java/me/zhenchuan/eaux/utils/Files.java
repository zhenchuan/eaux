package me.zhenchuan.eaux.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;

/**
 * Created by liuzhenchuan@foxmail.com on 1/22/15.
 */
public class Files {

    private static final Logger logger = LoggerFactory.getLogger(Files.class);

    public static String getFileExt(String fileName) {
        int dotPos = fileName.lastIndexOf('.');
        if (dotPos != -1 && dotPos != fileName.length() - 1) {
            return fileName.substring(dotPos);
        } else {
            return null;
        }
    }

    public static File concat(final File output ,List<File> fileList){
        try{
            ByteBuffer[] byteBuffers = new ByteBuffer[fileList.size()];
            for(int i = 0 ; i < fileList.size() ;i++){
                RandomAccessFile raf = new RandomAccessFile(fileList.get(i),"r");
                FileChannel channel = raf.getChannel();
                byteBuffers[i] = channel.map(FileChannel.MapMode.READ_ONLY,0,raf.length());
                channel.close();
            }
            FileOutputStream fileOutputStream = new FileOutputStream(output);
            FileChannel fileChannel = fileOutputStream.getChannel();
            fileChannel.write(byteBuffers);
            fileChannel.close();
        }catch (IOException e){
            output.delete();
        }
        return output;
    }

    private static final Method cleanerMethod;

    static {
        Method m;
        try {
            m = Class.forName("sun.nio.ch.DirectBuffer").getMethod("cleaner");
        } catch (Exception e) {
            // Perhaps a non-sun-derived JVM - contributions welcome
            logger.info("Cannot initialize un-mmaper.  (Are you using a non-SUN JVM?)  Compacted data files will not be removed promptly.  Consider using a SUN JVM or using standard disk access mode");
            m = null;
        }
        cleanerMethod = m;
    }

    public static void clean(MappedByteBuffer buffer) {
        try {
            Object cleaner = cleanerMethod.invoke(buffer);
            cleaner.getClass().getMethod("clean").invoke(cleaner);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }


}
