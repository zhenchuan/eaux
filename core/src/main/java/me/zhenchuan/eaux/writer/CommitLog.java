package me.zhenchuan.eaux.writer;

import com.google.common.base.Joiner;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

/**
 * Created by liuzhenchuan@foxmail.com on 7/3/15.
 */
public class CommitLog {

    public static final int END_OF_SEGMENT_MARKER = 0;
    private static final int END_OF_SEGMENT_MARKER_SIZE = 4;
    private static final int ENTRY_OVERHEAD_SIZE = 4 /*行长*/ + 0;

    private final MappedByteBuffer buffer;

    private byte[] readBuffer;

    private File commitLogFile;

    private long entryNumbers = 0;

    private Joiner joiner = Joiner.on("\u0009");

    private CommitLog(File commitLogFile,int maxFileSize,Processor processor) throws IOException {
        this.commitLogFile = commitLogFile ;
        this.readBuffer = new byte[4096];

        RandomAccessFile logFileAccessor = new RandomAccessFile(commitLogFile, "rw");
        logFileAccessor.setLength((long) (maxFileSize * 1.2));
        buffer = logFileAccessor.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, (long) (maxFileSize * 1.2));

        recover(processor);   //重新启动时自动进行恢复操作

        recycle();


    }

    private CommitLog(File commitLogFile,int maxFileSize,String separator) throws IOException {
        this(commitLogFile,maxFileSize);
    }

    public CommitLog(File commitLogFile,int maxFileSize) throws IOException {
        this.commitLogFile = commitLogFile ;
        this.readBuffer = new byte[4096];
        boolean existsFile = commitLogFile.exists();
        RandomAccessFile logFileAccessor = new RandomAccessFile(commitLogFile, "rw");
        logFileAccessor.setLength((long) (maxFileSize * 1.2));
        buffer = logFileAccessor.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, (long) (maxFileSize * 1.2));
        if(!existsFile){
            recycle();
        }
    }

    public boolean delete(){
        return commitLogFile.delete();
    }

    @Deprecated
    public boolean append(String line){
        byte[] serializedRow = line.getBytes();
        return append(serializedRow);
    }

    public boolean append(byte[] serializedRow){
        entryNumbers ++ ;
        buffer.putInt(serializedRow.length);
        buffer.put(serializedRow);
        if (buffer.remaining() >= 4){
            buffer.putInt(END_OF_SEGMENT_MARKER);
            buffer.position(buffer.position() - END_OF_SEGMENT_MARKER_SIZE);
            return true;
        }else{
            return false;
        }
    }

    public void recycle(){
        buffer.position(0);
        buffer.putInt(CommitLog.END_OF_SEGMENT_MARKER);
        buffer.position(0);
        entryNumbers = 0 ;
    }

    public boolean hasCapacityFor(String mutation){
        long totalSize = mutation.getBytes().length + ENTRY_OVERHEAD_SIZE;
        return totalSize <= buffer.remaining();
    }

    public long remaining(){
        return buffer.remaining();
    }

    public boolean hasCapacityFor(int size){
        long totalSize = size + ENTRY_OVERHEAD_SIZE;
        return totalSize <= buffer.remaining();
    }



    public synchronized long recover(Processor processor){
        int num  = 0 ;
        int replayPosition = 0 ;
        buffer.position(replayPosition);
        int capacity = (buffer.capacity() - END_OF_SEGMENT_MARKER_SIZE);
        if(capacity <= 0) return num;
        while (replayPosition < capacity){
            int serializedSize = buffer.getInt();
            if (serializedSize == END_OF_SEGMENT_MARKER
                    || serializedSize + replayPosition > buffer.capacity()){
                  break;
            }
            if (serializedSize > readBuffer.length){
                readBuffer = new byte[(int) (1.2 * serializedSize)];
            }
            buffer.get(readBuffer,0,serializedSize);
            byte[] message = Arrays.copyOf(readBuffer,serializedSize);
            processor.process(message) ;
            num ++ ;
            replayPosition = buffer.position();
        }
        //recycle();         //恢复完毕,等待下一次写入~
        return num;
    }

    public long getEntryNumbers() {
        return entryNumbers;
    }

    public interface Processor{
        void process(byte[] message) ;
    }


}
