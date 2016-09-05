package me.zhenchuan.eaux.writer;

import com.google.common.base.Preconditions;
import me.zhenchuan.eaux.DefaultRowSerDe;
import me.zhenchuan.eaux.Row;
import me.zhenchuan.eaux.sink.queue.SerDe;
import me.zhenchuan.eaux.upload.FileNameGenerator;
import me.zhenchuan.eaux.upload.FileRegistry;
import me.zhenchuan.eaux.utils.Granularity;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.CompressionKind;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 用于直接生产orc格式的文件,减少转换延时.
 * 其内部实现`WriterImpl`本身就是同步的~
 * Created by liuzhenchuan@foxmail.com on 3/16/15.
 */
public class FileWriter extends TimeRollingOrcSupport implements RecoverableWriter {

    private static final Logger log = LoggerFactory.getLogger(FileWriter.class);

    private static final long stripeSize = 256L * 1024 * 1024;
    private static final CompressionKind compress = CompressionKind.ZLIB;
    private static final int bufferSize = 256 * 1024;
    private static final int rowIndexStride = 10000;

    private FileSystem fs ;

    private volatile String curFileName;

    private CommitLog commitLog ;

    private FileRegistry fileRegistry;

    private FileNameGenerator nameGenerator;

    private AtomicLong written = new AtomicLong(0);

    private SerDe<Row> rowSerDe = DefaultRowSerDe.getInstance();


    public FileWriter(org.apache.commons.configuration.Configuration configuration,
                      FileRegistry fileRegistry, FileNameGenerator nameGenerator,
                      CommitLog commitLog) {
        String[] fieldNames = configuration.getStringArray("field.name.list");
        String[] fieldTypes = configuration.getStringArray("field.type.list");

        String gran = configuration.getString("rotation.gran","hour");
        String rotationPeriod = configuration.getString("rotation.period","PT5m") ;  //默认5分钟
        Granularity granularity = Granularity.HOUR;
        if(gran!=null){
            granularity = Granularity.valueOf(gran.toUpperCase());
        }
        Period period = new Period(rotationPeriod);

        init(fileRegistry, nameGenerator, commitLog, fieldNames, fieldTypes, granularity, period);
    }

    private void init(FileRegistry fileRegistry,
                      FileNameGenerator nameGenerator,
                      CommitLog commitLog, String[] fieldNames, String[] fieldTypes,
                      Granularity granularity, Period period) {
        this.fieldTypes = fieldTypes;
        this.fieldNames = fieldNames ;

        Preconditions.checkState(fieldNames.length == fieldTypes.length,
                "fieldNames.length:" + fieldNames.length +  " != fieldTypes.length:" + fieldTypes.length);

        this.fileRegistry = fileRegistry ;
        this.nameGenerator = nameGenerator ;
        this.granularity = granularity ;
        this.rotationPeriod = period ;
        this.commitLog = commitLog ;

        try {
            this.fs =  FileSystem.getLocal(new Configuration());
            resetWriter(nameGenerator.localPath(null));
        } catch (IOException e) {
            throw new RuntimeException("faild to create orc me.zhuenchuan.eaux.writer~",e);
        }
        calcNextRotation();
    }

    private FileWriter(String[] fieldNames, String[] fieldTypes,
                      FileRegistry fileRegistry, FileNameGenerator nameGenerator,
                      Granularity granularity, Period rotationPeriod)  {
        this(fieldNames,fieldTypes,fileRegistry,nameGenerator,granularity,rotationPeriod,null);
    }

    private FileWriter(String[] fieldNames, String[] fieldTypes,
                      FileRegistry fileRegistry, FileNameGenerator nameGenerator,
                      Granularity granularity, Period rotationPeriod,
                      CommitLog commitLog) {
        init(fileRegistry, nameGenerator, commitLog, fieldNames, fieldTypes, granularity, rotationPeriod);
    }

    private void resetWriter(String filename) throws IOException {
        Path path = new Path(filename);
        ObjectInspector inspector = createInspector(fieldNames,fieldTypes);
        this.writer = OrcFile.createWriter(fs, path, new Configuration(), inspector,
                stripeSize, compress, bufferSize, rowIndexStride);
        this.curFileName = filename;
        written.set(0);
    }

    private boolean inRecoverState = false;

    @Override
    public long recoverWith(CommitLog.Processor processor,Boolean persist){
        long num = 0 ;
         inRecoverState = true;
         if(commitLog!=null){
             num =  commitLog.recover(processor);
         }else{
             log.warn("commitLog is null , can't be recovered; Omits ~ ");
         }
        inRecoverState = false;

        if(num > 0 && persist){  //
            rotateTo(nameGenerator.localPath(null));

        }
        if(persist && commitLog != null){
            //使用persist时,不管是否成功,都应该调用recycle
            commitLog.recycle();
        }
        return num;
    }


    private boolean add0(Object[] cols){
        try {
            if(writer == null){
                return false;
            }
            writer.addRow(cols);
            written.incrementAndGet();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public synchronized boolean add(Row message) {
        if (!inRecoverState) {
            if ((commitLog != null && !commitLog.hasCapacityFor(message.estimateSize()))) {
                String oldName = curFileName;
                boolean ret = rotateTo(nameGenerator.localPath(null), true, message.estimateSize());
                if (ret) {
                    fileRegistry.send(oldName);
                }
            }
            if (timeFired()) {
                String oldName = curFileName;
                boolean ret = rotateTo(nameGenerator.localPath(null), false, message.estimateSize());
                if (ret) {
                    fileRegistry.send(oldName);
                }
            }

            if (commitLog != null) {
                commitLog.append(rowSerDe.serialize(message));
            }
        }

        if (message.fields().length == fieldNames.length) {
            if (message.fields() instanceof String[]) {
                add0(convert((String[]) message.fields()));
            } else {
                add0(message.fields());
            }
        } else {
            log.warn("error: cols.length:" + message.fields().length + " ; names.length:" + fieldNames.length + " ;types.length:" + fieldTypes.length
                    + "\n" + Arrays.asList(message.fields()));
            return false;
        }

        return true;
    }

    private synchronized  boolean rotateTo(String newFileName, boolean force, int dataSize)  {
        if(force){ // 达到大小限制~
            if(commitLog.hasCapacityFor(dataSize)){
                log.info("[{}] for capacity. canceled",Thread.currentThread().getName());
                return false;
            }
        }else{
            if(!timeFired()){  //可能上一个线程已经rotate过了(时间过期)
                log.info("[{}] for time fired. canceled",Thread.currentThread().getName());
                return false;
            }
        }
        return rotateTo(newFileName);
    }


    public synchronized boolean rotateTo(String newFileName)  {
        try {
            close();
            resetWriter(newFileName);
        } catch (IOException e) {
            throw new RuntimeException("failed to rotate file ~",e);
        }
        written.set(0);
        calcNextRotation();
        return true;
    }

    public long writtenLines(){
        return written.get();
    }


    @Override
    public synchronized  void close(){
        try {
            String oldName = curFileName;
            this.writer.close();    //close过程中应该阻止写入->从实现过程看,是阻塞的~
            if(commitLog!=null){
                commitLog.recycle();
            }
            log.info("rotateTo [{}]. written {} ", curFileName , written,
                    new Date());
            this.writer = null;
            fileRegistry.send(oldName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static class Builder {
        private String[] fieldNames;
        private String[] fieldTypes;
        private FileRegistry fileRegistry = new FileRegistry();
        private FileNameGenerator nameGenerator;
        private Granularity granularity = Granularity.valueOf("HOUR");
        private Period rotationPeriod = new Period("PT5m");
        private CommitLog commitLog = null;

        public Builder setFieldNames(String[] fieldNames) {
            this.fieldNames = fieldNames;
            return this;
        }

        public Builder setFieldTypes(String[] fieldTypes) {
            this.fieldTypes = fieldTypes;
            return this;
        }

        public Builder setFileRegistry(FileRegistry fileRegistry) {
            this.fileRegistry = fileRegistry;
            return this;
        }

        public Builder setNameGenerator(FileNameGenerator nameGenerator) {
            this.nameGenerator = nameGenerator;
            return this;
        }

        public Builder setGranularity(Granularity granularity) {
            this.granularity = granularity;
            return this;
        }

        public Builder setRotationPeriod(Period rotationPeriod) {
            this.rotationPeriod = rotationPeriod;
            return this;
        }

        public Builder setCommitLog(CommitLog commitLog) {
            this.commitLog = commitLog;
            return this;
        }

        public FileWriter createSingleWriter() {
            Preconditions.checkState(fieldNames.length == fieldTypes.length,"fieldNames同fieldTypes的长度必须相等~" + fieldNames.length + "!=" + fieldTypes.length);
            return new FileWriter(fieldNames, fieldTypes, fileRegistry, nameGenerator, granularity, rotationPeriod, commitLog);
        }
    }

}
