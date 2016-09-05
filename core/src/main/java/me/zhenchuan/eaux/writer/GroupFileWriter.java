package me.zhenchuan.eaux.writer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import jsr166e.ConcurrentHashMapV8;
import me.zhenchuan.eaux.DefaultRowSerDe;
import me.zhenchuan.eaux.Row;
import me.zhenchuan.eaux.sink.queue.SerDe;
import me.zhenchuan.eaux.upload.FileNameGenerator;
import me.zhenchuan.eaux.upload.FileRegistry;
import me.zhenchuan.eaux.utils.Granularity;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 用于处理多个时间并行的问题~
 * 必须线程安全~
 * Created by liuzhenchuan@foxmail.com on 7/25/16.
 */
public class GroupFileWriter implements Writer,RecoverableWriter{

    private static final Logger log = LoggerFactory.getLogger(GroupFileWriter.class);

    //过期清除writer占用资源~
    private ConcurrentHashMapV8<String,Segment> groupWriter = new ConcurrentHashMapV8<String,Segment>(10);

    private String fileDirectory;
    private String[] fieldNames ;
    private String[] fieldTypes ;

    private Granularity granularity = Granularity.valueOf("HOUR");
    private Period rotationPeriod = new Period("PT2m");

    private boolean useCommitLog ;
    private String commitLogDir ;
    private int commitLogMaxSize ;
    private int expiryRotationPeriodInTimes = 5;

    private FileNameGenerator fileNameGenerator;

    private ScheduledExecutorService sec ;

    private FileRegistry fileRegistry;

    private SerDe<Row> rowSerDe = DefaultRowSerDe.getInstance();

    public GroupFileWriter(org.apache.commons.configuration.Configuration configuration,
                           FileRegistry fileRegistry,
                           FileNameGenerator fileNameGenerator){
        String[] fieldNames = configuration.getStringArray("field.name.list");
        String[] fieldTypes = configuration.getStringArray("field.type.list");

        String gran = configuration.getString("rotation.gran","hour");
        String rotationPeriod = configuration.getString("rotation.period","PT5m") ;  //默认5分钟
        Granularity granularity = Granularity.HOUR;
        if(gran!=null){
            granularity = Granularity.valueOf(gran.toUpperCase());
        }
        Period period = new Period(rotationPeriod);

        int expirySegmentCheckInSeconds = configuration.getInt("segment.expiry.check.interval.seconds",90);
        int expiryRotationPeriodInTimes = configuration.getInt("segment.force.persist.interval.rp",5) ;
        boolean useCommitLog = configuration.getBoolean("commitlog.enable",false) ;//false;
        int commitLogMaxSize = configuration.getInt("rotation.file.size.mb",128) * 1024 * 1024; //默认128M

        String workDir = configuration.getString("work.dir");
        Preconditions.checkNotNull(workDir, "work dir is needed");

        init(workDir, fieldNames, fieldTypes, granularity,
                period, expirySegmentCheckInSeconds, expiryRotationPeriodInTimes, useCommitLog,
                commitLogMaxSize, fileNameGenerator, fileRegistry);


    }

    private GroupFileWriter(String fileDirectory , String[] fieldNames,
                           String[] fieldTypes,
                           Granularity granularity, Period rotationPeriod,
                           int expirySegmentCheckInSeconds, int expiryRotationPeriodInTimes,
                           boolean useCommitLog, int commitLogMaxSize,
                           FileNameGenerator fileNameGenerator,
                           FileRegistry fileRegistry) {

        init(fileDirectory, fieldNames, fieldTypes, granularity, rotationPeriod,
                expirySegmentCheckInSeconds, expiryRotationPeriodInTimes,
                useCommitLog, commitLogMaxSize,
                fileNameGenerator, fileRegistry);
    }

    private void init(String fileDirectory, String[] fieldNames, String[] fieldTypes,
                      Granularity granularity, Period rotationPeriod,
                      int expirySegmentCheckInSeconds, int expiryRotationPeriodInTimes,
                      boolean useCommitLog, int commitLogMaxSize,
                      FileNameGenerator fileNameGenerator, FileRegistry fileRegistry) {
        this.fileDirectory = fileDirectory ;
        this.fieldNames = fieldNames ;
        this.fieldTypes = fieldTypes ;
        this.useCommitLog = useCommitLog;

        this.expiryRotationPeriodInTimes = expiryRotationPeriodInTimes;

        if(useCommitLog){
            File commitLog = new File(fileDirectory, "commit_log");
            if(!commitLog.exists()){
                commitLog.mkdirs();
            }
            this.commitLogDir = commitLog.getAbsolutePath();
            this.commitLogMaxSize = commitLogMaxSize ;
        }

        this.granularity = granularity ;
        this.rotationPeriod = rotationPeriod ;

        this.fileRegistry = fileRegistry;
        this.fileNameGenerator = fileNameGenerator;

        sec = Executors.newSingleThreadScheduledExecutor();
        sec.scheduleAtFixedRate(new SegmentCleaner(),60,expirySegmentCheckInSeconds, TimeUnit.SECONDS);
    }

    public boolean add(Row message){
        String group = message.group();
        initSegment(group);
        if(message.fields().length == fieldNames.length
                && message.fields().length == fieldTypes.length){
            //FIXME the segment may be has been dropped ~
            return groupWriter.get(group).add(message);
        }else{
            log.warn("error: cols.length:"+message.fields().length+" ; names.length:"+fieldNames.length+" ;types.length:"+fieldTypes.length
                    +"\n" + Arrays.asList(message.fields()));
            return false;
        }

    }

    private void initSegment(String group) {
        Segment segment = groupWriter.get(group);
        if(segment == null){
            log.info("[{}] create segment for group {}",Thread.currentThread().getName(),group);
            groupWriter.computeIfAbsent(group, new ConcurrentHashMapV8.Fun<String, Segment>() {
                @Override
                public Segment apply(String group) {
                    try {
                        return new Segment(group);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    return null;
                }
            });
        }
    }

    @Override
    public boolean rotateTo(String newFileName) throws IOException {
         return true;
    }

    public void close() {
        try{
            sec.shutdown();
        }catch (Exception e){
            e.printStackTrace();
        }
        //
        for (Map.Entry<String, Segment> entry : groupWriter.entrySet()) {
            log.info("close segment {}. written {}", entry.getKey(),entry.getValue().written);
            entry.getValue().close();
        }
    }


    @Override
    public long recoverWith(CommitLog.Processor processor, Boolean persist) {
        File file = new File(commitLogDir);
        File[] commitLogs = file.listFiles();
        if(commitLogs == null) return 0 ;
        int size = commitLogs.length;
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("recover_thread_%d").build();
        ExecutorService recoverThreadPool = Executors.newSingleThreadExecutor(threadFactory);
        if(size > 1 ){
            recoverThreadPool = Executors.newFixedThreadPool(
                    Math.min(size,Runtime.getRuntime().availableProcessors()/2),
                    threadFactory);
        }
        for(File commitLog : commitLogs){
            final String group = commitLog.getName();
            recoverThreadPool.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        log.info("[{}] create segment for group {}",Thread.currentThread().getName(),group);
                        groupWriter.put(group,new Segment(group));
                    } catch (IOException e) {
                        throw new RuntimeException("faild to init segemnt me.zhuenchuan.eaux.writer for group " + group,e);
                    }
                }
            });

        }
        try {
            recoverThreadPool.shutdown();
            recoverThreadPool.awaitTermination(5,TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException("failed to recover in 5min~");
        }
        return 0;
    }

    /****
     * 长时间没有数据流进的segment,也必须要对writer进行close处理~
     */
    private class SegmentCleaner implements Runnable{

        @Override
        public void run() {
            for (Map.Entry<String, Segment> stringSegmentEntry : groupWriter.entrySet()) {
                try {
                    Segment segment = stringSegmentEntry.getValue();
                    String group = stringSegmentEntry.getKey();
                    int cleanUpPeriod = rotationPeriod.toStandardSeconds().getSeconds() * expiryRotationPeriodInTimes;
                    if (System.currentTimeMillis() - segment.nextRotation > cleanUpPeriod) {
                        log.info("closed {} after [{}] {} seconds. written {}", group,
                                new Date(segment.nextRotation), cleanUpPeriod,segment.written);
                        segment.close();
                        groupWriter.remove(group);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /****
     * 用于在启动时恢复数据~
     */
    class RecoverLogProcessor implements CommitLog.Processor{

        long count = 0 ;
        OrcWriter orcWriter;

        public RecoverLogProcessor(OrcWriter orcWriter){
            this.orcWriter = orcWriter;
        }

        @Override
        public void process(byte[] line) {
            Row row = rowSerDe.deserialize(line);
            orcWriter.add(row.fields());
            count ++;
        }

    }



    /****
     * 每个segment独占一个lock~ 线程安全~
     * 正常情况下的关闭, commit _log 处于rotate状态 ,可以用于后续等待的日志(多线程)错误时写入~
     * 异常关闭(kill -9) 则处于lock状态的日志都丢失~
     */
    private class Segment extends ReentrantLock {

        private String group ;
        private OrcWriter localOrcWriter;
        private CommitLog commitLog ;
        private volatile Date firstCreateTime ;
        private volatile Date lastRotateTime ;
        private long nextRotation ;
        private AtomicLong written = new AtomicLong();

        private Map<String,Object> context;
        private volatile String curFileName;

        public  Segment(String group) throws IOException {
            this.group = group ;
            this.firstCreateTime = new Date();

            Map<String,Object> context = Maps.newHashMap() ;
            context.put("group",group);
            context.put("firstCreateTime",this.firstCreateTime);
            this.context = context;

            curFileName = fileNameGenerator.localPath(this.context);
            localOrcWriter = new OrcWriter(curFileName,fieldNames,fieldTypes) ;
            if(useCommitLog){
                RecoverLogProcessor recover = new RecoverLogProcessor(localOrcWriter);
                this.commitLog = new CommitLog(new File(commitLogDir, group), commitLogMaxSize);
                long num = commitLog.recover(recover);
                if(num > 0){
                    log.info("init ~ recover {} records from commit log for [{}]",num,group);
                    rotate(true); //持久化到orc 文件中
                }
                this.commitLog.recycle();
            }

            calcNextRotation() ;
            log.info("[{}] create segment for group {} success",Thread.currentThread().getName(),group);
        }

        public boolean add(Row message) {
            //写入之前先做判断~
            try{
                lock();
                checkRotate();
                if(localOrcWriter != null){  //正常写入
                    if(useCommitLog){
                        if(! commitLog.hasCapacityFor(message.estimateSize())){
                            log.info("commit log [{}] don't have enough space~ begin to rotateTo ~",group);
                            rotate(true);
                        }
                        commitLog.append(rowSerDe.serialize(message));
                    }
                    written.incrementAndGet();
                    localOrcWriter.add(message.fields());
                }else{  //可能已经处于close状态~
                    log.debug("failed to write to [{}],localOrcWriter has been closed ! [{}]",
                            group,Thread.currentThread().getName());
                    return false;
                }
            } catch (IOException e) {
                e.printStackTrace();
                return false;
            } finally {
                unlock();
            }
            return true;
        }


        public void close() {
            try {
                lock();
                if(localOrcWriter!=null) {
                    String oldName = curFileName;
                    localOrcWriter.close();
                    fileRegistry.send(oldName);
                    if(useCommitLog){
                        commitLog.delete();   //正常关闭时commit log也必须清空
                    }
                    written.set(0);
                    localOrcWriter = null;
                }
            }finally {
                unlock();
            }
        }

        void rotate(boolean force) throws IOException {
            try{
                lock();
                if(this.lastRotateTime == null  //还没有rotate
                        || force //一般用于到达设置的大小限制
                        || rotatable() ){  //double check here !
                    this.context.put("lastRotateTime",lastRotateTime);
                    String oldName = curFileName;
                    this.curFileName = fileNameGenerator.localPath(this.context);
                    localOrcWriter.rotateTo(curFileName);
                    log.info("group[{}] rotateTo [{}]. written [{}]", group , curFileName, written.get());
                    fileRegistry.send(oldName);
                    written.set(0);
                    if(useCommitLog){
                        commitLog.recycle();
                    }
                    this.lastRotateTime = new Date();
                    calcNextRotation();
                }
            }finally {
                unlock();
            }
        }

        private void checkRotate() throws IOException {
            if (rotatable()) {  //初始条件下nextRotation为0,所有就会先调用rotate方法.
                if(localOrcWriter==null)    //可能此时已经超时关闭~
                    return;
                rotate(false);
            }
        }

        private boolean rotatable() {
            return System.currentTimeMillis() > nextRotation;
        }

        private void calcNextRotation(){
            DateTime currentDateTime = new DateTime();
            long nextRotation = currentDateTime.plus(rotationPeriod).getMillis();
            if(granularity!=null){ //下一次rotation的时间不能超过当前设置的时间间隔.
                long expectedBreak = granularity.next(currentDateTime).getMillis();
                if(nextRotation >expectedBreak){
                    nextRotation = expectedBreak;
                }
            }
            this.nextRotation = nextRotation;
        }

    }

    public static class Builder {
        private String fileDirectory;
        private String[] fieldNames;
        private String[] fieldTypes;
        private Granularity granularity = Granularity.valueOf("HOUR");
        private Period rotationPeriod = new Period("PT5m");
        private int expirySegmentCheckInSeconds = 90;
        private int expiryRotationPeriodInTimes = 2 ;
        private boolean useCommitLog = false;
        private int commitLogMaxSize = 128 * 1024 * 1024; //默认128M
        private FileRegistry fileRegistry;
        private FileNameGenerator fileNameGenerator;

        public Builder setFileDirectory(String fileDirectory) {
            this.fileDirectory = fileDirectory;
            return this;
        }

        public Builder setFieldNames(String[] fieldNames) {
            this.fieldNames = fieldNames;
            return this;
        }

        public Builder setFieldTypes(String[] fieldTypes) {
            this.fieldTypes = fieldTypes;
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

        public Builder setExpirySegmentCheckInSeconds(int expirySegmentCheckInSeconds) {
            this.expirySegmentCheckInSeconds = expirySegmentCheckInSeconds;
            return this;
        }

        /****
         * 设置关闭Segment的时间, N * rotationPeriod
         * @param expiryRotationPeriodInTimes
         * @return
         */
        public Builder setExpiryRotationPeriodInTimes(int expiryRotationPeriodInTimes) {
            this.expiryRotationPeriodInTimes = expiryRotationPeriodInTimes;
            return this;
        }

        public Builder setUseCommitLog(boolean useCommitLog) {
            this.useCommitLog = useCommitLog;
            return this;
        }

        public Builder setCommitLogMaxSize(int commitLogMaxSize) {
            this.commitLogMaxSize = commitLogMaxSize;
            return this;
        }

        public Builder setFileRegistry(FileRegistry fileRegistry) {
            this.fileRegistry = fileRegistry;
            return this;
        }

        public Builder setFileNameGenerator(FileNameGenerator fileNameGenerator) {
            this.fileNameGenerator = fileNameGenerator;
            return this;
        }

        public GroupFileWriter createMultiWriter() {
            Preconditions.checkState(fieldNames.length == fieldTypes.length,"fieldNames同fieldTypes的长度必须相等~" + fieldNames.length + "!=" + fieldTypes.length);
            if(!new File(fileDirectory).exists()) {
                boolean result = new File(fileDirectory).mkdirs();
                Preconditions.checkState(result,"创建目录 : " + fileDirectory + " 失败,请检查是否具有该目录的权限~");
            }
            if(useCommitLog){
                File commitLog = new File(fileDirectory, "commit_log");
                if(!commitLog.exists()){
                    boolean result = commitLog.mkdirs();
                    Preconditions.checkState(result,"创建目录 : " + commitLog.getAbsolutePath() + " 失败,请检查是否具有该目录的权限~");
                }
            }else{
                log.warn("你没有选择使用CommitLog~不过没啥事~");
            }

            return new GroupFileWriter(fileDirectory, fieldNames, fieldTypes, granularity, rotationPeriod,
                    expirySegmentCheckInSeconds, expiryRotationPeriodInTimes,
                    useCommitLog, commitLogMaxSize,
                    fileNameGenerator,fileRegistry);
        }
    }
}
