package me.zhenchuan.eaux.sink;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import me.zhenchuan.eaux.DefaultRowSerDe;
import me.zhenchuan.eaux.Row;
import me.zhenchuan.eaux.sink.queue.FileBlockingQueue;
import me.zhenchuan.eaux.upload.UploadService;
import me.zhenchuan.eaux.utils.Metrics;
import me.zhenchuan.eaux.writer.RecoverableWriter;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.*;

/**
 * Created by liuzhenchuan@foxmail.com on 7/19/15.
 */
public abstract class HdfsSink {

    private static final Logger log = LoggerFactory.getLogger(HdfsSink.class);

    private String name;

    //异步消费队列
    private  final BlockingQueue<Row> buffer ;
    private ThreadPoolExecutor writerExecutor ;


    //orc writer属性
    private RecoverableWriter writer;

    private UploadService uploadService ;

    private final String workDir;

    private volatile boolean isRunning = true ;

    private Meter writeSuccessCount = Metrics.meter(this.getClass(),"writer.success");
    private Meter writeFailedCount = Metrics.meter(this.getClass(),"writer.failed");


    public HdfsSink(Configuration configuration,
                    RecoverableWriter writer,
                    UploadService uploadService) throws IOException {
        String[] fieldNames = configuration.getStringArray("field.name.list");
        String[] fieldTypes = configuration.getStringArray("field.type.list");

        Preconditions.checkState(fieldNames.length == fieldTypes.length,
                "fieldNames.length:" + fieldNames.length +  " != fieldTypes.length:" + fieldTypes.length);

        this.name = configuration.getString("name");

        String outputDir = configuration.getString("work.dir");
        Preconditions.checkNotNull(outputDir, "work dir is needed");

        this.workDir = outputDir ;
        int poolSize =  configuration.getInt("writer.parallel",1);
        writerExecutor = newFixedThreadPoolWithQueueSize(poolSize,
                10000,
                new ThreadFactoryBuilder().setNameFormat("orc_writer_" + poolSize + "_%d").build());
        //Executors.newFixedThreadPool(poolSize,new ThreadFactoryBuilder().setNameFormat("orc_writer_" + poolSize + "_%d").build());

        //这个地方的queue设置的大一点,有时可能几天的数据堆积(sign!)
        this.buffer = new FileBlockingQueue<Row>(this.workDir,"file-queue",
                5 * 60 /*5min*/, DefaultRowSerDe.getInstance());

        this.writer = writer ;

        this.uploadService =uploadService;

        //注册gauge
        Metrics.getRegistry().register(MetricRegistry.name(this.getClass(), "pending.message"),
                new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return buffer.size();
            }
        }) ;

    }

    private  ThreadPoolExecutor newFixedThreadPoolWithQueueSize(int nThreads, int queueSize,ThreadFactory threadFactory) {
        return new ThreadPoolExecutor(nThreads, nThreads,
                1, TimeUnit.HOURS,
                new ArrayBlockingQueue<Runnable>(queueSize, true),
                threadFactory,
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    private WriteOrcFileService daemonOrcServcie;

    public void start() throws IOException {
        log.info("start upload service");
        uploadService.start();
        log.info("start orc me.zhuenchuan.eaux.writer service");
        daemonOrcServcie = new WriteOrcFileService();
        daemonOrcServcie.start();
    }

    public void writeMessage(Object[] fields) {
        try {
            Row row = decorate(fields);
            buffer.put(row);
        } catch (Exception e) {
            writeFailedCount.mark();
            log.error("failed to put this line[{}] to buffer."
                    ,fields,e);
        }
    }

    private class WriteOrcFileService extends Thread{
        @Override
        public void run() {
            generateORCFileService();
        }

        public void stop0(){
            try{
                HdfsSink.this.isRunning = false;
                HdfsSink.this.writerExecutor.shutdown();
                log.info("awaitTermination {} in 30s","writerExecutor");
                while (!HdfsSink.this.writerExecutor.isShutdown()){
                    log.info("waiting writerExecutor to shutdown ..");
                    Thread.sleep(50);
                }
                HdfsSink.this.writerExecutor.awaitTermination(30, TimeUnit.SECONDS);
                //log.info("me.zhuenchuan.eaux.writer close (+");
                //writer.close();
                //log.info("me.zhuenchuan.eaux.writer done  +)");
            }catch (Exception e){
                e.printStackTrace();
            }

        }
    }

    private void generateORCFileService(){
        while (isRunning) {
            writerExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        Row message = buffer.take();
                        writer.add(message);
                        writeSuccessCount.mark();
                    } catch (Exception e) {
                        writeFailedCount.mark();
                        e.printStackTrace();
                    }
                }
            });
        }
    }

    public String getName() {
        return name;
    }

    protected abstract Row decorate(Object[] message);

    public void stop() {
        try {
            log.info("daemonOrcServcie stop");
            daemonOrcServcie.stop0();
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            this.uploadService.stop();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}


