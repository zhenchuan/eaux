package me.zhenchuan.eaux.upload.impl;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import me.zhenchuan.eaux.upload.FileNameGenerator;
import me.zhenchuan.eaux.upload.FileRegistry;
import me.zhenchuan.eaux.upload.UploadService;
import me.zhenchuan.eaux.utils.Files;
import me.zhenchuan.eaux.utils.Metrics;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by liuzhenchuan@foxmail.com on 11/9/15.
 */
public class AsyncUploadService extends UploadService{

    private static final Logger log = LoggerFactory.getLogger(AsyncUploadService.class);

    //上传到hdfs用,暂不支持并行上传
    private ExecutorService uploadExecutor ;

    private Counter uploadFailedCount = Metrics.counter(this.getClass(),"upload.failed");

    public AsyncUploadService(String name,final FileRegistry fileRegistry,
                              FileNameGenerator fileNameGenerator,
                              Configuration configuration) {
        super(fileNameGenerator,configuration);
        this.fileRegistry = fileRegistry;
        this.uploadExecutor = Executors.newSingleThreadExecutor();

        //注册gauge
        Metrics.getRegistry().register(MetricRegistry.name(this.getClass(),name, "upload.pending"),
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return fileRegistry.size();
                    }
                }) ;
    }

    @Override
    public void start() {
        cleanUp();
        uploadExecutor.execute(new Runnable() {
            @Override
            public void run() {
                while (isRunning) {
                    if(fileRegistry.size()< 10 && RandomUtils.nextInt(100) < 5){
                        cleanUp() ;
                    }
                    String localFile = null, remoteFile = null;
                    boolean uploadSuccess = false;
                    try {
                        long s = System.currentTimeMillis();
                        localFile = fileRegistry.recv();
                        if(localFile == null) continue;
                        remoteFile = makeUploadPath(localFile);
                        if (upload && remoteFile!=null) {
                            log.debug("begin to upload localFile {}  to remoteFile {}", localFile, remoteFile);
                            uploadSuccess = hdfsUtils.upload(remoteFile, localFile);
                        }
                        log.info("[{}] upload localFile {}  to remoteFile {} \n" +
                                        "left:{} ,using:{} ms",
                                uploadSuccess, localFile, remoteFile,
                                fileRegistry.size(),(System.currentTimeMillis() - s));

                        if (uploadSuccess) {
                            log.debug("upload success,begin to delete {}", localFile);
                            hdfsUtils.delete(localFile, fileSystem);   //删除文件
                        }
                    } catch (Exception e) {
                        uploadFailedCount.inc();
                        log.error("failed to upload {}", localFile, e);
                    }
                }
                log.debug("upload done(+");
            }
        });
    }

    @Override
    public void stop()  {
        if(uploadExecutor!=null){
            try {
                this.isRunning = false;
                log.info("awaitTermination uploadExecutor in 30s");
                uploadExecutor.shutdownNow();
                uploadExecutor.awaitTermination(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void upload(String filePath) {
        if (filePath != null /* && messageWrittenInRotation */) {
            try {
                fileRegistry.send(filePath);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public int cleanUp() {
        int count = 0;
        if(!isRunning)
            return count;
        try {
            FileStatus[] files = fileSystem.listStatus(new Path(this.workDir));
            for (FileStatus file: files) {
                if (file.getLen() > 0 && file.isFile()) {
                    String fileName = file.getPath().getName();
                    String fileExt = Files.getFileExt(fileName);
                    if (fileExt != null && fileExt.equals(suffix)) {
                        String path = file.getPath().toString();
                        if(!fileRegistry.contains(path)){
                            fileRegistry.send(path);
                            count ++;
                        }
                    }else{
                        log.info("commit log {} has no valid extension[{}].",file.getPath(),suffix);
                    }
                }
            }
            log.info("after clean up [{}],found {} new files to upload.\t total:{}",this.workDir,count, fileRegistry.size());
        } catch (Exception e) {
            log.error("Exception while on cleanUp: " + e.getMessage(), e);
            return Integer.MIN_VALUE; // return non-zero value
        }

        return count;
    }
}
