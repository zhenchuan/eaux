package me.zhenchuan.eaux.upload.impl;

import me.zhenchuan.eaux.upload.FileNameGenerator;
import me.zhenchuan.eaux.upload.UploadService;
import me.zhenchuan.eaux.utils.Files;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by liuzhenchuan@foxmail.com on 11/9/15.
 */
public class SyncUploadService extends UploadService {

    private static final Logger log = LoggerFactory.getLogger(SyncUploadService.class);

    public SyncUploadService(FileNameGenerator fileNameGenerator,
                             Configuration configuration) {
        super(fileNameGenerator,configuration);
    }

    @Override
    public void start() {
        cleanUp();
    }

    @Override
    public void stop() {

    }

    @Override
    public void upload(String localFile) {
        if (localFile != null /* && messageWrittenInRotation */) {
            try {
                String remoteFile = makeUploadPath(localFile);
                boolean success = false;
                int retry = 1 ;
                while(!success && retry < 3){
                    log.info("begin to upload localFile {}  to remoteFile {}", localFile, remoteFile);
                    success = !upload || hdfsUtils.upload(remoteFile, localFile);
                    if(!success){
                        Thread.sleep(30 * 1000);
                    }
                    retry ++ ;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public int cleanUp() {
        int count = 0;
        try {
            FileStatus[] files = fileSystem.listStatus(new Path(this.workDir));
            for (FileStatus file: files) {
                if (file.getLen() > 0 && file.isFile()) {
                    String fileName = file.getPath().getName();
                    String fileExt = Files.getFileExt(fileName);
                    if (fileExt != null && fileExt.equals(suffix)) {
                        String path = file.getPath().toString();
                        upload(path);
                        count ++;
                    }else{
                        log.info(" {} has no valid extension[{}]. ommits!",file.getPath(),suffix);
                    }
                }
            }
            log.info("after clean up [{}],found {} new files to upload.",this.workDir,count);
        } catch (Exception e) {
            log.error("Exception while on cleanUp: " + e.getMessage(), e);
            return Integer.MIN_VALUE; // return non-zero value
        }

        return count;
    }
}
