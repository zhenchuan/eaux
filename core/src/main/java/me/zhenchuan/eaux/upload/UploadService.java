package me.zhenchuan.eaux.upload;

import com.google.common.base.Preconditions;
import me.zhenchuan.eaux.utils.HdfsUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * Created by liuzhenchuan@foxmail.com on 11/9/15.
 */
public abstract class UploadService {

    private static final Logger log = LoggerFactory.getLogger(UploadService.class);

    protected volatile boolean isRunning = true ;

    public static final String suffix = ".lzc";
    public static final String done = ".done";

    protected boolean deleteStaging = false ;

    protected String workDir;
    protected final boolean upload ;   //调试时可用

    protected FileSystem fileSystem;

    protected FileRegistry fileRegistry;

    protected FileNameGenerator fileNameGenerator;

    protected HdfsUtils hdfsUtils ;

    public UploadService(FileNameGenerator fileNameGenerator,
                         org.apache.commons.configuration.Configuration configuration){
        String workDir = configuration.getString("work.dir");
        Preconditions.checkNotNull(workDir, "work dir is needed");
        this.upload = configuration.getBoolean("hdfs.upload",false);
        init(fileNameGenerator,workDir);
        String[] res = configuration.getStringArray("hdfs.resources") ;
        if(res == null){
            log.warn("no hdfs.resources specified,will use file [core-site.xml,hdfs-site.xml] in classpath");
        }else{
            hdfsUtils = new HdfsUtils(res);
        }
    }

    private void init(FileNameGenerator fileNameGenerator,
                         String workDir){
        try {
            this.fileSystem = FileSystem.getLocal(new Configuration());
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.fileNameGenerator = fileNameGenerator;

        if (!workDir.endsWith("/")) {
            workDir += "/";
        }
        this.workDir = workDir ;
        if(!new File(this.workDir).exists()){
            boolean create = new File(this.workDir).mkdirs();
            Preconditions.checkState(create,"failed to create [" + this.workDir + "]");
        }
    }

    public abstract void start();

    public abstract void stop();

    /****
     * localFile 需要包含足够多的信息,用于构建上传路径时决策~
     * @param localFile
     * @return
     */
    protected String makeUploadPath(String localFile) {
        try{
            return fileNameGenerator.remotePath(localFile);
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }
    }

    public abstract void upload(String file);

    /****
     * 处理由于各种原因没有按时上传的文件
     * @return
     */
    public abstract int cleanUp();

    @Deprecated
    protected Path setDone(String oldName) throws IOException {
        String newName = oldName.replace(suffix, done);
        Path oldPath = new Path(oldName);
        if(fileSystem.exists(oldPath)){  //这里可能是文件系统的bug
            Path dst = new Path(newName);
            fileSystem.rename(oldPath, dst);
            return dst;
        }else{
            log.error("failed to found {}",oldName);
            return null;
        }
    }

    public void setDeleteStaging(boolean deleteStaging) {
        this.deleteStaging = deleteStaging;
    }
}
