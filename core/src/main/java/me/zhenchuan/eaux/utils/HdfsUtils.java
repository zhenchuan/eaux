package me.zhenchuan.eaux.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by liuzhenchuan@foxmail.com on 8/10/15.
 */
public class HdfsUtils {

    private static final Logger logger = LoggerFactory.getLogger(HdfsUtils.class);

    private Configuration conf;

    public boolean upload(String remoteFilePath,String localFilePath){
        boolean success = false ;
        try {
            Path outFile = new Path(remoteFilePath);
            FileSystem fs = getFileSystem();
            fs.mkdirs(outFile.getParent());
            fs.moveFromLocalFile(new Path(localFilePath), outFile);
            success = true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return success;
    }

    public boolean exist(String path){
        boolean success = false;
        try {
            FileSystem fileSystem = getFileSystem();
            return fileSystem.exists(new Path(path));
        }catch (Exception e){

        }
        return success;
    }


    private static final int deleteFileRetryCount = 5;

    public void delete(String filePath,FileSystem fileSystem) {
        int retryCount = 1;
        while (retryCount <= deleteFileRetryCount) {
            try {
                if (fileSystem.exists(new Path(filePath))) {
                    fileSystem.delete(new Path(filePath), false);
                }
                break;

            } catch (Exception e) {
                logger.warn("Exception while deleting the file: " + e.getMessage(), e);
            }
            ++retryCount;
        }
    }

    public boolean delete(String remote){
        boolean success = false;
        try {
            FileSystem fileSystem = getFileSystem();
            fileSystem.delete(new Path(remote),true);
            success = true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return success;
    }

    static {
        Configuration.addDefaultResource("hdfs-default.xml");
        Configuration.addDefaultResource("hdfs-site.xml");
    }

    public HdfsUtils(String... configResources){
        conf = new Configuration();
        conf.setQuietMode(true);
        for (String res:configResources) {
            conf.addResource(new Path(res));
        }
    }

    private FileSystem getFileSystem() {
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return fs;
    }



}
