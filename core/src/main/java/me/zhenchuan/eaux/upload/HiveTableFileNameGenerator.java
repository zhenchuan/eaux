package me.zhenchuan.eaux.upload;

import com.google.common.base.Preconditions;
import org.apache.commons.configuration.Configuration;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.server.UID;
import java.util.Map;

/**
 *
 * Created by liuzhenchuan@foxmail.com on 8/2/15.
 */
public class HiveTableFileNameGenerator implements FileNameGenerator {

    protected String table;
    protected String workDir;

    public HiveTableFileNameGenerator(Configuration configuration){
        String workDir = configuration.getString("work.dir");
        Preconditions.checkNotNull(workDir, "work dir is needed");
        String hiveTable = configuration.getString("hive.table");
        Preconditions.checkNotNull(workDir, "hive.table is needed");
        this.table = hiveTable;
        this.workDir = workDir;
    }

    public HiveTableFileNameGenerator(String table, String workDir){
        this.table = table;
        this.workDir = workDir;
    }

    @Override
    public String localPath(Map<String,Object> context) {
        return FileNameFormatter.get(workDir)+ UploadService.suffix;
    }

    @Override
    public String remotePath(String localFile) {
        final File file = new File(localFile);
        final DateTimeFormatter formatter = DateTimeFormat.forPattern("'/apps/hive/warehouse/" + table + "/pday='yyyyMMdd'/phour='H'/'");
        DateTime dateCreated = FileNameFormatter.getFileCreateTimeFromPath(new File(localFile)); //当前文件的创建时间
        String prefix = formatter.print(dateCreated);
        return prefix +  file.getName();
    }

    /**
     * Created by liuzhenchuan@foxmail.com on 4/29/15.
     */
    public static class FileNameFormatter {
        public static String localHostAddr;
        static {
            try {
                localHostAddr = InetAddress.getLocalHost().getHostName() ;
            } catch (UnknownHostException e) {
                localHostAddr = "UNKONWN_HOST";
            }
        }

        private static DateTimeFormatter fmt = DateTimeFormat.forPattern("'P'yyyyMMdd'T'HHmmss");

        private static DateTimeFormatter fmt_hour = DateTimeFormat.forPattern("'P'yyyyMMdd'T'HH0000");


        /**
         *
         * @param dir directory path where the files are written
         * @return full file path with the directory suffixed
         */
        public static String get(String dir) {
            return get(dir,new DateTime());
        }


        public static String get(String dir,DateTime dateTime){
            StringBuilder sb = new StringBuilder(dir);
            if (!dir.endsWith("/")) {
                sb.append('/');
            }
            sb.append(fmt.print(dateTime))
                    .append(localHostAddr)
                    .append(new UID().toString());

            return sb.toString().replaceAll("[-:]", "");
        }

        public static String getFixed(String dir,DateTime dateTime){
            StringBuilder sb = new StringBuilder(dir);
            if (!dir.endsWith("/")) {
                sb.append('/');
            }
            sb.append(fmt_hour.print(dateTime))
                    .append("lucas.local");  //FIXME 这里加个partition参数

            return sb.toString().replaceAll("[-:]", "");
        }

        public static DateTime getFileCreateTimeFromPath(File file){
            String fileName = file.getName();
            return fmt.parseDateTime(fileName.substring(0,"P20141006T213521".length()));
        }

    }
}
