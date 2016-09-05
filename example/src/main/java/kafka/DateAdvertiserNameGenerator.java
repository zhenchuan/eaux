package kafka;

import com.google.common.base.Preconditions;
import me.zhenchuan.eaux.upload.FileNameGenerator;
import me.zhenchuan.eaux.upload.UploadService;
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
 * eg , 根据日期和广告主分区~
 * 2015060704_100 <- date_advertiser
 * Created by liuzhenchuan@foxmail.com on 8/3/16.
 */
public class DateAdvertiserNameGenerator implements FileNameGenerator {

    private String workDir;
    private String table ;


    public DateAdvertiserNameGenerator(Configuration configuration){
        String workDir = configuration.getString("work.dir");
        Preconditions.checkNotNull(workDir, "work dir is needed");
        String hiveTable = configuration.getString("hive.table");
        Preconditions.checkNotNull(workDir, "hive.table is needed");
        this.table = hiveTable;
        this.workDir = workDir;
    }

    public DateAdvertiserNameGenerator(String workDir, String table) {
        this.workDir = workDir;
        this.table = table;
    }

    @Override
    public String localPath(Map<String, Object> context) {
        String group = (String) context.get("group");
        StringBuilder sb = new StringBuilder(workDir);
        if (!workDir.endsWith("/")) {
            sb.append('/');
        }
        sb.append(group)
                .append(";")
                .append(localHostAddr)
                .append(new UID().toString());

        return sb.toString().replaceAll("[-:]", "") + UploadService.suffix ;
    }


    private static DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyyMMddHH");

    static String group(int day, int hour, int advertiserId){
        return day + (hour>9? "" + hour :"0" + hour) + "_" + advertiserId % 2 ;
    }

    @Override
    public String remotePath(String localFile) {
        final File file = new File(localFile);
        String group = file.getName().split(";")[0];
        String[] items = group.split("_");
        final DateTimeFormatter formatter = DateTimeFormat.forPattern("'/tmp/test/" + table + "/pday='yyyyMMdd'/phour='H'/'");
        DateTime dateCreated = fmt.parseDateTime(items[0]) ;
        String prefix = formatter.print(dateCreated) + "adv=" + items[1] + "/";
        return prefix +  file.getName();
    }

    public static String localHostAddr;
    static {
        try {
            localHostAddr = InetAddress.getLocalHost().getHostName() ;
        } catch (UnknownHostException e) {
            localHostAddr = "UNKONWN_HOST";
        }
    }
}
