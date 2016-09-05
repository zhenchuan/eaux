package kafka;

import me.zhenchuan.eaux.upload.HiveTableFileNameGenerator;
import org.apache.commons.configuration.Configuration;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.File;

/**
 * Created by liuzhenchuan@foxmail.com on 9/3/16.
 */
public class PlainNameGenerator extends HiveTableFileNameGenerator{

    public PlainNameGenerator(Configuration configuration) {
        super(configuration);
    }


    @Override
    public String remotePath(String localFile) {
        final File file = new File(localFile);
        final DateTimeFormatter formatter = DateTimeFormat.forPattern("'/tmp/test/" + table + "/pday='yyyyMMdd'/phour='H'/'");
        DateTime dateCreated = FileNameFormatter.getFileCreateTimeFromPath(new File(localFile)); //当前文件的创建时间
        String prefix = formatter.print(dateCreated);
        return prefix +  file.getName();
    }
}
