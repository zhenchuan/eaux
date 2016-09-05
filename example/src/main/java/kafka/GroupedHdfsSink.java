package kafka;

import me.zhenchuan.eaux.Row;
import me.zhenchuan.eaux.sink.HdfsSink;
import me.zhenchuan.eaux.upload.UploadService;
import me.zhenchuan.eaux.writer.RecoverableWriter;
import org.apache.commons.configuration.Configuration;

import java.io.IOException;

/**
 * Created by liuzhenchuan@foxmail.com on 8/10/16.
 */
public class GroupedHdfsSink extends HdfsSink{

    public GroupedHdfsSink(Configuration configuration, RecoverableWriter writer,
                           UploadService uploadService) throws IOException {
        super(configuration, writer, uploadService);
    }

    @Override
    protected Row decorate(Object[] message) {
        return new Row(message,
                DateAdvertiserNameGenerator.group((int)message[0],(int)message[1],(int)message[2])
                ,100
        );
    }

}
