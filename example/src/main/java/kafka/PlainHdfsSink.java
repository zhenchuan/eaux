package kafka;

import me.zhenchuan.eaux.Row;
import me.zhenchuan.eaux.sink.HdfsSink;
import me.zhenchuan.eaux.upload.UploadService;
import me.zhenchuan.eaux.writer.RecoverableWriter;
import org.apache.commons.configuration.Configuration;

import java.io.IOException;

/**
 * Created by liuzhenchuan@foxmail.com on 9/3/16.
 */
public class PlainHdfsSink extends HdfsSink{

    public PlainHdfsSink(Configuration configuration, RecoverableWriter writer, UploadService uploadService) throws IOException {
        super(configuration, writer, uploadService);
    }

    @Override
    protected Row decorate(Object[] message) {
        return new Row(new Object[]{message[0],message[1],message[2],message[3],message[6],message[7]},
                100);
    }
}
