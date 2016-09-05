package kafka;

import com.google.common.collect.Lists;
import me.zhenchuan.eaux.sink.HdfsSink;
import me.zhenchuan.eaux.upload.FileNameGenerator;
import me.zhenchuan.eaux.upload.FileRegistry;
import me.zhenchuan.eaux.upload.HiveTableFileNameGenerator;
import me.zhenchuan.eaux.upload.UploadService;
import me.zhenchuan.eaux.upload.impl.AsyncUploadService;
import me.zhenchuan.eaux.writer.FileWriter;
import me.zhenchuan.eaux.writer.GroupFileWriter;
import me.zhenchuan.eaux.writer.RecoverableWriter;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by liuzhenchuan@foxmail.com on 8/10/16.
 */
public class App {

    private static final Logger log = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) throws Exception {
        //grouped sink writer
        Configuration groupedConfiguration = new PropertiesConfiguration("report.properties");
        Utils.decorate(groupedConfiguration);
        FileRegistry groupedFileRegistry = new FileRegistry() ;
        FileNameGenerator groupedFileNameGenerator = new DateAdvertiserNameGenerator(groupedConfiguration) ;

        final RecoverableWriter groupFileWriter = new GroupFileWriter(groupedConfiguration,
                groupedFileRegistry,
                groupedFileNameGenerator);
        groupFileWriter.recoverWith(null,null);
        UploadService groupedUploadService = new AsyncUploadService("grouped",groupedFileRegistry,
                groupedFileNameGenerator,groupedConfiguration);

        HdfsSink groupedHdfsSink = new GroupedHdfsSink(groupedConfiguration,groupFileWriter,groupedUploadService);

        //plain sink writer
        FileRegistry plainFileRegistry = new FileRegistry() ;
        Configuration plainConfiguration = new PropertiesConfiguration("plain.properties");
        Utils.decorate(plainConfiguration);
        HiveTableFileNameGenerator plainNameGenerator = new PlainNameGenerator(plainConfiguration);
        final RecoverableWriter plainWriter = new FileWriter(plainConfiguration, plainFileRegistry,
                plainNameGenerator,null);  //without commit log
        UploadService plainUploadService = new AsyncUploadService("plain",plainFileRegistry,
                plainNameGenerator,plainConfiguration);
        HdfsSink plainSink = new PlainHdfsSink(plainConfiguration,plainWriter,plainUploadService) ;

        //kafka source
        Configuration inputConfiguration = new PropertiesConfiguration("input.kafka.properties");
        final TestKafkaSource kafkaSource = new TestKafkaSource(inputConfiguration);

        kafkaSource.via(Lists.newArrayList(groupedHdfsSink,plainSink)).start();

        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run() {
                try {
                    log.info("stopping service...");
                    kafkaSource.stop();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

    }



}
