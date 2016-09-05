package me.zhenchuan.eaux.ext.kafka;

import com.google.common.collect.Maps;
import me.zhenchuan.eaux.source.Source;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

/**
 * Created by liuzhenchuan@foxmail.com on 7/28/15.
 */
public abstract class KafkaSource extends Source {

    private static final Logger log = LoggerFactory.getLogger(KafkaSource.class);

    private final KafkaConsumer kafkaConsumer ;

    public KafkaSource(Configuration configuration){
        Properties consumerProperties = ConfigurationConverter.getProperties(configuration);
        //设置每个topic及其消费的线程数
        Map<String,Integer> map = Maps.newHashMap();
        String consumerTopics = consumerProperties.getProperty("consumerTopics");
        String[] items = StringUtils.split(consumerTopics,",");
        for(String item : items) {
            String[] ii = item.split(":");
            map.put(ii[0],Integer.parseInt(ii[1]));
        }
        kafkaConsumer = new KafkaConsumer(consumerProperties,map);
        kafkaConsumer.setProcessor(new MessageProcessor() {
            @Override
            public boolean process(String topic, byte[] message) {
                return emit(convert(topic,message));
            };
        });
    }

    protected abstract Object[] convert(String topic,byte[] message );

    @Override
    public void start() throws Exception {
        startSinks();
        kafkaConsumer.start();
    }

    @Override
    public void stop() throws Exception {
        kafkaConsumer.shutdown();
        stopSinks();
    }
}
