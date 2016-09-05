package me.zhenchuan.eaux.ext.kafka;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import kafka.consumer.*;
import kafka.javaapi.consumer.ConsumerConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by liuzhenchuan@foxmail.com on 12/23/15.
 */
public class KafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);

    protected final Properties consumerProps;

    private ConsumerConnector connector;
    private ExecutorService executor;

    private List<Future<?>> runners = new ArrayList<Future<?>>();
    private volatile boolean running = false;

    private MessageProcessor processor ;
    private Map<String,Integer> topicCountMap ;

    public KafkaConsumer(Properties consumerProps,Map<String,Integer> topicCountMap) {
        Preconditions.checkNotNull(consumerProps);
        Preconditions.checkNotNull(consumerProps.getProperty("group.id"));
        Preconditions.checkNotNull(consumerProps.getProperty("zookeeper.connect"));
        String timeoutStr = consumerProps.getProperty("consumer.timeout.ms");
        Preconditions.checkNotNull(timeoutStr);
        Preconditions.checkArgument(Long.parseLong(timeoutStr) > 0);

        Preconditions.checkArgument(topicCountMap!=null && topicCountMap.size() > 0 ,"at least have one topic set up !");

        this.consumerProps = consumerProps;
        this.topicCountMap = topicCountMap;
    }

    public KafkaConsumer(
            Properties consumerProps,
            String topic,
            int readers
    ) {
        this(consumerProps,ImmutableMap.of(topic, readers == 0 ? 1 : readers));
    }

    public void setProcessor(MessageProcessor processor) {
        this.processor = processor;
    }

    public void start() throws Exception {
        executor = Executors.newCachedThreadPool(
                new ThreadFactoryBuilder().setNameFormat("KafkaConsumer-%d").build());
        connector = Consumer.createJavaConsumerConnector(new ConsumerConfig(consumerProps));

        final Map<String, List<KafkaStream<byte[], byte[]>>> streams = connector.createMessageStreams(topicCountMap);

        validateTopic(topicCountMap,streams);

        running = true;

        for(Map.Entry<String, List<KafkaStream<byte[], byte[]>>> entry : streams.entrySet()){
            final String topic = entry.getKey();
            final List<KafkaStream<byte[], byte[]>> streamList = entry.getValue();
            register(topic, streamList);
        }
        log.info("consumer started!");
    }

    public boolean isRunning() {
        return running;
    }

    public Map<String, Integer> getTopicCountMap() {
        return topicCountMap;
    }

    private void register(final String topic, List<KafkaStream<byte[], byte[]>> streamList) {
        for (KafkaStream<byte[], byte[]> stream : streamList) {
            final ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
            runners.add(
                    executor.submit(new Runnable() {
                        @Override
                        public void run() {
                            while (running) {
                                try {
                                    byte[] message = iterator.next().message();
                                    processor.process(topic,message);
                                } catch (ConsumerTimeoutException timeoutException) {
                                    // do nothing
                                } catch (Exception e) {
                                    log.error("Exception on consuming kafka with topic: " + topic, e);
                                }
                            }
                        }
                    })
            );
        }
    }

    private void validateTopic(Map<String, Integer> topicCountMap, Map<String, List<KafkaStream<byte[], byte[]>>> streams) {
        for(Map.Entry<String,Integer> entry : topicCountMap.entrySet()){
            String topic = entry.getKey();
            final List<KafkaStream<byte[], byte[]>> streamList = streams.get(topic);
            if (streamList == null) {
                throw new RuntimeException(topic + " is not valid");
            }
        }
    }

    public void shutdown() {
        log.info("stop consume");
        stop();
        log.info("shutdown connector");
        connector.shutdown();
    }

    private void stop() {
        running = false;
        try {
            for (Future<?> runner : runners) {
                runner.get();
            }
        } catch (InterruptedException e) {
            // do nothing
        } catch (ExecutionException e) {
            log.error("Exception on stopping the task", e);
        }
    }

}
