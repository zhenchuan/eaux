package me.zhenchuan.eaux.ext.rmq;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import me.zhenchuan.eaux.source.Source;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by liuzhenchuan@foxmail.com on 7/28/16.
 */
public abstract class RocketMqSource extends Source {

    private static final Logger log = LoggerFactory.getLogger(RocketMqSource.class);

    private DefaultMQPushConsumer consumer ;

    public RocketMqSource(Configuration configuration){
        String topicList = configuration.getString("topic.list");
        Preconditions.checkNotNull(topicList, "topicList is needed");

        String consumeGroup = configuration.getString("consumer.group");
        Preconditions.checkNotNull(consumeGroup, "consumeGroup is needed");
        String nameServers = configuration.getString("rocketmq.nameServers");
        Preconditions.checkNotNull(nameServers, "nameServers is needed");
        int consumerTimeOutInMS = configuration.getInt("consumer.timeout.ms", 10 * 1000);
        int consumeBatchSize = configuration.getInt("batch.size", 300);
        String offsetReset = configuration.getString("auto.offset.reset","largest");//smallest,largest

        try {
            setupConsumer(consumeGroup, nameServers, consumeBatchSize, Lists.newArrayList(topicList.split(";")), offsetReset);
        } catch (Exception e) {
            throw new RuntimeException("failed to init consumer",e);
        }
    }

    private void setupConsumer(String consumerGroup, String nameServers, int batchSize,
                                 List<String> topicList, String offsetRest) throws MQClientException {
        consumer = new DefaultMQPushConsumer(consumerGroup);
        consumer.setNamesrvAddr(nameServers);
        consumer.setInstanceName(String.valueOf(System.nanoTime()));
        consumer.setMessageModel(MessageModel.CLUSTERING);
        consumer.setConsumeMessageBatchMaxSize(batchSize);

        for(String topic : topicList){
            log.info("subscribe topic:{}",topic);
            consumer.subscribe(topic,"*");
        }

        if("smallest".equals(offsetRest)){
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        }else{
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);     //默认从最近开始消费
        }

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt message : msgs) {
                    try {
                        emit(convert(message));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
    }

    protected abstract Object[] convert(MessageExt message );

    @Override
    public void start() throws Exception {
        super.startSinks();
        try {
            consumer.start();
        } catch (MQClientException e) {
            throw new RuntimeException("启动RocketMq失败~");
        }
    }

    @Override
    public void stop() {
        consumer.shutdown();
        super.stopSinks();
    }
}
