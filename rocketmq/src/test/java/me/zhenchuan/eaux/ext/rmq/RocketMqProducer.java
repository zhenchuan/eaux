package me.zhenchuan.eaux.ext.rmq;

import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;

/**
 * 用于模拟rocketmq的数据
 * Created by liuzhenchuan@foxmail.com on 4/29/15.
 */
public class RocketMqProducer {

    private static final Logger log = LoggerFactory.getLogger(RocketMqProducer.class);

    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("ProducerGroupName");
        producer.setNamesrvAddr("localhost:9876");
        producer.setInstanceName("Producer");
        producer.start();

        String[] topicList = new String[]{"impression","click"};

        String line = null;
        int count = 0 ;
        BufferedReader br = new BufferedReader(new FileReader("/tmp/x.done")) ;
        while((line = br.readLine())!=null){
            Message message = new Message(topicList[count%4],line.getBytes());
            SendResult result = producer.send(message);
            Thread.sleep(1000);
        }

        producer.shutdown();

    }
}
