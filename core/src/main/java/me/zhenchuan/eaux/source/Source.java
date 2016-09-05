package me.zhenchuan.eaux.source;

import com.codahale.metrics.Meter;
import com.google.common.collect.Lists;
import me.zhenchuan.eaux.sink.HdfsSink;
import me.zhenchuan.eaux.utils.Metrics;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by liuzhenchuan@foxmail.com on 7/28/15.
 */
public abstract class Source {

    private static final Logger log = LoggerFactory.getLogger(Source.class);

    protected Meter successMessageCount = Metrics.meter(this.getClass(),"emit.success");

    protected Meter failedMessageCount = Metrics.meter(this.getClass(),"emit.failed");

    protected List<HdfsSink> hdfsSinks = Lists.newArrayList();

    public abstract void start() throws Exception ;

    public abstract void stop() throws Exception ;

    public void startSinks() throws Exception {
        if(CollectionUtils.isEmpty(hdfsSinks)){
            throw new RuntimeException("must have at least one hdfs sinks.");
        }
        for(HdfsSink sink: hdfsSinks){
            log.info("start sink:{}",sink.getName());
            sink.start();
        }
    }

    public void stopSinks(){
        for(HdfsSink sink: hdfsSinks){
            log.info("stop sink:{}",sink.getName());
            //TODO parallel stop here!
            sink.stop();
        }
        log.info("all sink stoped!");
    }

    /****
     * 调用这个方法不能阻塞~
     * @param line
     * @return
     */
    protected boolean emit(Object[] line){
        if(line == null){
            return true;
        }
        try{
            for(HdfsSink hdfsSink : hdfsSinks){
                hdfsSink.writeMessage(line);
            }
            successMessageCount.mark();
            return true;
        }catch (Exception e){
            failedMessageCount.mark();
            e.printStackTrace();
        }
        return false;
    }

    public Source via(List<HdfsSink> hdfsSinks){
        this.hdfsSinks = hdfsSinks;
        return this;
    }

}
