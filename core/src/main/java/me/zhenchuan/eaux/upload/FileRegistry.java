package me.zhenchuan.eaux.upload;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by liuzhenchuan@foxmail.com on 7/28/15.
 */
public class FileRegistry {

    private static final Logger log = LoggerFactory.getLogger(FileRegistry.class);

    private final BlockingQueue<String> queue;

    public FileRegistry() {
        this(10000);
    }

    public FileRegistry(int size) {
        queue = new LinkedBlockingQueue<String>(size);
    }

    public synchronized   boolean send(String filename){
          if(! contains(filename)){
              return queue.offer(filename);
          }
          return false;
    }

    public synchronized String recv() {
        try {
            return queue.poll(1000 * 6, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            return null;
        }
    }

    public synchronized String recv(int waitTimeoutInSeconds) {
        try {
            return queue.poll(1000 * waitTimeoutInSeconds, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            return null;
        }
    }

    public synchronized boolean contains(final String path){
        return queue.contains(path)
                || queue.contains(path.replace("file:",""))
                || queue.contains(path.replace("file://",""));
    }

    public long size(){
        return queue.size();
    }


}
