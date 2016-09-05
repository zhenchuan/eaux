package me.zhenchuan.eaux.utils

import spock.lang.Specification

import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * Created by liuzhenchuan@foxmail.com on 8/12/16.
 */
class SchedulerSpec extends Specification{

    def "test shutdown"(){
        def sec = Executors.newSingleThreadScheduledExecutor();
        def count = 0
        sec.scheduleAtFixedRate(
                new Runnable() {
                    @Override
                    void run() {
                        println(new Date().format("HHmmss.SSS"))
                        count ++
                        Thread.sleep(1000 * 20);
                    }
                },0,5, TimeUnit.SECONDS);
        when:
        Thread.sleep(30 * 1000)
        then:
        count == 2

        when:
        println(new Date().format("---HHmmss.SSS"))
        sec.shutdown()
        println(new Date().format("---HHmmss.SSS"))
        then:
        sec.isShutdown()
    }

}
