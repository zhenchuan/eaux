package me.zhenchuan.eaux.writer;

import me.zhenchuan.eaux.utils.Granularity;
import org.joda.time.DateTime;
import org.joda.time.Period;

import java.util.Date;

/**
 * Created by liuzhenchuan@foxmail.com on 7/28/15.
 */
public class TimeRollingOrcSupport extends OrcSupport {

    protected Granularity granularity ;
    protected Period rotationPeriod ;

    private long nextRotation ;


    protected boolean timeFired() {
        return System.currentTimeMillis() > nextRotation;
    }

    protected void calcNextRotation(){
        DateTime currentDateTime = new DateTime();
        long nextRotation = currentDateTime.plus(rotationPeriod).getMillis();
        if(granularity!=null){ //下一次rotation的时间不能超过当前设置的时间间隔.
            long expectedBreak = granularity.next(currentDateTime).getMillis();
            if(nextRotation >expectedBreak){
                nextRotation = expectedBreak;
            }
        }
        this.nextRotation = nextRotation;
    }

}
