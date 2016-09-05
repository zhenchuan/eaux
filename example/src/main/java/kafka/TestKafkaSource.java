package kafka;

import me.zhenchuan.eaux.ext.kafka.KafkaSource;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;

/**
 * Created by liuzhenchuan@foxmail.com on 8/10/16.
 */
public class TestKafkaSource extends KafkaSource {

    public TestKafkaSource(Configuration configuration) {
        super(configuration);
    }

    @Override
    protected Object[] convert(String topic, byte[] message) {
        try {
            String row = new String(message,"utf-8") ;
            String[] cols = StringUtils.splitByWholeSeparatorPreserveAllTokens(
                    row, "\t");
            String actionRequestTime = cols[6];
            int day = Integer.parseInt(actionRequestTime.substring(0, 8));
            int hour = Integer.parseInt(actionRequestTime.substring(8, 10));
            String platform = cols[3] ;
            String bid_policy_data = (cols[85]);
            String bid_rank_data = (cols[86]);
            int id_advertiser_id = Integer.parseInt(cols[50]) ;
            long imp = "imp".equals(topic) ? 1:0 ;
            long click = "click".equals(topic) ? 0:1 ;
            return new Object[]{day,hour,id_advertiser_id,platform,bid_policy_data,bid_rank_data,imp,click};
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
