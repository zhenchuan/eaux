package kafka;

import com.google.common.collect.Lists;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by liuzhenchuan@foxmail.com on 3/16/15.
 */
public class Utils {

    private static final Logger log = LoggerFactory.getLogger(Utils.class);

    //全部字段
    static List<String> INPUT_FIELD_NAMES = Lists.newArrayList("day","hour","device","pool_id","id_advertiser_id","id_order_id","id_campaign_id",
            "id_strategy_id","id_creative_id","action_platform","domain","top_level_domain","ad_unit_location","ad_unit_type","id_ad_unit_id",
            "ad_size","ad_unit_floor_price","ad_unit_idx","ad_unit_vi","province_id","city_id","id_channel_id","id_publisher_id",
            "id_vertical_id","bid_policy_version_1","bid_policy_version_2","bid_policy_version_3",
            "bid_policy_version_4","agent_product","device_type","device_brand",
            "device_os","device_os_n","device_carrier_id","agent_type","has_device_ids","id_prefer_deal_id","minute","bid_policy_data",
            "bid_rank_data","finish_rate",
            "holder1","holder2","holder3","holder4","holder5","holder6",
            "agency_cost","agency_srv_cost","adv_cost",
            "bid","","imp","","click","","","","","","","",""
            ,"");

    static List<String> INPUT_FIELD_TYPES = Lists.newArrayList("INT","INT","STRING","INT","INT","INT","INT","INT","INT",
            "STRING","STRING","STRING","STRING","STRING","STRING","STRING","INT","INT","INT","INT","INT","STRING","STRING",
            "STRING","TINYINT","TINYINT","TINYINT","TINYINT","STRING","STRING","STRING","STRING","STRING","STRING","STRING",
            "TINYINT","STRING","INT","STRING","STRING","INT","STRING","STRING","STRING","BIGINT","BIGINT","BIGINT","DOUBLE",
            "DOUBLE","DOUBLE","BIGINT","","BIGINT","","BIGINT","","","","","","","",""
            ,"");


    public static void decorate(Configuration configuration){
        String[] fieldNames = configuration.getStringArray("field.name.list");

        List<String> names = Utils.INPUT_FIELD_NAMES;
        int[] fieldIdxList = new int[fieldNames.length];
        String[] fieldTypeList = new String[fieldNames.length];

        for (int i = 0; i <fieldNames.length; i++) {
            String name = fieldNames[i];
            int idx = names.indexOf(name);
            if(idx == -1){
                log.error("invalid filed name:{}\n available:{}",name,names);
                throw new RuntimeException("invalid field name:" + name);
            }
            String type = Utils.INPUT_FIELD_TYPES.get(idx);
            fieldTypeList[i] = type;
            fieldIdxList[i] = idx;
        }
        configuration.setProperty("field.type.list",fieldTypeList);
        configuration.addProperty("field.idx.list",fieldIdxList);

    }


}
