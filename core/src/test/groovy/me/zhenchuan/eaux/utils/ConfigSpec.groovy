package me.zhenchuan.eaux.utils

import org.apache.commons.configuration.Configuration
import org.apache.commons.configuration.PropertiesConfiguration

/**
 * Created by liuzhenchuan@foxmail.com on 8/4/16.
 */
class ConfigSpec extends spock.lang.Specification{

    def "test add set"(){
        when:
        Configuration config = new PropertiesConfiguration("example.properties");
        config.setProperty("field.name.list",["pool_id"] as String[]);
        config.addProperty("field.idx.list",[1,2,3,4] as int[]);

        then:
        config.getList("field.idx.list").size()==4
        config.getList("field.idx.list")[0] == 1
        config.getStringArray("field.name.list").length == 1

    }

}
