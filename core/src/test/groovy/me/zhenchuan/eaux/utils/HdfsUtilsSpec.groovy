package me.zhenchuan.eaux.utils

import spock.lang.Shared
import spock.lang.Specification

/**
 * Created by liuzhenchuan@foxmail.com on 8/11/16.
 */
class HdfsUtilsSpec extends Specification{

    @Shared HdfsUtils hdfsUtils

    def setupSpec(){
        hdfsUtils = new HdfsUtils("/tmp/core-site.xml","/tmp/hdfs-site.xml")
    }

    def "test exist"() {
        when:
        def exist = hdfsUtils.exist("/tmp/not-exits--fkejkfj-djfekjf--hhhhh")
        then:
        !exist
    }

    def "test upload & delete"(){
         setup:
         def local = "/tmp/exist-hello-world"
         def remote = "/tmp/test/exist-hello-world"
         def localFile = new File(local)
         localFile << "hello world"
         when:
         hdfsUtils.upload(remote,local)
         then:
         hdfsUtils.exist(remote)

         when:
         hdfsUtils.delete(remote)
        then:
        !hdfsUtils.exist(remote)

        when:
        localFile.delete()
        then:
        !localFile.exists()

    }

}
