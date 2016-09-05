package me.zhenchuan.eaux.writer

import org.apache.commons.lang.RandomStringUtils

/**
 * Created by liuzhenchuan@foxmail.com on 8/4/16.
 */
class CommitLogSpec extends spock.lang.Specification {

    static int FILE_SIZE = 1000 * 1024;

    def "create-write-read-recycle"() {
        def file = File.createTempFile("orc","lzc")
        def commitLog = new CommitLog(file,FILE_SIZE)

        when:
        (1 .. 1000).each{
            //can't use [RandomStringUtils.random(1024)]
            def line = RandomStringUtils.randomAlphabetic(1024)
            if(commitLog.hasCapacityFor(line)){
                commitLog.append(line) ;
            }
        }
        then:
        commitLog.entryNumbers == 1000

        when:
        commitLog.recycle()
        then:
        commitLog.entryNumbers == 0

        when:
        (1 .. 100000).each{
            def line = RandomStringUtils.randomAlphabetic(1024)
            if(commitLog.hasCapacityFor(line)){
                commitLog.append(line) ;
            }
        }
        then:
        commitLog.entryNumbers < 100000

        when:
        commitLog.delete()
        then:
        !file.exists()
    }


    def "recover"(){
        def file = File.createTempFile("orc","lzc")
        def commitLog = new CommitLog(file,FILE_SIZE)

        when:
        (1 .. 1000).each{
            def line = RandomStringUtils.randomAlphabetic(1024)
            if(commitLog.hasCapacityFor(line)){
                commitLog.append(line) ;
            }
        }
        then:
        commitLog.entryNumbers == 1000

        when:
        def num = 0
        commitLog.recover(new CommitLog.Processor() {
            @Override
            void process(byte[] message) {
                num ++
            }
        })
        then:
        num == 1000

    }

}
