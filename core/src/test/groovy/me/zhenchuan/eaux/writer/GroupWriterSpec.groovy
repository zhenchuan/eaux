package me.zhenchuan.eaux.writer

import me.zhenchuan.eaux.Row
import me.zhenchuan.eaux.upload.FileNameGenerator
import me.zhenchuan.eaux.upload.FileRegistry
import me.zhenchuan.eaux.utils.Granularity
import org.apache.commons.lang.RandomStringUtils
import org.apache.commons.lang.math.RandomUtils
import org.apache.commons.lang.time.DateUtils
import org.joda.time.Period
import spock.lang.Shared
import spock.lang.Specification

/**
 * Created by liuzhenchuan@foxmail.com on 8/9/16.
 */
class GroupWriterSpec extends Specification{

    @Shared def builder = new GroupFileWriter.Builder();  //必须使用static或者@shard , 全局的

    def setupSpec(){   //同JUnit‘s @BeforeClass 类似 , cleanupSpec
        def fieldName = ["name", "gender", "age", "birthday"] as String[]
        def fieldType = ["string", "int", "int", "string"] as String[]

        def fileNameGenerator = new FileNameGenerator() {
            @Override
            public String localPath(Map<String,Object> context) {
                def group = context.get("group")
                def file = new File("/tmp/multi/${group}_${System.currentTimeMillis()}.lzc")//File.createTempFile(System.currentTimeMillis() as String,".lzc");
                return file.getAbsoluteFile();
            }

            @Override
            public String remotePath(String filename) {
                return filename;
            }
        };
        builder.setFieldNames(fieldName)
                .setFieldTypes(fieldType)
                .setFileDirectory("/tmp/multi")
                .setCommitLogMaxSize(1024 * 1024)
                .setExpirySegmentCheckInSeconds(60)
                .setExpiryRotationPeriodInTimes(4)
                .setGranularity(Granularity.HOUR)
                .setRotationPeriod(Period.minutes(1))
                .setFileNameGenerator(fileNameGenerator)
                //.setFileRegistry(fileRegistry)
                .setUseCommitLog(true)
    }

    def "test brand new create "(){
        setup:
        new File("/tmp/multi").deleteDir()
        def partitions = 4
        def fileRegistry = new FileRegistry();
        builder.setFileRegistry(fileRegistry)
        when:
        GroupFileWriter groupFileWriter = builder.createMultiWriter();
        then:
        groupFileWriter !=null

        when:
        for(int i = 0 ; i < 126;i++) {
            final String name = RandomStringUtils.randomAlphabetic(2);
            final int gender = RandomUtils.nextInt(2);
            final int age = RandomUtils.nextInt(100);
            int month = i % partitions + 1;    //月份
            String birthday = DateUtils.addMonths(new Date(), month).format("yyyyMMdd");
            Row row = new Row([name, gender, age, birthday] as Object[],String.valueOf(month),100);
            groupFileWriter.add(row)
            Thread.sleep(1000)
        }

        then:
        fileRegistry.size() == partitions * 2

        when:
        groupFileWriter.close()
        then:
        fileRegistry.size() == partitions * 3

    }

    def "test with commit log recover"(){
        setup:
        new File("/tmp/multi").deleteDir()
        def partitions = 4
        def fileRegistry = new FileRegistry();
        builder.setFileRegistry(fileRegistry)
        GroupFileWriter groupFileWriter = builder.createMultiWriter();
        for(int i = 0 ; i < 7;i++) {
            final String name = RandomStringUtils.randomAlphabetic(2);
            final int gender = RandomUtils.nextInt(2);
            final int age = RandomUtils.nextInt(100);
            int month = i % partitions + 1;    //月份
            String birthday = DateUtils.addMonths(new Date(), month).format("yyyyMMdd");
            Row row = new Row([name, gender, age, birthday] as Object[],String.valueOf(month),100);
            groupFileWriter.add(row)
        }
        groupFileWriter = builder.createMultiWriter();
        when:
        groupFileWriter.recoverWith(null,true);
        Thread.sleep(30 * 100)
        then:
        fileRegistry.size() == partitions
    }

}
