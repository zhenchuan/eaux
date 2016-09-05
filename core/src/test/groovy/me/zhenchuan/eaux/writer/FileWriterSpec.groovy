package me.zhenchuan.eaux.writer

import me.zhenchuan.eaux.Row
import me.zhenchuan.eaux.upload.FileNameGenerator
import me.zhenchuan.eaux.upload.FileRegistry
import org.apache.commons.lang.RandomStringUtils
import org.apache.commons.lang.math.RandomUtils
import org.apache.commons.lang.time.DateUtils
import org.joda.time.Period
import spock.lang.Specification

/**
 * Created by liuzhenchuan@foxmail.com on 8/4/16.
 */
class FileWriterSpec extends Specification{

    def fileNameGenerator = new FileNameGenerator() {
        @Override
        public String localPath(Map<String,Object> context) {
            def file = new File("/tmp/single/${System.currentTimeMillis()}.lzc")//File.createTempFile(System.currentTimeMillis() as String,".lzc");
            println(file)
            return file.getAbsoluteFile();
        }

        @Override
        public String remotePath(String filename) {
            return filename;
        }
    };


    def "create file writer without commit_log"(){

        def fileRegistry = new FileRegistry();


        def fieldName = ["name", "gender", "age", "birthday"] as String[]
        def fieldType = ["string", "int", "int", "string"] as String[]
        FileWriter.Builder builder = new FileWriter.Builder()
                .setFieldNames(fieldName)
                .setFieldTypes(fieldType)
                .setRotationPeriod(new Period("PT1m"))
                .setCommitLog(null)
                .setFileRegistry(fileRegistry)
                .setNameGenerator(fileNameGenerator)
        ;

        when:
        final FileWriter fileWriter = builder.createSingleWriter();
        then:
        fileWriter != null

        when:
        for(int i = 0 ; i < 126;i++) {
            final String name = RandomStringUtils.randomAlphabetic(2);
            final int gender = RandomUtils.nextInt(2);
            final int age = RandomUtils.nextInt(100);
            int month = RandomUtils.nextInt(12);    //月份
            String birthday = DateUtils.addMonths(new Date(), month).format("yyyyMMdd");
            Row row = new Row([name,gender,age,birthday] as Object[],null,100);
            fileWriter.add(row)
            Thread.sleep(1000)
        }
        then:
        fileWriter.writtenLines() == 6
        fileRegistry.size() == 2
        //2 * fileRegistry.send(_)   //只有mock后才能使用~

        when:
        fileWriter.close();
        then:
        fileRegistry.size() == 3
        //3 * fileRegistry.send(_)
        fileWriter.writtenLines() == 6
        fileWriter.writer == null


    }

}
