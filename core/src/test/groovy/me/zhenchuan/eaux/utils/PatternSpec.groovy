package me.zhenchuan.eaux.utils

import spock.lang.Specification

import java.util.regex.Pattern

/**
 * Created by liuzhenchuan@foxmail.com on 8/5/16.
 */
class PatternSpec extends Specification{

    def "test multi split"(){
        def data = "A#B,D#C"
        def data2 = "A[#,]C"

        when:
        def items = data.split("[#,]")
        then:
        items.length == 4

        when:
        items = data.split(Pattern.quote("[#,]"))
        then:
        items.length == 1

        when:
        items = data2.split(Pattern.quote("[#,]"))
        then:
        items.length == 2
    }

    def "test quote replaceAll"(){
        def data1 = ".*bbbb"

        when:
        def result = data1.replaceAll(Pattern.quote(".*"),"")
        then:
        result.length() == 4

        when:
        result = data1.replaceAll(".*","")
        then:
        result.length() == 0

    }

}
