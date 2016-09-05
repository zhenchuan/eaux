package me.zhenchuan.eaux.writer;

import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.List;

/**
 * Created by liuzhenchuan@foxmail.com on 8/3/15.
 */
public class OrcSupport {

    protected org.apache.hadoop.hive.ql.io.orc.Writer writer ;
    protected String[] fieldTypes ;
    protected String[] fieldNames ;

    protected Object[] convert(String[] cols){
        try {
            Object[] fieldValues = new Object[cols.length];
            for(int i = 0 ; i < cols.length ;i++){
                String col  = cols[i];
                String filedType = fieldTypes[i];
                if("int".equalsIgnoreCase(filedType)){
                    fieldValues[i] = NumberUtils.toInt(col,0);
                }else if("double".equalsIgnoreCase(filedType)){
                    fieldValues[i] = NumberUtils.toDouble(col,0.0);
                }else if("bigint".equalsIgnoreCase(filedType)){
                    fieldValues[i] = NumberUtils.toLong(col,0l);
                }else if("tinyint".equalsIgnoreCase(filedType)){
                    if(StringUtils.isBlank(col)){
                        fieldValues[i] = 0 ;
                    }else{
                        fieldValues[i] = Byte.parseByte(col);
                    }
                }else{
                    fieldValues[i] = col;
                }
            }
            return fieldValues;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    protected ObjectInspector createInspector(String[] fieldNames, String[] fieldTypes){
        List<ObjectInspector> inspectors = Lists.newArrayList();
        for(String filedType : fieldTypes){
            if("string".equalsIgnoreCase(filedType)){
                inspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
            }else if("int".equalsIgnoreCase(filedType)){
                inspectors.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
            }else if("double".equalsIgnoreCase(filedType)){
                inspectors.add(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
            }else if("bigint".equalsIgnoreCase(filedType)){
                inspectors.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
            }else if("tinyint".equalsIgnoreCase(filedType)){
                inspectors.add(PrimitiveObjectInspectorFactory.javaByteObjectInspector);
            }else if("boolean".equalsIgnoreCase(filedType)){
                inspectors.add(PrimitiveObjectInspectorFactory.javaBooleanObjectInspector);
            }else{
                throw new IllegalArgumentException("当前不支持[" + filedType + "]类型!");
            }
        }
        return ObjectInspectorFactory.getStandardStructObjectInspector(
                Lists.newArrayList(fieldNames),
                inspectors
        );
    }

}
