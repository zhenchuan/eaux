package me.zhenchuan.eaux;

import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import me.zhenchuan.eaux.sink.queue.SerDe;
import me.zhenchuan.eaux.utils.ByteArrayDataOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by liuzhenchuan@foxmail.com on 8/4/15.
 */
public class DefaultRowSerDe implements SerDe<Row>{

    private static final Logger log = LoggerFactory.getLogger(DefaultRowSerDe.class);

    private static DefaultRowSerDe instance = new DefaultRowSerDe();

    private DefaultRowSerDe(){

    }

    public static DefaultRowSerDe getInstance(){
        return instance;
    }

    private ThreadLocal<ByteArrayOutputStream> outputStream =
            new ThreadLocal<ByteArrayOutputStream>() {
                @Override
                protected ByteArrayOutputStream initialValue() {
                    return new ByteArrayOutputStream();
                }

                @Override
                public ByteArrayOutputStream get() {
                    ByteArrayOutputStream b = super.get();
                    b.reset();
                    return b;
                }
            };

    @Override
    public Row deserialize(byte[] payload) {
        try {
            DataInput dataInput = ByteStreams.newDataInput(payload);
            return readFields(dataInput);
        } catch (Exception e) {
            log.error("Exception on deserialize: " + e.getMessage(), e);
            return null;
        }
    }

    @Override
    public byte[] serialize(Row row) {
        try {
            ByteArrayDataOutput out = new ByteArrayDataOutputStream(outputStream.get());
            writeFields(out,row);
            return out.toByteArray();
        } catch (IOException e) {
            log.error("Exception on serialize: " + e.getMessage(), e);
            return new byte[]{};
        }
    }

    private void writeFields(DataOutput dataOutput,Row row) throws IOException {
        dataOutput.writeUTF(row.group);
        dataOutput.writeInt(row.estimateSize);
        int size = row.fields().length;
        dataOutput.writeInt(size);
        for (Object field : row.fields()) {
            if (field instanceof String) {
                dataOutput.writeChar('s');
                dataOutput.writeUTF((String) field);
            } else if (field instanceof Long) {
                dataOutput.writeChar('L');
                dataOutput.writeLong((Long) field);
            } else if (field instanceof Integer) {
                dataOutput.writeChar('I');
                dataOutput.writeInt((Integer) field);
            } else if (field instanceof Double) {
                dataOutput.writeChar('D');
                dataOutput.writeDouble((Double) field);
            } else if (field instanceof Byte) {
                dataOutput.writeChar('B');
                dataOutput.writeByte((Byte) field);
            } else if (field instanceof Short) {
                dataOutput.writeChar('S');
                dataOutput.writeShort((Short) field);
            } else if (field instanceof Float) {
                dataOutput.writeChar('F');
                dataOutput.writeFloat((Float) field);
            } else {
                throw new IllegalArgumentException(field + "\t is not a primitive type~");
            }
        }
    }

    private Row readFields(DataInput dataInput) throws IOException {
        String group = dataInput.readUTF();
        int estimateSize = dataInput.readInt();
        int size = dataInput.readInt();
        Object[] c = new Object[size];
        for(int i = 0 ; i < size ; i++){
            switch (dataInput.readChar()){
                case 's':
                    c[i] = dataInput.readUTF();
                    break;
                case 'L':
                    c[i] = dataInput.readLong();
                    break;
                case 'I':
                    c[i] = dataInput.readInt();
                    break;
                case 'D':
                    c[i] = dataInput.readDouble();
                    break;
                case 'S':
                    c[i] = dataInput.readShort();
                    break;
                case 'B':
                    c[i] = dataInput.readBoolean();
                    break;
                default:
                    throw new IllegalArgumentException(" only support [sLIDSB] type~");

            }
        }
        return new Row(c,group,estimateSize);

    }
}
