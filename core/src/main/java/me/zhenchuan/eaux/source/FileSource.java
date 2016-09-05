package me.zhenchuan.eaux.source;

import java.io.BufferedReader;
import java.io.FileReader;

/**
 * Created by liuzhenchuan@foxmail.com on 8/2/15.
 */
public abstract class FileSource extends Source{

    private String file;

    private volatile boolean runnable;

    public FileSource(String file){
        this.file = file;
    }

    @Override
    public void start() throws Exception {
        super.startSinks();
        this.runnable = true;
        String line = null;
        BufferedReader br = new BufferedReader(new FileReader(file));
        while(runnable && (line=br.readLine())!=null){
            emit(convert(line));
        }
    }

    protected abstract Object[] convert(String line);

    public void stop(){
        this.runnable = false;
        super.stopSinks();
    }

}
