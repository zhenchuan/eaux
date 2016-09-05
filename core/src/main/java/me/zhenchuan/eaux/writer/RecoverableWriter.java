package me.zhenchuan.eaux.writer;

/**
 * Created by liuzhenchuan@foxmail.com on 7/29/15.
 */
public interface RecoverableWriter extends Writer {

    long recoverWith(CommitLog.Processor processor,Boolean persist);

}
