package me.zhenchuan.eaux.upload;

import java.util.Map;

/**
 *
 * Created by liuzhenchuan@foxmail.com on 7/28/15.
 */
public interface FileNameGenerator {
    /****
     * 生成本地路径(保存相关信息供上传使用)
     * @return
     */
    String localPath(Map<String,Object> context);

    /****
     * 通过本地文件名称构建上传路径~
     * @param localFile
     * @return
     */
    String remotePath(String localFile);

}
