package com.jing.minio.client.config;

import com.jing.minio.client.common.exception.ServiceException;
import com.jing.minio.client.common.response.ResultCode;
import io.minio.*;
import io.minio.errors.ErrorResponseException;
import io.minio.messages.DeleteError;
import io.minio.messages.DeleteObject;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: huangjingyan-200681
 * @Date: 2021/9/15 13:31
 * @Mail: huangjingyan@eastmoney.com
 * @Description: TODO
 * @Version: 1.0
 **/

@Component
public class MinioClient {

    @Value("${minio.url}")
    private String url;

    @Value("${minio.port}")
    private Integer port;

    @Value("${minio.accessKey}")
    private String accessKey;

    @Value("${minio.secretKey}")
    private String secretKey;

    @Value("${minio.ssl:false}")
    private Boolean ssl;

    private io.minio.MinioClient minioClient;

    @PostConstruct
    public void init(){
        minioClient = io.minio.MinioClient.builder()
                                            .endpoint(url,port,ssl)
                                            .credentials(accessKey,secretKey)
                                            .build();
    }

    /**
     * 判断bucket是否存在
     * @param bucketName
     * @return
     */
    public Boolean bucketExist(String bucketName){
        BucketExistsArgs args = BucketExistsArgs.builder().bucket(bucketName).build();
        try {
            Boolean exist = minioClient.bucketExists(args);
            return exist;
        }catch (Exception e){
            throw new ServiceException(ResultCode.MINIO_CLIENT_ERR);
        }
    }

    /**
     * 拷贝bucket中的对象至另一个地方
     * @param objectName
     * @param bucketName
     * @param srcObjectName
     * @param srcBucketName
     * @return
     */
    public Boolean copyObject(String objectName,String bucketName,String srcObjectName,String srcBucketName){
        CopyObjectArgs args = CopyObjectArgs.builder().bucket(bucketName)
                                            .object(objectName)
                                            .source(CopySource.builder().bucket(srcBucketName).object(srcObjectName).build())
                                            .build();
        try{
            ObjectWriteResponse response = minioClient.copyObject(args);
            if(StringUtils.isNotBlank(response.versionId())){
                return true;
            }
            return false;
        }catch (Exception e){
            throw new ServiceException(ResultCode.MINIO_CLIENT_ERR);
        }
    }

    /**
     * 移动存储对象
     * @param objectName
     * @param bucketName
     * @param srcObjectName
     * @param srcBucketName
     * @return
     */
    public Boolean moveObject(String objectName,String bucketName,String srcObjectName,String srcBucketName){
        CopyObjectArgs copyArgs = CopyObjectArgs.builder()
                .bucket(bucketName)
                .object(objectName)
                .source(CopySource.builder().bucket(srcBucketName).object(srcObjectName).build())
                .build();
        RemoveObjectArgs removeArgs = RemoveObjectArgs.builder()
                .bucket(bucketName)
                .object(objectName)
                .build();
        try{
            ObjectWriteResponse writeResponse = minioClient.copyObject(copyArgs);
            if(StringUtils.isNotBlank(writeResponse.versionId())){
                minioClient.removeObject(removeArgs);
                return true;
            }
            return false;
        }catch (Exception e){
            throw new ServiceException(ResultCode.MINIO_CLIENT_ERR);
        }
    }

    /**
     *  替换掉指定路径下的存储对象
     * @param objectName
     * @param bucketName
     * @return
     */
    public Boolean replaceObject(String objectName,String bucketName){
        PutObjectArgs putArgs = PutObjectArgs.builder()
                .bucket(bucketName)
                .object(objectName)
                .build();

        RemoveObjectArgs removeArgs = RemoveObjectArgs.builder()
                .bucket(bucketName)
                .object(objectName)
                .build();
        try{
            minioClient.removeObject(removeArgs);
            ObjectWriteResponse writeResponse = minioClient.putObject(putArgs);
            if(StringUtils.isNotBlank(writeResponse.versionId())){
                return true;
            }
            return false;
        }catch (Exception e){
            throw new ServiceException(ResultCode.MINIO_CLIENT_ERR);
        }
    }

    /**
     * 判断存储对象是否存在
     * @param objectName
     * @param bucketName
     * @return
     */
    public Boolean existObject(String objectName,String bucketName){
        GetObjectArgs getArgs = GetObjectArgs.builder()
                .bucket(bucketName)
                .object(objectName)
                .build();
        try{
            GetObjectResponse response = minioClient.getObject(getArgs);
            return true;
        }catch (ErrorResponseException e){
            if(e.errorResponse().code().equals("NoSuchKey")){
                return false;
            }
            throw new ServiceException(ResultCode.MINIO_CLIENT_ERR);
        } catch (Exception e){
            throw new ServiceException(ResultCode.MINIO_CLIENT_ERR);
        }
    }

    /**
     * 删除某个存储对象
     * @param objectName
     * @param bucketName
     * @return
     */
    public Boolean deleteObject(String objectName,String bucketName){
        try {
            RemoveObjectArgs removeArgs = RemoveObjectArgs.builder().bucket(bucketName).object(objectName).build();
            minioClient.removeObject(removeArgs);
            return true;
        }catch (Exception e){
            throw new ServiceException(ResultCode.MINIO_CLIENT_ERR);
        }
    }

    /**
     * 批量删除存储对象
     * @param objectNames
     * @param bucketName
     * @return
     */
    public List<String> deleteObjects(List<String> objectNames,String bucketName){
        List<DeleteObject> deleteObjects = new ArrayList<>();
        objectNames.forEach(x->{
            DeleteObject deleteObject = new DeleteObject(x);
            deleteObjects.add(deleteObject);
        });
        RemoveObjectsArgs removeArgs = RemoveObjectsArgs.builder().bucket(bucketName).objects(deleteObjects).build();
        try{
            List<String> errObjectNames = new ArrayList<>();
            Iterable<Result<DeleteError>> responses = minioClient.removeObjects(removeArgs);
            responses.forEach(x->{
                try {
                    errObjectNames.add(x.get().objectName());
                }catch (Exception e){
                }
            });
            return errObjectNames;
        }catch (Exception e){
            throw new ServiceException(ResultCode.MINIO_CLIENT_ERR);
        }
    }


}
