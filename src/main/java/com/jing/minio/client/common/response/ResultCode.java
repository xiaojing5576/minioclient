package com.jing.minio.client.common.response;

public enum ResultCode {
    //全局
    OK(0, "ok"),
    ERROR(500, "系统维护"),
    UNAUTHORIZED(403, "未授权"),
    SESSION_ERR(405, "未登录或身份认证已过期"),

    //minio 相关
    MINIO_CLIENT_ERR(10001,"minio 客户端出现异常"),

    ;

    private Integer code;

    private String msg;

    ResultCode(int code, String msg) {
        this.code = code;
        this.msg = msg;
    }

    public static ResultCode getResultCode(int code) {
        ResultCode resultCode = null;
        for (ResultCode result : values()) {
            if (code == result.getCode().intValue()) {
                resultCode = result;
                break;
            }
        }
        return resultCode;
    }

    public Integer getCode() {
        return Integer.valueOf(this.code);
    }

    public void setCode(Integer code) {
        this.code = code.intValue();
    }

    public String getMsg() {
        return this.msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }
}
