package com.itwobyte.framework.message;

import com.itwobyte.common.utils.uuid.UUID;

import java.io.Serializable;

public class RedisMessage  implements Serializable {
    private static final long serialVersionUID = -1L;

    private String messageId;

    private Object data;

    public RedisMessage(Object data) {
        this.data = data;
        this.messageId = UUID.randomUUID().toString();
    }


    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }
}
