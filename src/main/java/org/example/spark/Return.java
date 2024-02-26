package org.example.spark;

import java.io.Serializable;

public class Return implements Serializable {
    private String orderId;
    private String returned;

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getReturned() {
        return returned;
    }

    public void setReturned(String returned) {
        this.returned = returned;
    }

    public Return(String orderId, String returned) {
        this.orderId = orderId;
        this.returned = returned;
    }

    public Return() {
    }
}
