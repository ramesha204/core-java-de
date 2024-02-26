package org.example.spark;

import java.io.Serializable;

public class Sale implements Serializable {
    private String orderId;
    private String orderDate;
    private String category;
    private String subCategory;
    private String profit;
    private String quantity;

    private String returns;

    public Sale(String orderId, String orderDate, String category, String subCategory, String profit, String quantity,String returns) {
        this.orderId = orderId;
        this.orderDate = orderDate;
        this.category = category;
        this.subCategory = subCategory;
        this.profit = profit;
        this.quantity = quantity;
        this.returns=returns;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getOrderDate() {
        return orderDate;
    }

    public void setOrderDate(String orderDate) {
        this.orderDate = orderDate;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getSubCategory() {
        return subCategory;
    }

    public void setSubCategory(String subCategory) {
        this.subCategory = subCategory;
    }

    public String getProfit() {
        return profit;
    }

    public void setProfit(String profit) {
        this.profit = profit;
    }

    public String getQuantity() {
        return quantity;
    }

    public void setQuantity(String quantity) {
        this.quantity = quantity;
    }

    public String getReturns() {
        return returns;
    }

    public void setReturns(String returns) {
        this.returns = returns;
    }

    public Sale() {
    }
}
