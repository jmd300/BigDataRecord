package com.zoo.hbase.join;

import lombok.Data;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


@Data
public class TableBean implements Writable {
    private String id; //订单 id
    private String pid; //产品 id
    private int amount; //产品数量
    private String productName; //产品名称
    private String flag; //判断是 order 表还是 pd 表的标志字段

    @Override
    public String toString() {
        return id + "\t" + productName + "\t" + amount;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(id);
        out.writeUTF(pid);
        out.writeInt(amount);
        out.writeUTF(productName);
        out.writeUTF(flag);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readUTF();
        this.pid = in.readUTF();
        this.amount = in.readInt();
        this.productName = in.readUTF();
        this.flag = in.readUTF();
    }
}
