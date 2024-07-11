package com.zoo.hbase.TopN;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author: JMD
 * @Date: 5/6/2023
 */
@Getter
@Setter
@AllArgsConstructor
public class People implements WritableComparable<People> {
    private String id;
    private String name;
    private Double weight;

    @Override
    // 自定义比较操作
    public int compareTo(People people) {
        return this.weight.compareTo(people.getWeight());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(id);
        out.writeUTF(name);
        out.writeDouble(weight);
    }

    @Override
    // 和写的顺序一致
    public void readFields(DataInput dataInput) throws IOException {
        id = dataInput.readUTF();
        name = dataInput.readUTF();
        weight = dataInput.readDouble();
    }

    People(){

    }

    @Override
    public String toString() {
        return "People{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", weight=" + weight +
                '}';
    }
}
