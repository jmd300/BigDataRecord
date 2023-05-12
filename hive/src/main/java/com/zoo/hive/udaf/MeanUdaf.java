package com.zoo.hive.udaf;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryStruct;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
/**
 * @Author: JMD
 * @Date: 5/11/2023
   回头再看，这里有些乱
   https://blog.csdn.net/qq_39664250/article/details/106870116
 * 计算平均值的自定义 UDAF 函数
 */

public class MeanUdaf extends AbstractGenericUDAFResolver {

    // 重写实现AbstractGenericUDAFResolver函数的执行器
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] info) throws SemanticException {
        // 验证参数的有效性
        if (null != info && info.length == 1) {
            // 正常情况
            // 判断是不是简单类型
            if (info[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
                throw new UDFArgumentException("该函数该函数只能接收接收简单类型的参数！");
            }
            // 判断是不是Long类型
            // bigint -> long
            // 类型转换
            PrimitiveTypeInfo pti = (PrimitiveTypeInfo) info[0];
            if (!pti.getPrimitiveCategory().equals(PrimitiveObjectInspector.PrimitiveCategory.LONG)) {
                throw new UDFArgumentException("该函数只能接收Long类型的参数");
            }
        } else {
            // 不正常情况
            throw new UDFArgumentException("该函数需要接收参数！并且只能传递一个参数！");
        }

        return new MyGenericUDAFEvaluator();

    }

    // 创建自己的执行器
    private static class MyGenericUDAFEvaluator extends GenericUDAFEvaluator {
        // 自定义我们自己的缓冲区类型 保存数据处理的临时结果
        private static class MyAggregationBuffer extends AbstractAggregationBuffer{
            // 定义缓冲区中存储什么
            // 保存sum 和count
            @Setter @Getter
            private Double sum = 0D;
            @Setter @Getter
            private Long count = 0L;
        }
        // 创建缓冲区对象
        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            printMode("getNewAggregationBuffer");
            return new MyAggregationBuffer();
        }
        // 初始化 参数校验 返回值设置
        // 一个阶段调用一次
        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            printMode("init");
            // 保留父类调用
            super.init(m, parameters);
            // 实现自己的
            // 根据不同的执行阶段返回不同的数据
            // 需求1：mapper阶段 包括map(PARTIAL1)和combiner(PARTIAL2) 需要返回sum+count->struct
            if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
                List<String> structFieldNames = new ArrayList<String>();
                List<ObjectInspector> structFieldObjectInspectors = new ArrayList<ObjectInspector>();
                // struct<sum:double,count:bigint>
                structFieldNames.add("sum");
                structFieldNames.add("count");
                structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
                structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
                // 返回
                return ObjectInspectorFactory.getStandardStructObjectInspector(structFieldNames, structFieldObjectInspectors);
            }else {
                // reduce阶段
                return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
            }

        }
        // AggregationBuffer是聚合函数缓冲区对象 贯穿于 聚合函数始终的一个数据传输对象
        // 擦写缓冲区 让缓冲区重复使用
        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            printMode("reset");
            ((MyAggregationBuffer)agg).setCount(0L);
            ((MyAggregationBuffer)agg).setSum(0D);
        }

        private Long p = 0L;
//		private Long history_count = 0L;
//		private Double history_sum = 0D;

        private Long current_count = 0L;
        private Double current_sum = 0D;
        // Mapper类的map函数用于处理输入数据即迭代局部数据的
        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            printMode("iterate");
            // parameters[0]为传入的参数，每次一个 为bigint类型
            // 先将其转为string类型，再转为Long
            p = Long.parseLong(String.valueOf(parameters[0]).trim());
            // map的循环 将数据放入缓冲区
            // 将agg转为自己的缓冲区
            MyAggregationBuffer ab = (MyAggregationBuffer) agg;
            // 从缓冲区中获取之前存储的数据
//			history_count = ab.getCount();
//			history_sum = ab.getSum();
            //进行本次循环操作
            current_sum += p;
            current_count++;
            // 保存本次数据
            ab.setCount(current_count);
            ab.setSum(current_sum);

        }

        // 定义一个结构进行数据的存储
        private Object[] mapout = {new DoubleWritable(),new LongWritable()};

        // map的最终结果输出方法  处理全部输出数据中的部分数据
        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            printMode("terminatePartial");
            // 获取map的最终输出
            MyAggregationBuffer ab = (MyAggregationBuffer) agg;
            ((DoubleWritable)mapout[0]).set(ab.getSum());
            ((LongWritable)mapout[1]).set(ab.getCount());
            // 直接返回mapout
            return mapout;

        }
        // 进行  map 局部结果的全局化处理 Combiner 和 Reducer的reduce方法
        // partial来自terminatePartial的返回值
        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            printMode("merge");
            // map结构通过网络到partial 要将partial转为结构
            if (partial instanceof LazyBinaryStruct) {
                // 强转参数
                LazyBinaryStruct lbs = (LazyBinaryStruct) partial;

                DoubleWritable sum = (DoubleWritable) lbs.getField(0);
                LongWritable count = (LongWritable) lbs.getField(1);

                // 将本次map输出的数据放到reducer的缓冲区
                MyAggregationBuffer ab = (MyAggregationBuffer) agg;
                ab.setCount(ab.getCount() + count.get());
                ab.setSum(ab.getSum() + sum.get());
            }

        }
        private Text reduceout = new Text();
        // Combiner 或 Reducer的最终输出方法
        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            printMode("terminate");
            // 获取reduce累加之后的最终结果
            MyAggregationBuffer ab = (MyAggregationBuffer) agg;

            Double avg = ab.getSum() / ab.getCount();
            DecimalFormat df = new DecimalFormat("###,###.00");
            reduceout.set(df.format(avg));
            return reduceout;
        }
        // 打印个阶段信息
        public void printMode(String mname){
            System.out.println("=================================== "+mname+" is Running! ================================");
        }
    }
}
