package com.zoo.flink.java.window;

import com.zoo.flink.java.util.FlinkEnv;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @Author: JMD
 * @Date: 5/15/2023
 */
public class WindowsDemo extends FlinkEnv {
    public static void main(String[] args) throws Exception {
        // 1.滚动窗口
        // of()还有一个重载方法，可以传入两个 Time 类型的参数： size 和 offset。第一个参数当然还是窗口大小，第二个参数则表示窗口起始点的偏移量。
        arrayStream.keyBy(e -> e.user)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(null);

        // 2.滑动窗口
        // 滑动窗口同样可以追加第三个参数，用于指定窗口起始点的偏移量，用法与滚动窗口完全一致。
        arrayStream.keyBy(e -> e.user)
        .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)));

        // 3. 会话窗口
        arrayStream.keyBy(e -> e.user)
        .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)));

        /*
         * .window(ProcessingTimeSessionWindows.withDynamicGap(new
         * SessionWindowTimeGapExtractor<Tuple2<String, Long>>() {
         * @Override
         * public long extract(Tuple2<String, Long> element) {
         * // 提取 session gap 值返回, 单位毫秒
         * return element.f0.length() * 1000;
         * }
         * }))
         * 这里.withDynamicGap()方法需要传入一个 SessionWindowTimeGapExtractor 作为参数，用
         * 来定义 session gap 的动态提取逻辑。在这里，我们提取了数据元素的第一个字段，用它的长
         * 度乘以 1000 作为会话超时的间隔。
         */

        env.execute();
    }
}
