package storm.sample.pv_uv.spout;


import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;

/**
 * Storm Spout 数据收集
 *
 * @author sunjiaxin
 * @date 2019-06-12 14:21
 */
public class PvUvSpout implements IRichSpout {

    /**
     * 序列化
     */
    private static final long serialVersionUID = -8551511411505633240L;

    /**
     * 网址访问域名
     */
    private static final String HOST = "www.jd.com ";

    /**
     * 初始数据
     */
    private static final String[] SENTENCE_LIST = new String[]{
            "p 2019-06-11 8:45:12",
            "o 2019-06-11 9:45:12",
            "n 2019-06-11 10:45:12",
            "m 2019-06-11 11:45:12",
            "l 2019-06-12 12:45:12",
            "k 2019-06-12 13:45:12",
            "j 2019-06-12 14:45:12",
            "i 2019-06-12 15:45:12",
            "h 2019-06-13 16:45:12",
            "g 2019-06-13 17:45:12",
            "f 2019-06-13 18:45:12",
            "e 2019-06-13 19:45:12",
            "d 2019-06-14 20:45:12",
            "c 2019-06-14 21:45:12",
            "b 2019-06-14 22:45:12",
            "a 2019-06-14 23:45:12"};

    /**
     * 发射器
     */
    private SpoutOutputCollector collector;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector collector) {
        // 初始化数据
        this.collector = collector;
    }

    @Override
    public void nextTuple() {

        // 循环获取tuple
        try {
            StringBuilder sb;
            for (int j = 0; j < SENTENCE_LIST.length; j++) {
                sb = new StringBuilder(HOST);
                sb.append(SENTENCE_LIST[j]);
                collector.emit(new Values(sb.toString()));
            }

            Utils.sleep(1000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // 定义出参格式
        outputFieldsDeclarer.declare(new Fields("log"));
    }

    @Override
    public void ack(Object o) {

    }

    @Override
    public void fail(Object o) {

    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void close() {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
