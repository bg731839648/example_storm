package storm.lesson.visits.spout;


import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Map;

public class ZkSpout implements IRichSpout {

    // 读取文件
    FileInputStream fis;
    InputStreamReader isr;
    BufferedReader br;

    // 读取数据
    String str;

    private SpoutOutputCollector spoutOutputCollector;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        // 初始化数据
        this.spoutOutputCollector = spoutOutputCollector;

        try {
            this.fis = new FileInputStream("D:\\track.log");
            this.isr = new InputStreamReader(fis, "utf-8");
            this.br = new BufferedReader(isr);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    String[] sentences = new String[]{
            "www.jd.com ap-2asdasd5446sad564asd 2019-05-30 8:45:12",
            "www.jd.com ao-2asdasd5446sad564asd 2019-05-30 9:45:12",
            "www.jd.com an-2asdasd5446sad564asd 2019-05-30 10:45:12",
            "www.jd.com am-2asdasd5446sad564asd 2019-05-30 11:45:12",
            "www.jd.com al-2asdasd5446sad564asd 2019-05-30 12:45:12",
            "www.jd.com ak-2asdasd5446sad564asd 2019-05-30 13:45:12",
            "www.jd.com aj-2asdasd5446sad564asd 2019-05-30 14:45:12",
            "www.jd.com ai-2asdasd5446sad564asd 2019-05-30 15:45:12",
            "www.jd.com ah-2asdasd5446sad564asd 2019-05-30 16:45:12",
            "www.jd.com ag-2asdasd5446sad564asd 2019-05-30 17:45:12",
            "www.jd.com af-2asdasd5446sad564asd 2019-05-30 18:45:12",
            "www.jd.com ae-2asdasd5446sad564asd 2019-05-30 19:45:12",
            "www.jd.com ad-2asdasd5446sad564asd 2019-05-30 20:45:12",
            "www.jd.com ac-2asdasd5446sad564asd 2019-05-30 21:45:12",
            "www.jd.com ab-2asdasd5446sad564asd 2019-05-30 22:45:12",
            "www.jd.com aa-2asdasd5446sad564asd 2019-05-30 23:45:12"};

    @Override
    public void nextTuple() {
        // 循环获取tuple
        try {

            for (int i = 0; i < 10000; i++) {
                for (String sentence : sentences) {
                    spoutOutputCollector.emit(new Values(sentence));
                }
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
        System.err.println("spout ack: " + o.toString());
    }

    @Override
    public void fail(Object o) {
        System.err.println("spout fail: " + o.toString());

    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void close() {
        // 资源释放
        try {
            br.close();
            isr.close();
            fis.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
