package storm.lesson.local.spout;




import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Map;

public class LocalSpout implements IRichSpout {

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

    @Override
    public void nextTuple() {
        // 循环获取tuple
        try {
            Thread.sleep(1500);
            while ((str = this.br.readLine()) != null) {
                spoutOutputCollector.emit(new Values(str));
            }
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
