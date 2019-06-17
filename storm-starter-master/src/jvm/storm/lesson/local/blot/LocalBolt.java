package storm.lesson.local.blot;


import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class LocalBolt implements IRichBolt {

    private int num = 0;
    private String log = "";
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {

        try {
            log = tuple.getStringByField("log");
            if (log != null && log.length() > 0) {
                num++;
                System.out.println(Thread.currentThread().getName() + " lines :" + num + " sessionId: " + log.split(" ")[1]);

                collector.emit(new Values(num, log.split(" ")[1]));
            }
            collector.ack(tuple);
        } catch (Exception e) {

            collector.fail(tuple);
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // 有下级bolt定义出参格式,无下级bolt则无需定义
        outputFieldsDeclarer.declare(new Fields("lines", "sessionId"));
    }

    @Override
    public void cleanup() {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
