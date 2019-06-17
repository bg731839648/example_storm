package storm.lesson.wordCount.bolt;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.FailedException;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class MySplitBolt implements IBasicBolt {

    private static final long serialVersionUID = 8734916846002431038L;

    String patton;

    public MySplitBolt(String patton) {
        this.patton = patton;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext) {

    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        try {
            String string = tuple.getString(0);
            if (string != null) {
                for (String str : string.split(patton)) {
                    basicOutputCollector.emit(new Values(str));
                }
            }
        } catch (FailedException e) {
            throw new FailedException("split fail!");
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
