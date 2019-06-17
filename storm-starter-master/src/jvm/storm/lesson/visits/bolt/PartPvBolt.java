package storm.lesson.visits.bolt;


import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class PartPvBolt implements IRichBolt {

    private static final long serialVersionUID = 5533689750126813026L;

    private OutputCollector outputCollector = null;

    private String logString = null;

    private String sessionId = null;

    private long pv = 0L;

    @Override
    public void prepare(Map map,
                        TopologyContext topologyContext,
                        OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {

        try {
            logString = tuple.getString(0);
            sessionId = logString.split(" ")[1];

//            synchronized (this) {
                if (sessionId != null) {
                    pv++;
                }
//            }

            outputCollector.ack(tuple);
            outputCollector.emit(new Values(Thread.currentThread().getId(), pv));

            System.err.println("threadId=" + Thread.currentThread().getId()
                    + " sessionId=" + sessionId
                    + " pv=" + pv);

        } catch (Exception e) {
            outputCollector.fail(tuple);
            e.printStackTrace();
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("threadId", "pv"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
