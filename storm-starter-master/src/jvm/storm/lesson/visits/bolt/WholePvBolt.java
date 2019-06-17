package storm.lesson.visits.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class WholePvBolt implements IRichBolt {

    private Map<Long, Long> counts = new HashMap<Long, Long>();

    private static final long serialVersionUID = 5533689750126813026L;

    private OutputCollector outputCollector;

    private Long threadId = null;

    private Long pv = 0L;

    @Override
    public void prepare(Map map,
                        TopologyContext topologyContext,
                        OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {

        long wordSum = 0;

        try {
            threadId = tuple.getLong(0);
            pv = tuple.getLong(1);

            counts.put(threadId, pv);

            // 获取总数,遍历counts 的values. 进行sum
            Iterator<Long> iteratorI = counts.values().iterator();
            while (iteratorI.hasNext()) {
                wordSum += iteratorI.next();
            }

            System.out.println(Thread.currentThread().getName() + " pv_all= " + wordSum);

            outputCollector.ack(tuple);

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
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
