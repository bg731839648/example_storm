package storm.sample.pv_uv.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Storm Bolt Kafka收集
 *
 * @author sunjiaxin
 * @date 2019-06-12 14:21
 */
public class KafkaPvUvPartBolt extends BaseRichBolt {

    /**
     * 序列化
     */
    private static final long serialVersionUID = -7583638897534292482L;

    /**
     * 发射器
     */
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        int tupleSize = tuple.size();
        String str;
        if (tupleSize > 0) {
            str = tuple.getString(tupleSize - 1);
            collector.emit(new Values(str));
        }
        collector.ack(tuple);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("log"));
    }

}