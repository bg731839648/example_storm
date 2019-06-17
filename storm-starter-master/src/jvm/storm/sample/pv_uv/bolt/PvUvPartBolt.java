package storm.sample.pv_uv.bolt;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import storm.common.constant.DateConstant;
import storm.common.constant.StringConstant;
import storm.common.util.DateUtils;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Storm Bolt 分段收集
 *
 * @author sunjiaxin
 * @date 2019-06-12 14:21
 */
public class PvUvPartBolt implements IRichBolt {

    /**
     * 序列化
     */
    private static final long serialVersionUID = 5533689750126813026L;

    /**
     * 发射器
     */
    private OutputCollector collector = null;

    /**
     * 请求次数map
     */
    private ConcurrentHashMap<String, Long>  requestCountMap = new ConcurrentHashMap<String, Long>();

    @Override
    public void prepare(Map map,
                        TopologyContext topologyContext,
                        OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {

        try {

            String request = tuple.getString(0);
            String[] requestArray;
            String sessionId;
            String date;

            if (request != null && !StringConstant.EMPTY.equals(request)) {
                requestArray = request.split(StringConstant.SPACE);
                if (requestArray.length > 2){
                    sessionId = requestArray[1];
                    date = requestArray[2];

                    if (StringUtils.isNotBlank(sessionId) && StringUtils.isNotBlank(date)) {
                        // 判断数据是时间是否小于等于当前时间 满足条件均视为当日PV UV
                        if (DateUtils.compareStrDate(date, DateUtils.formatDate(new Date(), DateConstant.YMD), DateConstant.YMD, DateUtils.BE_EQUAL_OR_LESS_THAN_TO)) {
                            Long count = requestCountMap.get(sessionId);
                            if (count == null) {
                                count = 0L;
                            }
                            count++;

                            requestCountMap.put(sessionId, count);
                            collector.emit(new Values(sessionId, count));
                            collector.ack(tuple);
                        }
                    }
                }
            }
        } catch (Exception e) {

            collector.fail(tuple);
            e.printStackTrace();
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sessionId", "count"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
