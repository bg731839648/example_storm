package storm.lesson.local.blot;


import com.wangyin.r2m.client.util.SafeEncoder;
import com.wangyin.rediscluster.client.CacheClusterClient;
import com.wangyin.rediscluster.exeption.SerializationException;
import com.wangyin.rediscluster.experimental.data.R2mHashMap;
import com.wangyin.rediscluster.experimental.storm.R2mClientBuilder;
import com.wangyin.rediscluster.experimental.storm.R2mClusterInfo;
import com.wangyin.rediscluster.serializer.Serializer;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class LocalR2MBolt implements IRichBolt {

    OutputCollector collector;

    final R2mClusterInfo r2mClusterInfo;

    CacheClusterClient cacheClusterClient;

    Integer id;

    String name;

    Map<String, Integer> counters;


    public LocalR2MBolt(R2mClusterInfo r2mClusterInfo) {
        this.r2mClusterInfo = r2mClusterInfo;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;

        this.name = topologyContext.getThisComponentId();
        this.id = topologyContext.getThisTaskId();
        this.cacheClusterClient = R2mClientBuilder.build(r2mClusterInfo);

        this.counters = new R2mHashMap<String, Integer>(cacheClusterClient, "test", new Serializer() {
            @Override
            public byte[] serialize(Object var1) throws SerializationException {
                return var1 == null ? null : SafeEncoder.encode((var1 + ""));
            }

            @Override
            public Object deserialize(byte[] var1) throws SerializationException {
                return var1 == null ? null : Integer.valueOf(new String(var1));
            }
        });
    }

    @Override
    public void execute(Tuple tuple) {
        int count = tuple.getInteger(0);
        String sessionId = tuple.getString(1);

        if (StringUtils.isNotBlank(sessionId)) {
            /**
             * If the word dosn't exist in the map we will create
             * this, if not We will add 1
             */
            if (!counters.containsKey(sessionId)) {
                counters.put(sessionId, count);
                System.out.println("R2M->put sessionId: " + sessionId + ",count:" + count);
            } else {
                Integer integer = counters.get(sessionId);
                System.out.println("R2M->get sessionId: " + sessionId + ",count:" + integer);
                Integer remove = counters.remove(sessionId);
                System.out.println("R2M->del sessionId: " + sessionId + ",result:" + remove);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    @Override
    public void cleanup() {
        System.out.println("-- Word Counter [" + name + "-" + id + "] --");
        for (Map.Entry<String, Integer> entry : counters.entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
