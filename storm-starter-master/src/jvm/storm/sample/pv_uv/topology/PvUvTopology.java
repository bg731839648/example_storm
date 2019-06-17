package storm.sample.pv_uv.topology;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import storm.sample.pv_uv.bolt.PvUvPartBolt;
import storm.sample.pv_uv.bolt.PvUvWholeBolt;
import storm.sample.pv_uv.spout.PvUvSpout;

/**
 * Storm Topology
 *
 * @author sunjiaxin
 * @date 2019-06-12 14:21
 */
public class PvUvTopology {

    /**
     * 主函数
     */
    public static void main(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new PvUvSpout(), 1);
        builder.setBolt("bolt", new PvUvPartBolt(), 1).fieldsGrouping("spout", new Fields("log"));
        builder.setBolt("sumBolt", new PvUvWholeBolt(), 1).shuffleGrouping("bolt");

        Config config = new Config();
        config.put(Config.TOPOLOGY_WORKERS, 1);

        try {
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("topology", config, builder.createTopology());

            Thread.sleep(100 * 1000);
            localCluster.shutdown();
        } catch (Exception e) {

            e.printStackTrace();
        }
    }
}
