package storm.lesson.hbase.topology;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import storm.lesson.hbase.state.HBaseAggregateState;
import storm.lesson.hbase.state.TridentConfig;
import storm.lesson.trident.functions.MySplit;


public class TridentPvTopology {

    private static StormTopology buildTopology(LocalDRPC drpc) {

        FixedBatchSpout spout = new FixedBatchSpout(
                new Fields("eachLog"), 3,
                new Values("www.jd.com 11111111111111111 2019-06-03 8:45:12"),
                new Values("www.jd.com 22222222222222222 2019-06-03 9:45:12"),
                new Values("www.jd.com 33333333333333333 2019-06-03 10:45:12"),
                new Values("www.jd.com 44444444444444444 2019-06-04 11:45:12"),
                new Values("www.jd.com 55555555555555555 2019-06-04 12:45:12"),
                new Values("www.jd.com 66666666666666666 2019-06-04 13:45:12"),
                new Values("www.jd.com 77777777777777777 2019-06-02 14:45:12"),
                new Values("www.jd.com 88888888888888888 2019-06-02 15:45:12"),
                new Values("www.jd.com 99999999999999999 2019-06-04 15:45:12"));
        spout.setCycle(true);

        TridentConfig config = new TridentConfig("hbase_table", "rowKey");
        StateFactory state = HBaseAggregateState.transactional(config);

        TridentTopology topology = new TridentTopology();
        topology
                .newStream("spout1", spout)
                .each(
                        new Fields("eachLog"),
                        new MySplit(" "),
                        new Fields("date", "cf", "pvCount", "sessionId"))
                .project(new Fields("date", "cf", "pvCount"))
                .groupBy(
                        new Fields("date", "cf", "pvCount"))
                .persistentAggregate(
                        state,
                        new Count(),
                        new Fields("pv"));

        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        conf.setMaxSpoutPending(20);
        if (args.length == 0) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("wordCounter", conf, buildTopology(null));
        } else {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, buildTopology(null));
        }
    }

}
