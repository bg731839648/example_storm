package storm.lesson.trident.topology;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.FilterNull;
import org.apache.storm.trident.operation.builtin.FirstN;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import storm.lesson.trident.functions.MySplit;
import storm.starter.trident.TridentWordCount;


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

        TridentTopology topology = new TridentTopology();
        TridentState wordCounts = topology
                .newStream("spout1", spout)
                .each(
                        new Fields("eachLog"),
                        new MySplit(" "),
                        new Fields("date", "sessionId"))
                .groupBy(
                        new Fields("date"))
                .persistentAggregate(
                        new MemoryMapState.Factory(),
                        new Fields("sessionId"),
                        new Count(),
                        new Fields("pv"));

        topology.newDRPCStream("GetPv", drpc)
                .each(
                        new Fields("args"),
                        new TridentWordCount.Split(),
                        new Fields("date"))
                .groupBy(
                        new Fields("date"))
                .stateQuery(wordCounts,
                        new Fields("date"),
                        new MapGet(),
                        new Fields("pv"))
                .each(
                        new Fields("pv"),
                        new FilterNull())
                .applyAssembly(new FirstN(2, "pv", true));

        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        conf.setMaxSpoutPending(20);
        if (args.length == 0) {
            LocalDRPC drpc = new LocalDRPC();
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("wordCounter", conf, buildTopology(drpc));
            for (int i = 0; i < 100; i++) {
                System.err.println(drpc.execute("GetPv", "2019-06-03 2019-06-02 2019-06-04"));
                Thread.sleep(1000);
            }
        } else {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, buildTopology(null));
        }
    }

}
