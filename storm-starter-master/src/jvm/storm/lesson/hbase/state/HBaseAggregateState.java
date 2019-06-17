package storm.lesson.hbase.state;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.trident.state.OpaqueValue;
import org.apache.storm.trident.state.Serializer;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.state.StateType;
import org.apache.storm.trident.state.map.IBackingMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings({"rawtypes", "unchecked"})
public class HBaseAggregateState<T> implements IBackingMap<T> {

    private HTableConnector connector;

    private Serializer serializer;

    public HBaseAggregateState(TridentConfig config) {
        this.serializer = config.getStateSerializer();
        try {
            this.connector = new HTableConnector(config);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static StateFactory opaque(TridentConfig<OpaqueValue> config) {
        return new HBaseAggregateFactory(config, StateType.OPAQUE);
    }

    public static StateFactory transactional(TridentConfig<OpaqueValue> config) {
        return new HBaseAggregateFactory(config, StateType.TRANSACTIONAL);
    }

    public static StateFactory nonTransactional(TridentConfig<OpaqueValue> config) {
        return new HBaseAggregateFactory(config, StateType.NON_TRANSACTIONAL);
    }

    @Override
    public List<T> multiGet(List<List<Object>> list) {

        List<Get> gets = new ArrayList<Get>(list.size());
        byte[] rk;
        byte[] cf;
        byte[] cq;

        for (List<Object> k : list) {
            rk = Bytes.toBytes((String) k.get(0));
            cf = Bytes.toBytes((String) k.get(1));
            cq = Bytes.toBytes((String) k.get(2));

            Get get = new Get(rk);
            gets.add(get.addColumn(cf, cq));
        }

        Result[] results = null;
        try {
            results = connector.getTable().get(gets);
        } catch (IOException e) {
            e.printStackTrace();
        }

        List<T> rtn = new ArrayList<T>(list.size());
        for (int i = 0; i < list.size(); i++) {
            cf = Bytes.toBytes((String) list.get(i).get(1));
            cq = Bytes.toBytes((String) list.get(i).get(2));

            Result result = results[i];
            if (result.isEmpty()) {
                rtn.add(null);
            } else {
                rtn.add((T) serializer.deserialize(result.getValue(cf, cq)));
            }
        }

        return rtn;
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<T> vals) {

        List<Put> puts = new ArrayList<Put>();

        byte[] rk;
        byte[] cf;
        byte[] cq;

        for (int i = 0; i < keys.size(); i++) {
            rk = Bytes.toBytes((String) keys.get(i).get(0));
            cf = Bytes.toBytes((String) keys.get(i).get(1));
            cq = Bytes.toBytes((String) keys.get(i).get(2));

            byte[] cv = serializer.serialize(vals.get(i));
            Put p = new Put(rk);
            puts.add(p.add(cf, cq, cv));
        }

        try {
            connector.getTable().put(puts);
            connector.getTable().flushCommits();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
