package storm.lesson.hbase.state;




import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.state.StateType;

import org.apache.storm.trident.state.map.*;
import org.apache.storm.tuple.Values;

import java.util.Map;

@SuppressWarnings({"rawtypes", "unchecked"})
public class HBaseAggregateFactory implements StateFactory {

    private static final long serialVersionUID = 6984065961776498420L;

    private StateType type;

    private TridentConfig config;

    @Override
    public State makeState(Map map, IMetricsContext iMetricsContext, int i, int i1) {
        HBaseAggregateState state = new HBaseAggregateState(config);
        CachedMap cachedMap = new CachedMap(state, config.getStateCacheSize());

        MapState ms;
        if (type == StateType.NON_TRANSACTIONAL) {
            ms = NonTransactionalMap.build(cachedMap);
        } else if (type == StateType.OPAQUE) {
            ms = OpaqueMap.build(cachedMap);
        } else if (type == StateType.TRANSACTIONAL) {
            ms = TransactionalMap.build(cachedMap);
        }else {
            ms = null;
        }
        return new SnapshottableMap(ms,new Values("$GLOBAL$"));
    }

    public HBaseAggregateFactory(TridentConfig config, StateType type) {
        this.config = config;
        this.type = type;

        if (config.getStateSerializer() == null) {
            config.setStateSerializer(TridentConfig.DEFAULT_SERIALIZES.get(type));
        }
    }
}
