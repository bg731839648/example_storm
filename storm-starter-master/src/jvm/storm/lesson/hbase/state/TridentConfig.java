package storm.lesson.hbase.state;


import org.apache.storm.trident.state.*;

import java.util.HashMap;
import java.util.Map;

public class TridentConfig<T> extends TupleTableConfig {

    private static final long serialVersionUID = -5301502757775629012L;

    private int stateCacheSize = 1000;

    private Serializer stateSerializer;

    public int getStateCacheSize() {
        return stateCacheSize;
    }

    public void setStateCacheSize(int stateCacheSize) {
        this.stateCacheSize = stateCacheSize;
    }

    public Serializer getStateSerializer() {
        return stateSerializer;
    }

    public void setStateSerializer(Serializer stateSerializer) {
        this.stateSerializer = stateSerializer;
    }

    public static final Map<StateType, Serializer> DEFAULT_SERIALIZES = new HashMap<StateType, Serializer>() {
        {
            put(StateType.NON_TRANSACTIONAL, new JSONNonTransactionalSerializer());
            put(StateType.TRANSACTIONAL, new JSONTransactionalSerializer());
            put(StateType.OPAQUE, new JSONOpaqueSerializer());
        }
    };

    public TridentConfig(String tableName, String rowKey) {
        super(tableName, rowKey);
    }

    public TridentConfig(String tableName, String rowKey, String timeStampField) {
        super(tableName, rowKey, timeStampField);
    }

}
