package storm.lesson.hbase.state;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class TupleTableConfig implements Serializable {

    private static final long serialVersionUID = 3968737265730915161L;

    private String tableName;

    protected String tupleRowKeyField;

    protected String tupleTimeStampField;

    protected Map<String, Set<String>> columnFamilies;

    public TupleTableConfig(final String tableName, final String rowKeyField) {
        this.tableName = tableName;
        this.tupleRowKeyField = rowKeyField;
        this.tupleTimeStampField = "";
        this.columnFamilies = new HashMap<String, Set<String>>();
    }

    public TupleTableConfig(final String tableName, final String rowKeyField, final String timeStampField) {
        this.tableName = tableName;
        this.tupleRowKeyField = rowKeyField;
        this.tupleTimeStampField = timeStampField;
        this.columnFamilies = new HashMap<String, Set<String>>();
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getTupleRowKeyField() {
        return tupleRowKeyField;
    }

    public void setTupleRowKeyField(String tupleRowKeyField) {
        this.tupleRowKeyField = tupleRowKeyField;
    }

    public String getTupleTimeStampField() {
        return tupleTimeStampField;
    }

    public void setTupleTimeStampField(String tupleTimeStampField) {
        this.tupleTimeStampField = tupleTimeStampField;
    }

    public Map<String, Set<String>> getColumFamilies() {
        return columnFamilies;
    }

    public void setColumFamilies(Map<String, Set<String>> columnFamilies) {
        this.columnFamilies = columnFamilies;
    }
}
