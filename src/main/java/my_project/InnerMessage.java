package my_project;


import java.util.HashMap;
import java.util.Objects;

public class InnerMessage {
    public OperationType type;
    public String key_value;
    public HashMap<String, String> fields;

    public InnerMessage(String[] raw_fields) {
        this.type = OperationType.INSERT;
        String table_name = raw_fields[0];

        // initialize fields with fixed capacity, reduce hashmap resizing
        this.fields = new HashMap<>(70);

        switch (table_name) {
            case "nation": {
                this.fields.put("n_nationkey", raw_fields[1]);
                this.fields.put("n_name", raw_fields[2]);
                this.fields.put("n_regionkey", raw_fields[3]);
                this.fields.put("n_comment", raw_fields[4]);
                this.key_value = this.fields.get("n_nationkey");
                break;
            }
            case "nation2": {
                this.fields.put("n2_nationkey", raw_fields[1]);
                this.fields.put("n2_name", raw_fields[2]);
                this.fields.put("n2_regionkey", raw_fields[3]);
                this.fields.put("n2_comment", raw_fields[4]);
                this.key_value = this.fields.get("n2_nationkey");
                break;
            }
            case "supplier": {
                this.fields.put("s_suppkey", raw_fields[1]);
                this.fields.put("s_name", raw_fields[2]);
                this.fields.put("s_address", raw_fields[3]);
                this.fields.put("s_nationkey", raw_fields[4]);
                this.fields.put("s_phone", raw_fields[5]);
                this.fields.put("s_acctbal", raw_fields[6]);
                this.fields.put("s_comment", raw_fields[7]);
                this.key_value = this.fields.get("s_nationkey");
                break;
            }
            case "customer": {
                this.fields.put("c_custkey", raw_fields[1]);
                this.fields.put("c_name", raw_fields[2]);
                this.fields.put("c_address", raw_fields[3]);
                this.fields.put("c_nationkey", raw_fields[4]);
                this.fields.put("c_phone", raw_fields[5]);
                this.fields.put("c_acctbal", raw_fields[6]);
                this.fields.put("c_mktsegment", raw_fields[7]);
                this.fields.put("c_comment", raw_fields[8]);
                this.key_value = this.fields.get("c_nationkey");
                break;
            }
            case "orders": {
                this.fields.put("o_orderkey", raw_fields[1]);
                this.fields.put("o_custkey", raw_fields[2]);
                this.fields.put("o_orderstatus", raw_fields[3]);
                this.fields.put("o_totalprice", raw_fields[4]);
                this.fields.put("o_orderdate", raw_fields[5]);
                this.fields.put("o_orderpriority", raw_fields[6]);
                this.fields.put("o_clerk", raw_fields[7]);
                this.fields.put("o_shippriority", raw_fields[8]);
                this.fields.put("o_comment", raw_fields[9]);
                this.key_value = this.fields.get("o_custkey");
                break;
            }
            case "lineitem": {
                this.fields.put("l_orderkey", raw_fields[1]);
                this.fields.put("l_partkey", raw_fields[2]);
                this.fields.put("l_suppkey", raw_fields[3]);
                this.fields.put("l_linenumber", raw_fields[4]);
                this.fields.put("l_quantity", raw_fields[5]);
                this.fields.put("l_extendedprice", raw_fields[6]);
                this.fields.put("l_discount", raw_fields[7]);
                this.fields.put("l_tax", raw_fields[8]);
                this.fields.put("l_returnflag", raw_fields[9]);
                this.fields.put("l_linestatus", raw_fields[10]);
                this.fields.put("l_shipdate", raw_fields[11]);
                this.fields.put("l_commitdate", raw_fields[12]);
                this.fields.put("l_receiptdate", raw_fields[13]);
                this.fields.put("l_shipinstruct", raw_fields[14]);
                this.fields.put("l_shipmode", raw_fields[15]);
                this.fields.put("l_comment", raw_fields[16]);
                this.key_value = this.fields.get("l_suppkey");
                break;
            }
        }
    }

    public InnerMessage(InnerMessage self, InnerMessage other) {
        this.type = self.type;
        this.key_value = self.key_value;
        this.fields = new HashMap<>(self.fields);
        this.fields.putAll(other.fields);
    }

    public void setType(OperationType type) {
        assert (type != this.type) : "Logic error: setting the same type";
        this.type = type;
    }

    /// Find the value in fields and set it as the key
    public void setKeyByKeyName(String keyName) {
        String value = this.fields.get(keyName);
        assert value != null : "Logic error: key not found in fields";
        this.key_value = value;
    }

    public void setGroupKeyByKeyNames(String keyNames[]) {
        String mixedValue = "";
        for (String keyName : keyNames) {
            String value = this.fields.get(keyName);
            mixedValue += value;
            assert value != null : "Logic error: key not found in fields";
        }
        this.key_value = mixedValue;
    }

    // get the value of the key, null if not found
    public String getValueByKey(String key) {
        return this.fields.get(key);
    }

    public enum OperationType {
        // input operations:
        INSERT,
        DELETE,
        // update operations:
        SET_ALIVE,
        SET_DEAD,
        SET_ALIVE_LEFT,
        SET_ALIVE_RIGHT,
        SET_DEAD_LEFT,
        SET_DEAD_RIGHT,
        AGGREGATE,
        AGGREGATE_DELETE
    }

    @Override
    public String toString() {
        String result = "InnerMessage{" +
                "type=" + type +
                ", key_value='" + key_value + '\'' +
                ", fields=" + fields.toString() + "}";
        return result;

    }

    // overide equals and hashcode to use InnerMessage as key in HashMap
    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof InnerMessage)) {
            return false;
        }
        InnerMessage other = (InnerMessage) obj;
        return this.fields.equals(other.fields);
    }


    @Override
    public int hashCode() {
        return Objects.hash(fields);
    }

}
