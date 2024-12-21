package my_project;

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public abstract class LeafProcessFunction extends KeyedProcessFunction<String, InnerMessage, InnerMessage> {
    private String nextKey;

    public LeafProcessFunction(String nextKey) {
        super();
        this.nextKey = nextKey;
    }

    @Override
    public void processElement(InnerMessage data, KeyedProcessFunction<String, InnerMessage, InnerMessage>.Context context, Collector<InnerMessage> collector) throws Exception {
        switch (data.type) {
            case INSERT: {
                // All tuples in the leaf relation are alive
                data.setType(InnerMessage.OperationType.SET_ALIVE);
                data.setKeyByKeyName(nextKey);
                collector.collect(data);
                break;
            }
            case DELETE: {
                data.setType(InnerMessage.OperationType.SET_DEAD);
                data.setKeyByKeyName(nextKey);
                collector.collect(data);
                break;
            }
            default:
                throw new Exception("Invalid operation type: " + data.type);
        }
    }

}

