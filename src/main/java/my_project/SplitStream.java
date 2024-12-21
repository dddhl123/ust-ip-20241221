package my_project;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Arrays;

public class SplitStream extends ProcessFunction<String, InnerMessage> {

    final static OutputTag<InnerMessage> customerTag = new OutputTag<InnerMessage>("customer") {
    };

    final static OutputTag<InnerMessage> nationTag = new OutputTag<InnerMessage>("nation") {
    };
    final static OutputTag<InnerMessage> nation2Tag = new OutputTag<InnerMessage>("nation2") {
    };

    final static OutputTag<InnerMessage> lineitemTag = new OutputTag<InnerMessage>("lineitem") {
    };

    final static OutputTag<InnerMessage> ordersTag = new OutputTag<InnerMessage>("orders") {
    };

    final static OutputTag<InnerMessage> supplierTag = new OutputTag<InnerMessage>("supplier") {
    };

    /// Split the input stream into different streams according to the tag
    /// Note: nation and nation2 are the same table, but we need to split them into different streams
    @Override
    public void processElement(String value, Context ctx, Collector<InnerMessage> out) throws Exception {
        String[] values = value.split("\\|");
        if (values.length == 0) {
            throw new Exception("Invalid row: " + value);
        }
        String operation = values[0];
        String tag = values[1];
        InnerMessage container = new InnerMessage(Arrays.copyOfRange(values, 1, values.length));
        if(operation.equals("INSERT")) {
            container.setType(InnerMessage.OperationType.INSERT);
        } else if(operation.equals("DELETE")) {
            container.setType(InnerMessage.OperationType.DELETE);
        } else {
            throw new Exception("Invalid operation: " + operation);
        }
        switch (tag) {
            case "customer":
                ctx.output(customerTag, container);
                break;
            case "nation":
                ctx.output(nationTag, container);
                values[1] = "nation2";
                InnerMessage container2 = new InnerMessage(Arrays.copyOfRange(values, 1, values.length));
                container2.setType(container.type);
                ctx.output(nation2Tag, container2);
                break;
            case "lineitem":
                ctx.output(lineitemTag, container);
                break;
            case "orders":
                ctx.output(ordersTag, container);
                break;
            case "supplier":
                ctx.output(supplierTag, container);
                break;
            default:
                throw new Exception("Invalid tag: " + tag);
        }
    }
}