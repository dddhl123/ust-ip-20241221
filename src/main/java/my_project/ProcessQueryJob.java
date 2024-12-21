package my_project;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class ProcessQueryJob {
	private static final String mockDataFile = "./data/simple_test/test2.tbl";
	private static final int interval_ms = 0;


	public static void main(String[] args) throws Exception {
		// Set up the Execution Environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		// Create a File Source
		DataStream<String> dataStream = env.addSource(new MockStream(mockDataFile, interval_ms)).
				name("Mock Data File Source").setParallelism(1);
		// Split the stream according to the tag
		SingleOutputStreamOperator<InnerMessage> mainDataStream = dataStream.process(new SplitStream()).setParallelism(1);
		DataStream<InnerMessage> customerStream = mainDataStream.getSideOutput(SplitStream.customerTag);
		DataStream<InnerMessage> nationStream = mainDataStream.getSideOutput(SplitStream.nationTag);
		DataStream<InnerMessage> nation2Stream = mainDataStream.getSideOutput(SplitStream.nation2Tag);
		DataStream<InnerMessage> lineitemStream = mainDataStream.getSideOutput(SplitStream.lineitemTag);
		DataStream<InnerMessage> ordersStream = mainDataStream.getSideOutput(SplitStream.ordersTag);
		DataStream<InnerMessage> supplierStream = mainDataStream.getSideOutput(SplitStream.supplierTag);
		// Process the stream
		DataStream<InnerMessage> nationProcessedStream = nationStream.
				keyBy((container) -> container.key_value).
				process(new NationProcessFunction());
		DataStream<InnerMessage> supplierProcessedStream = nationProcessedStream.
				connect(supplierStream).
				keyBy((container) -> container.key_value, (container) -> container.key_value).
				process(new SupplierProcessFunction());
		DataStream<InnerMessage> lineitemProcessedStream = supplierProcessedStream.
				connect(lineitemStream).
				keyBy((container) -> container.key_value, (container) -> container.key_value).
				process(new LineitemProcessFunction());
//		lineitemProcessedStream.print();

		DataStream<InnerMessage> nation2ProcessedStream = nation2Stream.
				keyBy((container) -> container.key_value).
				process(new Nation2ProcessFunction());

		DataStream<InnerMessage> customerProcessedStream = nation2ProcessedStream.
				connect(customerStream).
				keyBy((container) -> container.key_value, (container) -> container.key_value).
				process(new CustomerProcessFunction());

		DataStream<InnerMessage> ordersProcessedStream = customerProcessedStream.
				connect(ordersStream).
				keyBy((container) -> container.key_value, (container) -> container.key_value).
				process(new OrdersProcessFunction());
//		ordersProcessedStream.print();

		DataStream<InnerMessage> mixedProcessStream = ordersProcessedStream.
				connect(lineitemProcessedStream).
				keyBy((container) -> container.key_value, (container) -> container.key_value).
				process(new MixedProcessFunction());
//		mixedProcessStream.print();

		DataStream<String> aggregateStream = mixedProcessStream.
				keyBy((container) -> container.key_value).
				process(new AggregateProcessFunction());

		aggregateStream.print();

//		aggregateStream.writeAsText("./output.out", FileSystem.WriteMode.OVERWRITE);

		env.execute("Process query job");
	}

}

