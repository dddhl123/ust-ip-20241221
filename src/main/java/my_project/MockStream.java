package my_project;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.io.BufferedReader;
import java.io.FileReader;

public class MockStream implements SourceFunction<String> {
    private String filePath;
    private int interval;
    private volatile boolean isRunning = true;

    private long currentTimestamp = 0;

    public MockStream(String filePath, int interval) {
        this.filePath = filePath;
        this.interval = interval;
    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            int cnt = 0;
            while (isRunning && (line = reader.readLine()) != null) {
                currentTimestamp += interval;
                sourceContext.collectWithTimestamp(line, currentTimestamp);
                sourceContext.emitWatermark(new Watermark(currentTimestamp));
                cnt ++;
//                if (cnt % 10000 == 0) {
//                    System.out.println("Emitted " + cnt + " records");
//                }
            }
        }catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
