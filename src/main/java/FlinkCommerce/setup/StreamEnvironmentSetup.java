package FlinkCommerce.setup;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamEnvironmentSetup {
    public static StreamExecutionEnvironment createEnvironment() {
        return StreamExecutionEnvironment.getExecutionEnvironment();
    }
}