package FlinkCommerce.setup;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamEnvironmentSetup {
    public StreamExecutionEnvironment createEnvironment() {
        return StreamExecutionEnvironment.getExecutionEnvironment();
    }
}