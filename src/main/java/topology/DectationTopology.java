package topology;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import static topology.ConfigUtil.*;

/**
 * Created by ding on 14-7-3.
 */
public class DectationTopology implements Constant {

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.out.println("Enter path to config file!");
            System.exit(0);
        }
        Config conf = ConfigUtil.readConfig(args[0]);

        TopologyBuilder builder = new TopologyBuilder();

        String host = (String) conf.get("redis.host");
        int port = getInt(conf, "redis.port", 6379);
        String queue = (String) conf.get("redis.queue");

        builder.setSpout("image-input", new ImageSource(host, port, queue), getInt(conf, "vd.spout.parallelism", 1));

        builder.setBolt("feat-ext", new FeatureExtracter(), getInt(conf, "vd.feat-ext.parallelism", 1))
                .shuffleGrouping("image-input", STREAM_IMG_OUTPUT)
                .setNumTasks(getInt(conf, "vd.feat-ext.tasks", 1));
        builder.setBolt("matcher", new Matcher(), getInt(conf, "vd.matcher.parallelism", 1))
                .allGrouping("feat-ext", STREAM_FEATURE_DESC)
                .setNumTasks(getInt(conf, "vd.matcher.tasks", 1));
        builder.setBolt("aggregator", new Aggregater(), getInt(conf, "vd.aggregator.parallelism", 1))
                .fieldsGrouping("feat-ext", STREAM_FEATURE_COUNT, new Fields(FIELD_FRAME_ID))
                .fieldsGrouping("matcher", STREAM_MATCH_IMAGES, new Fields(FIELD_FRAME_ID))
                .setNumTasks(getInt(conf, "vd.aggregator.tasks", 1));

        int numWorkers = getInt(conf, "vd-worker.count", 1);
        conf.setNumWorkers(numWorkers);
        conf.setMaxSpoutPending(getInt(conf, "vd-MaxSpoutPending", 0));
        conf.setStatsSampleRate(1.0);

        StormSubmitter.submitTopology("ton-normal-vld-JB", conf, builder.createTopology());

    }

}
