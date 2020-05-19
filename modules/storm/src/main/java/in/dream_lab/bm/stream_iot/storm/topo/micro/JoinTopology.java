package in.dream_lab.bm.stream_iot.storm.topo.micro;

import in.dream_lab.bm.stream_iot.storm.bolts.BaseTaskBolt;

import in.dream_lab.bm.stream_iot.storm.bolts.IoTStatsBolt.MultiLinePlotBolt;
import in.dream_lab.bm.stream_iot.storm.genevents.factory.ArgumentClass;
import in.dream_lab.bm.stream_iot.storm.genevents.factory.ArgumentParser;
import in.dream_lab.bm.stream_iot.storm.sinks.Sink;
import in.dream_lab.bm.stream_iot.storm.spouts.SampleSpout;
import in.dream_lab.bm.stream_iot.storm.spouts.JoinSpout1;
import in.dream_lab.bm.stream_iot.storm.spouts.JoinSpout2;
import in.dream_lab.bm.stream_iot.tasks.io.ZipMultipleBufferTask;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by anshushukla on 18/05/15.
 */
public class JoinTopology {

    public static void main(String[] args) throws Exception {

        ArgumentClass argumentClass = ArgumentParser.parserCLI(args);
        if (argumentClass == null) {
            System.out.println("ERROR! INVALID NUMBER OF ARGUMENTS");
            return;
        }

        String logFilePrefix = argumentClass.getTopoName() + "-" + argumentClass.getExperiRunId() + "-"
                + argumentClass.getScalingFactor() + ".log";
        String sinkLogFileName = argumentClass.getOutputDirName() + "/sink-" + logFilePrefix;
        String spoutLogFileName1 = argumentClass.getOutputDirName() + "/spout1-" + logFilePrefix;
        String spoutLogFileName2 = argumentClass.getOutputDirName() + "/spout2-" + logFilePrefix;
        String taskPropFilename = argumentClass.getTasksPropertiesFilename();

        Config conf = new Config();
        conf.setDebug(false); // make it false for actual benchmark
        // conf.put("topology.eventlogger.executors",1);
        // conf.setNumWorkers(12);

        Properties p_ = new Properties();
        InputStream input = new FileInputStream(taskPropFilename);
        p_.load(input);

        TopologyBuilder builder = new TopologyBuilder();

        String spout2InputFilePath = "/home/rrk/Documents/Github/riot-bench/modules/tasks/src/main/resources/csv-input-2.csv";
        builder.setSpout("join-spout-1", new JoinSpout1(argumentClass.getInputDatasetPathName(), spoutLogFileName1,
                argumentClass.getScalingFactor()), 1);
        builder.setSpout("join-spout-2", new JoinSpout2(spout2InputFilePath, spoutLogFileName2,
                argumentClass.getScalingFactor()), 1);

        String taskName = argumentClass.getTasksName();
        BaseTaskBolt taskBolt = MicroTopologyFactory.newTaskBolt(taskName, p_);

        builder.setBolt(taskName, taskBolt, 1).shuffleGrouping("join-spout-1").shuffleGrouping("join-spout-2");
        // builder.setBolt("join-bolt", new JoinBolt()).shuffleGrouping("join-spout-1").shuffleGrouping("join-spout-2");

        builder.setBolt("sink", new Sink(sinkLogFileName), 1).shuffleGrouping(taskName);

        StormTopology stormTopology = builder.createTopology();

        if (argumentClass.getDeploymentMode().equals("C")) {
            StormSubmitter.submitTopology(argumentClass.getTopoName(), conf, stormTopology);
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(argumentClass.getTopoName(), conf, stormTopology);
            Utils.sleep(100000);
            cluster.killTopology(argumentClass.getTopoName());
            cluster.shutdown();
        }
    }
}

// L IdentityTopology
// /Users/anshushukla/PycharmProjects/DataAnlytics1/Storm-Scheduler-SC-scripts/SYS-inputcsvSCTable-1spouts100mps-480sec.csv
// PLUG-210 1.0 /Users/anshushukla/data/output/temp
// /Users/anshushukla/Downloads/Incomplete/stream/iot-bm/modules/tasks/src/main/resources/tasks.properties
// BlockWindowAverage
// L IdentityTopology
// /Users/anshushukla/PycharmProjects/DataAnlytics1/Storm-Scheduler-SC-scripts/SYS-inputcsvSCTable-1spouts100mps-480sec.csv
// PLUG-210 1.0 /Users/anshushukla/data/output/temp
// /Users/anshushukla/Downloads/Incomplete/stream/iot-bm/modules/tasks/src/main/resources/tasks_TAXI.properties
// LinearRegressionTrainBatched
// L IdentityTopology
// /Users/anshushukla/PycharmProjects/DataAnlytics1/Storm-Scheduler-SC-scripts/SYS-inputcsvSCTable-1spouts100mps-480sec.csv
// PLUG-210 1.0 /Users/anshushukla/data/output/temp
// /Users/anshushukla/Downloads/Incomplete/stream/iot-bm/modules/tasks/src/main/resources/tasks_TAXI.properties
// DecisionTreeTrainBatched