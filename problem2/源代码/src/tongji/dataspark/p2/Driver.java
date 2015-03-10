package tongji.dataspark.p2;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.mapred.JobConf;

public class Driver {

    public static String HDFS = "hdfs://";
    public static final Pattern DELIMITER = Pattern.compile("[\t,]|:{2}");

    public static void main(String[] args) throws Exception {
        Map<String, String> path = new HashMap<String, String>();

        if (args.length < 2 || args.length > 3) {
            System.err.println("Usage: hadoop jar DataSpark_problem2.jar <local_path> <hdfs_path> [IP:PORT]");
            System.exit(2);
        } else if (args.length == 3) {
        	HDFS = HDFS + args[2];
        } else {
        	HDFS = HDFS + "namenode:9000";
        }

        path.put("data", args[0]);
        path.put("Step1Input", HDFS + "/user/dataspark/recommend");
        path.put("Step1Output", path.get("Step1Input") + "/stage1");
        path.put("Step2Input", path.get("Step1Output"));
        path.put("Step2Output", path.get("Step1Input") + "/stage2");
        path.put("Step3Input1", path.get("Step1Output"));
        path.put("Step3Output1", path.get("Step1Input") + "/stage3");
        path.put("Step3Input2", path.get("Step2Output"));
        path.put("Step3Output2", path.get("Step1Input") + "/stage4");
		path.put("Step4Input1", path.get("Step3Output1"));
		path.put("Step4Input2", path.get("Step3Output2"));
		path.put("Step4Output", path.get("Step1Input") + "/stage5");
		path.put("Step5Input", path.get("Step4Output"));
		path.put("Step5Output", args[1]);


        Stage1_uv.run(path);
        Stage2_cc.run(path);
        Stage3_uvs.run1(path);
        Stage3_uvs.run2(path);
        Stage4_mul_add.run(path);
        Stage4_mul_add.run2(path);

        System.exit(0);
    }

    public static JobConf config() {
        JobConf conf = new JobConf(Driver.class);
        conf.setJobName("Film_Recommend");
        conf.set("mapreduce.task.io.sort.mb", "1024");
        return conf;
    }

}