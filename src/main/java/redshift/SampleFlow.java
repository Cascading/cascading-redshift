package redshift;

import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import org.pingles.cascading.redshift.RedshiftScheme;
import org.pingles.cascading.redshift.RedshiftTap;
import java.util.Properties;

public class SampleFlow {
    public static int main(String[] args) {
        String inputPath = args[0];
        String outputPath = args[1];
        String redshiftJdbcUrl = args[2];
        String redshiftUsername = args[3];
        String redshiftPassword = args[4];

        Properties properties = new Properties();
        String accessKey = System.getenv("AWS_ACCESS_KEY_ID");
        String secretKey = System.getenv("AWS_SECRET_KEY");
        properties.setProperty("fs.s3n.awsAccessKeyId", accessKey);
        properties.setProperty("fs.s3n.awsSecretAccessKey", secretKey);

        System.out.println(String.format("fs.s3n.awsAccessKeyId=%s", accessKey));
        System.out.println(String.format("fs.s3n.awsSecretAccessKey=%s", secretKey));

        AppProps.setApplicationJarClass(properties, SampleFlow.class);

        HadoopFlowConnector flowConnector = new HadoopFlowConnector(properties);

        Tap inTap = new Hfs(new TextDelimited(new Fields("line"), false, "\t"), inputPath);

        String tableName = "cascading_redshift_sample";
        String[] columnNames = {"line"};
        String[] columnDefinitions = {"VARCHAR(500)"};
        String distributionKey = "line";
        String[] sortKeys = {"line"};
        RedshiftScheme scheme = new RedshiftScheme(new Fields("line"), new Fields("line"), tableName, columnNames, columnDefinitions, distributionKey, sortKeys);
        Tap outTap = new RedshiftTap(outputPath, accessKey, secretKey, redshiftJdbcUrl, redshiftUsername, redshiftPassword, scheme);

        //outTap = new Hfs(new TextDelimited(false, "\t"), outputPath);

        Pipe copyPipe = new Pipe("copy");
        FlowDef flowDef = FlowDef.flowDef().addSource(copyPipe, inTap).addTailSink(copyPipe, outTap);

        flowConnector.connect(flowDef).complete();

        return 0;
    }
}
