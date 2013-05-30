package redshift;

import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.Aggregator;
import cascading.operation.Function;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexGenerator;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
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
        String[] columnNames = {"word", "freq"};
        String[] columnDefinitions = {"VARCHAR(500)", "SMALLINT"};
        String distributionKey = "word";
        String[] sortKeys = {"freq"};

        RedshiftScheme scheme = new RedshiftScheme(Fields.ALL, new Fields("word", "count"), tableName, columnNames, columnDefinitions, distributionKey, sortKeys, new String[] {}, "\001");
        Tap outTap = new RedshiftTap(outputPath, accessKey, secretKey, redshiftJdbcUrl, redshiftUsername, redshiftPassword, scheme, SinkMode.REPLACE);

        Pipe assembly = new Pipe("wordcount");
        String wordSplitRegex = "(?<!\\pL)(?=\\pL)[^ ]*(?<=\\pL)(?!\\pL)";

        assembly = new Each(assembly, new Fields("line"), new RegexGenerator(new Fields("word"), wordSplitRegex));
        assembly = new GroupBy(assembly, new Fields("word"));
        assembly = new Every(assembly, new Fields("word"), new Count(new Fields("count")), new Fields("word", "count"));

        flowConnector.connect("word-count", inTap, outTap, assembly).complete();

        return 0;
    }
}
