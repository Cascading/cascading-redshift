package redshift;

import cascading.flow.hadoop.HadoopFlowConnector;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class SampleFlow {
    private static final Logger LOGGER = LoggerFactory.getLogger(SampleFlow.class);

    public static void main(String[] args) throws IOException {
        String propertiesPath = args[0];
        FileInputStream propsStream = new FileInputStream(new File(propertiesPath));
        Properties props = new Properties();
        props.load(propsStream);
        propsStream.close();

        String inputPath = props.getProperty("input.path");
        String outputPath = props.getProperty("output.path");
        String redshiftJdbcUrl = props.getProperty("jdbc.url");
        String redshiftUsername = props.getProperty("jdbc.username");
        String redshiftPassword = props.getProperty("jdbc.password");
        String accessKey = props.getProperty("aws.access.key");
        String secretKey = props.getProperty("aws.secret.key");

        Properties cascadingProperties = new Properties();
        for (Object k : props.keySet()) {
            LOGGER.info("{}= {}", k, props.getProperty((String) k));
        }

        cascadingProperties.setProperty("fs.s3n.awsAccessKeyId", accessKey);
        cascadingProperties.setProperty("fs.s3n.awsSecretAccessKey", secretKey);
        AppProps.setApplicationJarClass(cascadingProperties, SampleFlow.class);

        HadoopFlowConnector flowConnector = new HadoopFlowConnector(cascadingProperties);

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

        LOGGER.info("Successfully completed sample");
    }
}
