package redshift;

import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Dfs;
import cascading.tap.hadoop.Hfs;
import com.twitter.maple.jdbc.TableDesc;
import org.pingles.org.pingles.cascading.redshift.RedshiftTap;

import java.util.Properties;

public class SampleFlow {
    public static int main(String[] args) {
        String inputPath = args[0];
        String outputPath = args[1];
        String redshiftJdbcUrl = args[2];
        String redshiftUsername = args[3];
        String redshiftPassword = args[4];

        Properties properties = new Properties();
        AppProps.setApplicationJarClass(properties, SampleFlow.class);

        HadoopFlowConnector flowConnector = new HadoopFlowConnector(properties);

        Tap inTap = new Dfs(new TextDelimited(false, "\t"), inputPath);

        String[] columnNames = {"col1", "col2"};
        String[] columnDefinitions = {""};
        String[] primaryKeys = {};
        TableDesc tableDesc = new TableDesc("cascading_redshift_sample", columnNames, columnDefinitions, primaryKeys);
        Tap outTap = new RedshiftTap(outputPath, redshiftJdbcUrl, redshiftUsername, redshiftPassword, );

        System.out.println("Hello, world!");
        return 0;
    }
}
