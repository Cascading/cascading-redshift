# cascading.redshift

Optimistic punt: create redshift tables and sink data via S3

## Sample

```java
Properties properties = new Properties();
String accessKey = System.getenv("AWS_ACCESS_KEY_ID");
String secretKey = System.getenv("AWS_SECRET_KEY");
properties.setProperty("fs.s3n.awsAccessKeyId", accessKey);
properties.setProperty("fs.s3n.awsSecretAccessKey", secretKey);

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
```