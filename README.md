# cascading.redshift

Optimistic punt: create redshift tables and sink data via S3

## Usage

cascading.redshift is hosted on [conjars.org](http://conjars.org). You can add conjars.org as a
maven repository and install the early WIP 0.1-SNAPSHOT release.

## Sample

```java
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
```