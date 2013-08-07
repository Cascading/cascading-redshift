# cascading.redshift

Provides an optimised [Cascading](http://cascading.org) sink for Amazon Redshift; String data will be checked for invalid codepoints, output files will be Gzip compressed automatically etc.

Data is sunk using an s3 path before a JDBC connection issues the DROP/CREATE/COPY statements.

## Installing

[cascading.redshift](http://conjars.org/org.pingles/cascading.redshift) is hosted on [conjars.org](http://conjars.org). You can add conjars.org as a
maven repository and install the latest release:

```xml
<dependency>
  <groupId>org.pingles</groupId>
  <artifactId>cascading.redshift</artifactId>
  <version>0.14</version>
</dependency>
```

## Using

The wordcount example from Cascading with a sink to send data to Redshift. 

```java
HadoopFlowConnector flowConnector = new HadoopFlowConnector(properties);

Tap inTap = new Hfs(new TextDelimited(new Fields("line"), false, "\t"), inputPath);

String tableName = "cascading_redshift_sample";
String[] columnNames = {"word", "freq"};
String[] columnDefinitions = {"VARCHAR(500)", "SMALLINT"};
String distributionKey = "word";
String[] sortKeys = {"freq"};

String outputPath = "s3n://mybucket/data"

RedshiftScheme scheme = new RedshiftScheme(Fields.ALL, new Fields("word", "count"), tableName, columnNames, columnDefinitions, distributionKey, sortKeys, new String[] {}, "\001");
Tap outTap = new RedshiftTap(outputPath, accessKey, secretKey, redshiftJdbcUrl, redshiftUsername, redshiftPassword, scheme, SinkMode.REPLACE);

Pipe assembly = new Pipe("wordcount");
String wordSplitRegex = "(?<!\\pL)(?=\\pL)[^ ]*(?<=\\pL)(?!\\pL)";

assembly = new Each(assembly, new Fields("line"), new RegexGenerator(new Fields("word"), wordSplitRegex));
assembly = new GroupBy(assembly, new Fields("word"));
assembly = new Every(assembly, new Fields("word"), new Count(new Fields("count")), new Fields("word", "count"));

flowConnector.connect("word-count", inTap, outTap, assembly).complete();
```
