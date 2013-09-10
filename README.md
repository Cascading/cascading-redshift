# cascading.redshift

Provides an optimised [Cascading](http://cascading.org) sink for Amazon Redshift; String data will be checked for invalid codepoints, output files will be Gzip compressed automatically etc.

Data is sunk using an s3 path before a JDBC connection issues the DROP/CREATE/COPY statements.

## Installing

[cascading-redshift](http://conjars.org/cascading/cascading-redshift) is hosted on [conjars.org](http://conjars.org). You can add conjars.org as a
maven repository and install the latest release:

```xml
<dependency>
  <groupId>cascading</groupId>
  <artifactId>cascading-redshift</artifactId>
  <version>0.15</version>
</dependency>
```

## Using

### Java

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

AWSCredentials credentials = new AWSCredentials(accessKey, secretKey);

RedshiftScheme scheme = new RedshiftScheme(Fields.ALL, new Fields("word", "count"), tableName, columnNames, columnDefinitions, distributionKey, sortKeys, new String[] {}, "\001");
Tap outTap = new RedshiftTap(outputPath, credentials, redshiftJdbcUrl, redshiftUsername, redshiftPassword, scheme, SinkMode.REPLACE);

Pipe assembly = new Pipe("wordcount");
String wordSplitRegex = "(?<!\\pL)(?=\\pL)[^ ]*(?<=\\pL)(?!\\pL)";

assembly = new Each(assembly, new Fields("line"), new RegexGenerator(new Fields("word"), wordSplitRegex));
assembly = new GroupBy(assembly, new Fields("word"));
assembly = new Every(assembly, new Fields("word"), new Count(new Fields("count")), new Fields("word", "count"));

flowConnector.connect("word-count", inTap, outTap, assembly).complete();
```

### Lingual

The sink can also be used from within lingual, if you use the `fatJar` produced
by the build. If you have followed the [lingual
introduction](http://docs.cascading.org/lingual/1.0/), this should be
straight forward. If you haven't followed the tutorial, might be a bit to terse
for you. If you are intersted in using the redshift tap from lingual, follow the
tutorial first.

Please note that the redshift provider only supports the `hadoop` platform of
lingual, the `local` platform is not supported. To simplify the interaction with
the lingual catalog, you can set the platform as an evironment variable:

```
export LINGUAL_PLATFORM=hadoop
```

The provder name is `redshift` and the format and protocol are defined as
`redshift-s3`, since this provider uses amazon `S3` as intermediate storage.
Redshift also allows you to read from a dynamo db instead of S3, however that is
currently not supported.

To register the sink within lingual, you first have to build the provider
compliant jar file in this project:

```
gradle build
```

The register the provider-jar, use lingual catalog:

```
lingual catalog --provider -add ./build/libs/cascading-redshift-0.15.0-provider.jar
```

Next add a schema called `working`:

```
lingual catalog --schema working --add
```

Next we define a stereotype:
```
lingual catalog --schema working --stereotype titles -add --columns TITLE,CNT --types string,int
```

Register the `redshift-s3` format in the schema.

```
lingual catalog --schema working --format redshift-s3 --add --properties-file redshift-format.properties --provider redshift
```

The redshift-format.properties contains information about the table structure
for redshift:

```
tableName=title_counts
columnNames=title:cnt
columnDefs=varchar(100):int
distributionKey=title
sortKeys=title
copyOptions=
```

Next we register the `redshift-s3` protocol in the `working` schema:

```
lingual catalog --schema working --protocol redshift-s3 --add --properties-file redshift-protocol.properties  --provider redshift
```

The `redshift-protocol.properties` looks like this:

```
s3OutputPath=s3n://<bucketName>/<path>/
jdbcUser=redshift-master-user
jdbcPassword=redshift-master-user-password
copyTimeout=5
```

And finally register the table in lingual. You can find the jdbc-url to use in
the redshift cluster details in your aws console:

```
lingual catalog --schema working --table title_counts --add "jdbc:postgresql://<cluster-name>.<random>.<region>.redshift.amazonaws.com:<port>/<database>" --stereotype titles --protocol redshift-s3  --format redshift-s3
```

Before you write into the table, make sure, that the security group is correctly
configured.

After this, we can directly select into redshift from the lingual shell:

```
(lingual shell) insert into "working"."title_counts" select title, count( title) as cnt  from employees.titles group by title;
    +-----------+
    | ROWCOUNT  |
    +-----------+
    | 7         |
    +-----------+
```

The data has now been written into the redshift table `title_counts`.

#### AWS credentials

Since redshift reads the data initially from S3, you have to provide a valid aws
access-key/secret-key combination. There are multiple options to do that:

- put them in the redshift-protocol.properties file as `awsAccessKey` and `awsSecretKey`
- set them as the environment variables `AWS_ACCESS_KEY` and `AWS_SECRET_KEY`
- put them in the your `mapred-site.xml` file as `fs.s3n.awsAccessKeyId` and
  `fs.s3n.awsSecretAccessKey`

If you are running your jobs on Amazon EMR, the credentials will be in the
job-conf and will automatically be picked up from there.
