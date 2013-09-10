/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pingles.cascading.redshift.provider;

import static org.pingles.cascading.redshift.provider.Utils.*;

import java.util.Properties;

import org.pingles.cascading.redshift.AWSCredentials;
import org.pingles.cascading.redshift.RedshiftScheme;
import org.pingles.cascading.redshift.RedshiftTap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/**
 * The {@link RedshiftFactory} is a factory class to create {@link RedshiftTap}s
 * and {@link RedshiftScheme}s. The class is meant to be used by <a
 * href="http://www.cascading.org/lingual/">lingual</a> for dynamically creating
 * Taps and Schemes, so that redshift can be used as a <a
 * href="http://docs.cascading.org/lingual/1.0/#_creating_a_data_provider"
 * >provider</a> within lingual.
 */
public class RedshiftFactory
  {
  /**
   * LOGGER
   */
  private static final Logger LOG = LoggerFactory.getLogger( RedshiftFactory.class );

  /** environment variable for the aws access key */
  private static final String SYSTEM_AWS_ACCESS_KEY = "AWS_ACCESS_KEY";

  /** environment variable for the aws secret key */
  private static final String SYSTEM_AWS_SECRET_KEY = "AWS_SECRET_KEY";

  public static final String PROTOCOL_S3_OUTPUT_PATH = "s3OutputPath";
  public static final String PROTOCOL_AWS_ACCESS_KEY = "awsAccesKey";
  public static final String PROTOCOL_AWS_SECRET_KEY = "awsSecretKey";
  public static final String PROTOCOL_JDBC_USER = "jdbcUser";
  public static final String PROTOCOL_JDBC_PASSWORD = "jdbcPassword";
  public static final String PROTOCOL_COPY_TIMEOUT = "copyTimeout";

  public static final String FORMAT_SEPARATOR = "separator";
  public static final String FORMAT_DEFAULT_SEPARATOR = ":";
  public static final String FORMAT_TABLE_NAME = "tableName";
  public static final String FORMAT_COLUMN_NAMES = "columnNames";
  public static final String FORMAT_COLUMN_DEFINITIONS = "columnDefs";
  public static final String FORMAT_DISTRIBUTION_KEY = "distributionKey";
  public static final String FORMAT_SORT_KEYS = "sortKeys";
  public static final String FORMAT_COPY_OPTIONS = "copyOptions";
  public static final String FORMAT_FIELD_DELIMITER = "fieldDelimiter";
  public static final String FORMAT_DEFAULT_FIELD_DELIMITER = "\t";

  @SuppressWarnings("rawtypes")
  public Tap createTap(String protocol, Scheme scheme, String identifier, SinkMode mode, Properties properties)
    {
    LOG.info( "creating redshift protocol with properties {} in mode {}", properties, mode );

    String s3Path = throwIfNullOrEmpty( properties.getProperty( PROTOCOL_S3_OUTPUT_PATH ), "s3Path missing" );

    AWSCredentials credentials = determineAwsCredentials( properties );

    String jdbcUrl = throwIfNullOrEmpty( identifier, "missing identifier" );
    String jdbcUser = throwIfNullOrEmpty( properties.getProperty( PROTOCOL_JDBC_USER ), "jdbcUser missing" );
    String jdbcPassword = throwIfNullOrEmpty( properties.getProperty( PROTOCOL_JDBC_PASSWORD ), "jdbcPassword missing" );

    String timeoutProperty = properties.getProperty( PROTOCOL_COPY_TIMEOUT );
    if ( isNullOrEmpty( timeoutProperty ) )
      return new RedshiftTap( s3Path, credentials, jdbcUrl, jdbcUser, jdbcPassword, (RedshiftScheme) scheme, mode );

    return new RedshiftTap( s3Path, credentials, jdbcUrl, jdbcUser, jdbcPassword, (RedshiftScheme) scheme, mode,
        Integer.parseInt( timeoutProperty ) );

    }

  @SuppressWarnings("rawtypes")
  public Scheme createScheme(String format, Fields fields, Properties properties)
    {
    String tableName = throwIfNullOrEmpty( properties.getProperty( FORMAT_TABLE_NAME ), "table name missing" );

    String separator = properties.getProperty( FORMAT_SEPARATOR, FORMAT_DEFAULT_SEPARATOR );

    String columnNamesProperty = throwIfNullOrEmpty( properties.getProperty( FORMAT_COLUMN_NAMES ), "column names missing" );
    String[] columnNames = columnNamesProperty.split( separator );

    String columnDefinitionsProperty = throwIfNullOrEmpty( properties.getProperty( FORMAT_COLUMN_DEFINITIONS ),
        "column definitions missing" );
    String[] columnDefinitions = columnDefinitionsProperty.split( separator );

    String distributionKey = throwIfNullOrEmpty( properties.getProperty( FORMAT_DISTRIBUTION_KEY ), "distribution key missing" );

    String sortKeysProperty = throwIfNullOrEmpty( properties.getProperty( FORMAT_SORT_KEYS ), "sort keys missing" );
    String[] sortKeys = sortKeysProperty.split( separator );

    String[] copyOptions = new String[] {};
    String copyOptionsProperty = properties.getProperty( FORMAT_COPY_OPTIONS );
    if ( !isNullOrEmpty( copyOptionsProperty ) )
      copyOptions = copyOptionsProperty.split( separator );

    String delimiter = properties.getProperty( FORMAT_FIELD_DELIMITER, FORMAT_DEFAULT_FIELD_DELIMITER );

    return new RedshiftScheme( Fields.ALL, fields, tableName, columnNames, columnDefinitions, distributionKey, sortKeys, copyOptions,
        delimiter );
    }

  /**
   * Helper method that tries to determine the AWS credentials. It first tries
   * the {@link Properties} passed in, next it checks for the environment
   * variables <code>AWS_ACCESS_KEY</code> and <code>AWS_SECRET_KEY</code>. If
   * none of the above contains the credentials, the method returns
   * {@link AWSCredentials#UNKNOWN}.
   * 
   * @param properties a {@link Properties} object, which can contain the AWS
   *          credentials.
   * @return an {@link AWSCredentials} installed.
   */
  private AWSCredentials determineAwsCredentials(Properties properties)
    {
    // try to determine the aws credentials, using the default unknown
    AWSCredentials awsCredentials = AWSCredentials.UNKNOWN;

    // first try the properties
    String awsAccessKey = properties.getProperty( PROTOCOL_AWS_ACCESS_KEY );
    String awsSecretKey = properties.getProperty( PROTOCOL_AWS_SECRET_KEY );

    if ( !isNullOrEmpty( awsAccessKey ) && !isNullOrEmpty( awsSecretKey ) )
      awsCredentials = new AWSCredentials( awsAccessKey, awsSecretKey );

    // next try environment variables
    if ( awsCredentials == AWSCredentials.UNKNOWN )
      {
      awsAccessKey = System.getenv( SYSTEM_AWS_ACCESS_KEY );
      awsSecretKey = System.getenv( SYSTEM_AWS_SECRET_KEY );
      if ( !isNullOrEmpty( awsAccessKey ) && !isNullOrEmpty( awsSecretKey ) )
        awsCredentials = new AWSCredentials( awsAccessKey, awsSecretKey );

      }
    // if this does not work, the last try will be in RedshiftTap#sinkConfInit
    return awsCredentials;
    }
  }
