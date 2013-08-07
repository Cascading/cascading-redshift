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

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.Properties;

import org.junit.Test;
import org.pingles.cascading.redshift.RedshiftScheme;
import org.pingles.cascading.redshift.RedshiftTap;

import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/**
 * Tests for {@link RedshiftFactory}.
 * 
 */
public class RedshiftFactoryTest
  {

  @Test(expected = IllegalArgumentException.class)
  public void testCreateTapNoS3Path()
    {
    RedshiftScheme mockScheme = mock( RedshiftScheme.class );
    RedshiftFactory factory = new RedshiftFactory();
    factory.createTap( "redshift", mockScheme, "some identifier", SinkMode.REPLACE, new Properties() );
    }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateTapNoAwsAcccessKey()
    {
    RedshiftScheme mockScheme = mock( RedshiftScheme.class );
    RedshiftFactory factory = new RedshiftFactory();
    Properties props = new Properties();
    props.setProperty( RedshiftFactory.PROTOCOL_S3_OUTPUT_PATH, "s3n://something/something" );
    factory.createTap( "redshift", mockScheme, "some identifier", SinkMode.REPLACE, props );
    }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateTapNoAwsSecretKey()
    {
    RedshiftScheme mockScheme = mock( RedshiftScheme.class );
    RedshiftFactory factory = new RedshiftFactory();
    Properties props = new Properties();
    props.setProperty( RedshiftFactory.PROTOCOL_S3_OUTPUT_PATH, "s3n://something/something" );
    props.setProperty( RedshiftFactory.PROTOCOL_AWS_ACCESS_KEY, "ABCDEFG" );
    factory.createTap( "redshift", mockScheme, "some identifier", SinkMode.REPLACE, props );
    }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateTapNoJdbcUrlInIdentifier()
    {
    RedshiftScheme mockScheme = mock( RedshiftScheme.class );
    RedshiftFactory factory = new RedshiftFactory();
    Properties props = new Properties();
    props.setProperty( RedshiftFactory.PROTOCOL_S3_OUTPUT_PATH, "s3n://something/something" );
    props.setProperty( RedshiftFactory.PROTOCOL_AWS_ACCESS_KEY, "ABCDEFG" );
    props.setProperty( RedshiftFactory.PROTOCOL_AWS_SECRET_KEY, "ahdsjfhalkjfhaklhalhdalkhflahjfdshfl" );
    factory.createTap( "redshift", mockScheme, "", SinkMode.REPLACE, props );
    }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateTapNoJdbcUser()
    {
    RedshiftScheme mockScheme = mock( RedshiftScheme.class );
    RedshiftFactory factory = new RedshiftFactory();
    Properties props = new Properties();
    props.setProperty( RedshiftFactory.PROTOCOL_S3_OUTPUT_PATH, "s3n://something/something" );
    props.setProperty( RedshiftFactory.PROTOCOL_AWS_ACCESS_KEY, "ABCDEFG" );
    props.setProperty( RedshiftFactory.PROTOCOL_AWS_SECRET_KEY, "ahdsjfhalkjfhaklhalhdalkhflahjfdshfl" );
    factory.createTap( "redshift", mockScheme, "jdbc://redshift", SinkMode.REPLACE, props );
    }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateTapNoJdbcPassword()
    {
    RedshiftScheme mockScheme = mock( RedshiftScheme.class );
    RedshiftFactory factory = new RedshiftFactory();
    Properties props = new Properties();
    props.setProperty( RedshiftFactory.PROTOCOL_S3_OUTPUT_PATH, "s3n://something/something" );
    
    props.setProperty( RedshiftFactory.PROTOCOL_AWS_ACCESS_KEY, "ABCDEFG" );
    props.setProperty( RedshiftFactory.PROTOCOL_AWS_SECRET_KEY, "ahdsjfhalkjfhaklhalhdalkhflahjfdshfl" );
    props.setProperty( RedshiftFactory.PROTOCOL_JDBC_USER, "username" );
    factory.createTap( "redshift", mockScheme, "jdbc://redshift", SinkMode.REPLACE, props );
    }

  @SuppressWarnings("rawtypes")
  @Test
  public void testCreateTapDefaultTimeout()
    {
    RedshiftScheme mockScheme = mock( RedshiftScheme.class );
    when( mockScheme.getFieldDelimiter() ).thenReturn( "☃" );
    when( mockScheme.getSinkFields() ).thenReturn( new Fields( new String[] { "test" } ) );

    RedshiftFactory factory = new RedshiftFactory();
    Properties props = new Properties();
    props.setProperty( RedshiftFactory.PROTOCOL_S3_OUTPUT_PATH, "s3n://something/something" );
    props.setProperty( RedshiftFactory.PROTOCOL_AWS_ACCESS_KEY, "ABCDEFG" );
    props.setProperty( RedshiftFactory.PROTOCOL_AWS_SECRET_KEY, "ahdsjfhalkjfhaklhalhdalkhflahjfdshfl" );
    props.setProperty( RedshiftFactory.PROTOCOL_JDBC_USER, "username" );
    props.setProperty( RedshiftFactory.PROTOCOL_JDBC_PASSWORD, "username" );

    Tap tap = factory.createTap( "redshift", mockScheme, "jdbc://redshift", SinkMode.REPLACE, props );

    assertNotNull( tap );
    assertTrue( tap instanceof RedshiftTap );
    }

  @SuppressWarnings("rawtypes")
  @Test
  public void testCreateTapCustomTimeout()
    {
    RedshiftScheme mockScheme = mock( RedshiftScheme.class );
    when( mockScheme.getFieldDelimiter() ).thenReturn( "☃" );
    when( mockScheme.getSinkFields() ).thenReturn( new Fields( new String[] { "test" } ) );

    RedshiftFactory factory = new RedshiftFactory();
    Properties props = new Properties();
    props.setProperty( RedshiftFactory.PROTOCOL_S3_OUTPUT_PATH, "s3n://something/something" );
    props.setProperty( RedshiftFactory.PROTOCOL_AWS_ACCESS_KEY, "ABCDEFG" );
    props.setProperty( RedshiftFactory.PROTOCOL_AWS_SECRET_KEY, "ahdsjfhalkjfhaklhalhdalkhflahjfdshfl" );
    props.setProperty( RedshiftFactory.PROTOCOL_JDBC_USER, "username" );
    props.setProperty( RedshiftFactory.PROTOCOL_JDBC_PASSWORD, "username" );
    props.setProperty( RedshiftFactory.PROTOCOL_COPY_TIMEOUT, "42" );

    Tap tap = factory.createTap( "redshift", mockScheme, "jdbc://redshift", SinkMode.REPLACE, props );

    assertNotNull( tap );
    assertTrue( tap instanceof RedshiftTap );

    }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateSchemeNoTableName()
    {
    Fields field = new Fields( new String[] { "a", "b", "c" } );
    RedshiftFactory factory = new RedshiftFactory();
    Properties props = new Properties();

    factory.createScheme( "redshift", field, props );
    }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateSchemeNoColumnNames()
    {
    Fields field = new Fields( new String[] { "a", "b", "c" } );
    RedshiftFactory factory = new RedshiftFactory();
    Properties props = new Properties();
    props.setProperty( RedshiftFactory.FORMAT_TABLE_NAME, "table" );

    factory.createScheme( "redshift", field, props );
    }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateSchemeNoColumnDefs()
    {
    Fields field = new Fields( new String[] { "a", "b", "c" } );
    RedshiftFactory factory = new RedshiftFactory();
    Properties props = new Properties();
    props.setProperty( RedshiftFactory.FORMAT_TABLE_NAME, "table" );
    props.setProperty( RedshiftFactory.FORMAT_COLUMN_NAMES, "a:b:c" );

    factory.createScheme( "redshift", field, props );
    }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateSchemeNoDistributionKey()
    {
    Fields field = new Fields( new String[] { "a", "b", "c" } );
    RedshiftFactory factory = new RedshiftFactory();
    Properties props = new Properties();
    props.setProperty( RedshiftFactory.FORMAT_TABLE_NAME, "table" );
    props.setProperty( RedshiftFactory.FORMAT_COLUMN_NAMES, "a:b:c" );
    props.setProperty( RedshiftFactory.FORMAT_COLUMN_DEFINITIONS, "int:int:int" );

    factory.createScheme( "redshift", field, props );
    }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateSchemeNoSortKeys()
    {
    Fields field = new Fields( new String[] { "a", "b", "c" } );
    RedshiftFactory factory = new RedshiftFactory();
    Properties props = new Properties();
    props.setProperty( RedshiftFactory.FORMAT_TABLE_NAME, "table" );
    props.setProperty( RedshiftFactory.FORMAT_COLUMN_NAMES, "a:b:c" );
    props.setProperty( RedshiftFactory.FORMAT_COLUMN_DEFINITIONS, "int:int:int" );
    props.setProperty( RedshiftFactory.FORMAT_DISTRIBUTION_KEY, "a" );

    factory.createScheme( "redshift", field, props );
    }

  @SuppressWarnings("rawtypes")
  @Test
  public void testCreateScheme()
    {
    Fields field = new Fields( new String[] { "a", "b", "c" } );
    RedshiftFactory factory = new RedshiftFactory();
    Properties props = new Properties();
    props.setProperty( RedshiftFactory.FORMAT_TABLE_NAME, "table" );
    props.setProperty( RedshiftFactory.FORMAT_COLUMN_NAMES, "a:b:c" );
    props.setProperty( RedshiftFactory.FORMAT_COLUMN_DEFINITIONS, "int:int:int" );
    props.setProperty( RedshiftFactory.FORMAT_DISTRIBUTION_KEY, "a" );
    props.setProperty( RedshiftFactory.FORMAT_SORT_KEYS, "a:b" );

    Scheme scheme = factory.createScheme( "redshift", field, props );
    assertTrue( scheme instanceof RedshiftScheme );
    }

  @SuppressWarnings("rawtypes")
  @Test
  public void testCreateSchemeWithCopyOptions()
    {
    Fields field = new Fields( new String[] { "a", "b", "c" } );
    RedshiftFactory factory = new RedshiftFactory();
    Properties props = new Properties();
    props.setProperty( RedshiftFactory.FORMAT_TABLE_NAME, "table" );
    props.setProperty( RedshiftFactory.FORMAT_COLUMN_NAMES, "a:b:c" );
    props.setProperty( RedshiftFactory.FORMAT_COLUMN_DEFINITIONS, "int:int:int" );
    props.setProperty( RedshiftFactory.FORMAT_DISTRIBUTION_KEY, "a" );
    props.setProperty( RedshiftFactory.FORMAT_SORT_KEYS, "a:b" );
    props.setProperty( RedshiftFactory.FORMAT_COPY_OPTIONS, "option1:option2" );
    props.setProperty( RedshiftFactory.FORMAT_FIELD_DELIMITER, "☃" );

    Scheme scheme = factory.createScheme( "redshift", field, props );
    assertTrue( scheme instanceof RedshiftScheme );

    }

  }
