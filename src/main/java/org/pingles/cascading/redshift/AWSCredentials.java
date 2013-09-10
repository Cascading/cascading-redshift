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
package org.pingles.cascading.redshift;

import java.io.Serializable;


/**
 * {@link AWSCredentials} holds the aws access key and secret key. If you the {@link AWSCredentials} are currently
 * unknown you can use the constant {@link AWSCredentials#UNKNOWN}.
 */
public class AWSCredentials implements Serializable
  {
  
  private static final long serialVersionUID = 6847761328031597888L;

  /** used to signal, that the credentials are still missing. */
  public final static AWSCredentials UNKNOWN = new AWSCredentials( "", "" );
  
  /** AWS access key */
  private String accessKey;

  /** AWS */
  private String secretKey;


  /**
   * Constructs a new {@link AWSCredentials} instance with the given accesKey and secretKey.
   * @param accessKey The aws access key.
   * @param secretKey The aws secret key.
   */
  public AWSCredentials(String accessKey, String secretKey)
    {
    this.accessKey = accessKey;
    this.secretKey = secretKey;
    }


  /**
   * Returns the acccess key
   * @return the access key.
   */
  public String getAccessKey()
    {
    return accessKey;
    }

  /**
   * Returns the secret key.
   * @return te secret key.
   */
  public String getSecretKey()
    {
    return secretKey;
    }

  @Override
  public int hashCode()
    {
    final int prime = 31;
    int result = 1;
    result = prime * result + ( ( accessKey == null ) ? 0 : accessKey.hashCode() );
    result = prime * result + ( ( secretKey == null ) ? 0 : secretKey.hashCode() );
    return result;
    }

  @Override
  public boolean equals(Object obj)
    {
    if ( this == obj )
      return true;
    if ( obj == null )
      return false;
    if ( getClass() != obj.getClass() )
      return false;
    AWSCredentials other = (AWSCredentials) obj;
    if ( accessKey == null )
      {
      if ( other.accessKey != null )
        return false;
      }
    else if ( !accessKey.equals( other.accessKey ) )
      return false;
    if ( secretKey == null )
      {
      if ( other.secretKey != null )
        return false;
      }
    else if ( !secretKey.equals( other.secretKey ) )
      return false;
    return true;
    }

  }
