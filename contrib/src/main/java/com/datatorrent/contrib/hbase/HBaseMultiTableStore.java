/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.contrib.hbase;

import java.io.IOException;

import javax.validation.constraints.Min;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.client.HTable;

import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

/**
 * Created by lakshmi on 6/25/17.
 */
public class HBaseMultiTableStore extends HBaseWindowStore
{
  /**
   * The tableName and table from HBaseWindowStore will be used as the meta table to store
   * recovery information
   */
  
  private static final Logger logger = LoggerFactory.getLogger(HBaseMultiTableStore.class);

  protected String[] allowedTableNames;
  
  protected transient LoadingCache<String, HTable> tableCache;

  @Min(1)
  protected int maxOpenTables = Integer.MAX_VALUE;
  
  /**
   * Get the HBase table for the given table name.
   *
   * @param tableName The name of the table
   *                  
   * @return The HBase table
   * @omitFromUI
   */
  public HTable getTable(String tableName) {
    try {
      return tableCache.get(tableName);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public void setAllowedTableNames(String[] allowedTableNames)
  {
    this.allowedTableNames = allowedTableNames;
  }

  @Override
  public void connect() throws IOException
  {
    super.connect();
    CacheLoader<String, HTable> cacheLoader = new CacheLoader<String, HTable>()
    {
      @Override
      public HTable load(String key) throws Exception
      {
        return loadTable(key);
      }
    };
    RemovalListener<String, HTable> removalListener = new RemovalListener<String, HTable>()
    {
      @Override
      public void onRemoval(RemovalNotification<String, HTable> notification)
      {
        unloadTable(notification.getValue());
      }
    };
    tableCache = CacheBuilder.<String, HTable>newBuilder().maximumSize(maxOpenTables).removalListener(removalListener).build(cacheLoader);
  }
  
  protected HTable loadTable(String tableName) throws IOException
  {
    if ((allowedTableNames != null) && !ArrayUtils.contains(allowedTableNames, tableName)) {
      return null;
    }
    return connectTable(tableName);
  }
  
  protected void unloadTable(HTable table)
  {
    try {
      table.close();
    } catch (IOException e) {
      logger.warn("Could not close table", e);
    }
  }

  public int getMaxOpenTables()
  {
    return maxOpenTables;
  }

  public void setMaxOpenTables(int maxOpenTables)
  {
    this.maxOpenTables = maxOpenTables;
  }

}
