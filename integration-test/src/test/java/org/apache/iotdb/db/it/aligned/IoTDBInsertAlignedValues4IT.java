/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.it.aligned;

import org.apache.iotdb.it.env.ConfigFactory;
import org.apache.iotdb.it.env.EnvFactory;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.fail;

public class IoTDBInsertAlignedValues4IT {
  private boolean autoCreateSchemaEnabled;
  private int primitiveArraySize;

  @Before
  public void setUp() throws Exception {
    autoCreateSchemaEnabled = ConfigFactory.getConfig().isAutoCreateSchemaEnabled();
    primitiveArraySize = ConfigFactory.getConfig().getPrimitiveArraySize();
    ConfigFactory.getConfig().setAutoCreateSchemaEnabled(true);
    ConfigFactory.getConfig().setPrimitiveArraySize(2);
    EnvFactory.getEnv().initBeforeClass();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterClass();
    ConfigFactory.getConfig().setAutoCreateSchemaEnabled(autoCreateSchemaEnabled);
    ConfigFactory.getConfig().setPrimitiveArraySize(primitiveArraySize);
  }

  @Test
  public void testExtendTextColumn() {

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("insert into root.sg.d1(time,s1,s2) aligned values(1,'test','test')");
      statement.execute("insert into root.sg.d1(time,s1,s2) aligned values(2,'test','test')");
      statement.execute("insert into root.sg.d1(time,s1,s2) aligned values(3,'test','test')");
      statement.execute("insert into root.sg.d1(time,s1,s2) aligned values(4,'test','test')");
      statement.execute("insert into root.sg.d1(time,s1,s3) aligned values(5,'test','test')");
      statement.execute("insert into root.sg.d1(time,s1,s2) aligned values(6,'test','test')");
      statement.execute(ConfigFactory.getConfig().getFlushCommand());
      statement.execute("insert into root.sg.d1(time,s1,s3) aligned values(7,'test','test')");
    } catch (SQLException e) {
      fail();
    }
  }
}
