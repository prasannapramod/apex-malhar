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

package org.apache.apex.examples.transform;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.partitioner.StatelessPartitioner;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.transform.TransformOperator;

@ApplicationAnnotation(name = "TransformExample")
public class Application implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    POJOGenerator input = dag.addOperator("Input", new POJOGenerator());
    TransformOperator transform = dag.addOperator("Process", new TransformOperator());
    // Set expression map
    Map<String, String> expMap = new HashMap<>();
    expMap.put("name", "{$.firstName}.concat(\" \").concat({$.lastName})");
    expMap.put("age", "(new java.util.Date()).getYear() - {$.dateOfBirth}.getYear()");
    expMap.put("address", "{$.address}.toLowerCase()");
    transform.setExpressionMap(expMap);
    ConsoleOutputOperator output = dag.addOperator("Output", new ConsoleOutputOperator());

    dag.addStream("InputToTransform", input.output, transform.input);
    dag.addStream("TransformToOutput", transform.output, output.input);

    dag.setInputPortAttribute(transform.input, Context.PortContext.TUPLE_CLASS, CustomerEvent.class);
    dag.setOutputPortAttribute(transform.output, Context.PortContext.TUPLE_CLASS, CustomerInfo.class);
    dag.setAttribute(transform, Context.OperatorContext.PARTITIONER, new StatelessPartitioner<TransformOperator>(2));
  }
}
