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

package org.apache.apex.examples.enricher;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.enrich.JsonFSLoader;
import com.datatorrent.contrib.enrich.POJOEnricher;
import com.datatorrent.contrib.parser.JsonParser;
import com.datatorrent.lib.io.ConsoleOutputOperator;

@ApplicationAnnotation(name = "EnricherAppWithJSONFile")
public class EnricherAppWithJSONFile implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    DataGenerator dataGenerator = dag.addOperator("DataGenerator", DataGenerator.class);
    JsonParser parser = dag.addOperator("Parser", JsonParser.class);

    /**
     * FSLoader is used to configure Enricher backend. Property of FSLoader file which is fileName is set in
     * properties.xml file.
     * The format that is used to read the file is present as an example in resources/circleMapping.txt file.
     */
    JsonFSLoader fsLoader = new JsonFSLoader();
    POJOEnricher enrich = dag.addOperator("Enrich", POJOEnricher.class);
    enrich.setStore(fsLoader);

    ArrayList includeFields = new ArrayList();
    includeFields.add("circleName");
    ArrayList lookupFields = new ArrayList();
    lookupFields.add("circleId");

    enrich.setIncludeFields(includeFields);
    enrich.setLookupFields(lookupFields);

    ConsoleOutputOperator console = dag.addOperator("Console", ConsoleOutputOperator.class);

    dag.addStream("Parse", dataGenerator.output, parser.in);
    dag.addStream("Enrich", parser.out, enrich.input);
    dag.addStream("Console", enrich.output, console.input);
  }
}
