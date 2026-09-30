/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.internal.config

private[spark] object UDF {

  object Dispatcher extends Enumeration {
    val DIRECT = Value
  }

  val DISPATCHER = ConfigBuilder("spark.udf.worker.dispatcher")
    .doc("Built-in dispatcher used to provision and manage external UDF workers " +
      "(SPARK-55278). DIRECT launches local worker processes and communicates with them " +
      "through gRPC over Unix domain sockets. Worker-specific launch and provisioning " +
      "details belong in UDFWorkerSpecification instead of a custom dispatcher. This is " +
      "an operator-level setting and is not intended to be changed by individual queries.")
    .version("4.4.0")
    .withBindingPolicy(ConfigBindingPolicy.NOT_APPLICABLE)
    .enumConf(Dispatcher)
    .createWithDefault(Dispatcher.DIRECT)
}
