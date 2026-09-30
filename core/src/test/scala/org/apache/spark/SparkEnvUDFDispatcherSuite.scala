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
package org.apache.spark

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.Try

import org.mockito.Answers.RETURNS_DEEP_STUBS
import org.mockito.Mockito.{mock, verify}

import org.apache.spark.broadcast.BroadcastManager
import org.apache.spark.internal.config.UDF
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.scheduler.OutputCommitCoordinator
import org.apache.spark.storage.BlockManager
import org.apache.spark.udf.worker.UDFWorkerSpecification
import org.apache.spark.udf.worker.core.{UDFDispatcherFactory, UDFDispatcherManager,
  WorkerDispatcher, WorkerLogger}
import org.apache.spark.util.ThreadUtils

class SparkEnvUDFDispatcherSuite extends SparkFunSuite {

  private def spec = UDFWorkerSpecification.getDefaultInstance

  private def newEnv(
      conf: SparkConf,
      managerFactory: Option[() => UDFDispatcherManager] = None): SparkEnv = {
    new SparkEnv(
      SparkContext.DRIVER_IDENTIFIER,
      mock(classOf[RpcEnv]),
      null,
      null,
      null,
      mock(classOf[MapOutputTracker]),
      mock(classOf[BroadcastManager]),
      mock(classOf[BlockManager], RETURNS_DEEP_STUBS),
      null,
      mock(classOf[MetricsSystem]),
      mock(classOf[OutputCommitCoordinator]),
      conf) {
      override private[spark] def createUDFDispatcherManager(): UDFDispatcherManager = {
        managerFactory match {
          case Some(create) => create()
          case None => super.createUDFDispatcherManager()
        }
      }
    }
  }

  test("the DIRECT dispatcher is selected by default") {
    assert(new SparkConf(false).get(UDF.DISPATCHER) === UDF.Dispatcher.DIRECT)
  }

  test("dispatcher values are case-insensitive and arbitrary classes are rejected") {
    assert(new SparkConf(false).set(UDF.DISPATCHER.key, "direct").get(UDF.DISPATCHER) ===
      UDF.Dispatcher.DIRECT)

    val factoryClass = classOf[UDFDispatcherFactory].getName
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        new SparkConf(false).set(UDF.DISPATCHER.key, factoryClass).get(UDF.DISPATCHER)
      },
      condition = "INVALID_CONF_VALUE.OUT_OF_RANGE_OF_OPTIONS",
      parameters = Map(
        "confName" -> UDF.DISPATCHER.key,
        "confValue" -> factoryClass,
        "confOptions" -> UDF.Dispatcher.values.mkString(", ")))
  }

  test("stopping an unused SparkEnv does not load the default factory") {
    val env = newEnv(new SparkConf(false))
    env.stop()
    val error = intercept[IllegalStateException] {
      env.getExternalUDFDispatcher(spec)
    }
    assert(error.getMessage === "SparkEnv is stopped")
  }

  test("stop waits for lazy dispatcher manager creation and closes created dispatchers") {
    val entered = new CountDownLatch(1)
    val release = new CountDownLatch(1)
    val dispatcher = mock(classOf[WorkerDispatcher])
    val dispatcherCreated = new AtomicBoolean(false)
    val factory = new UDFDispatcherFactory {
      override def createDispatcher(
          workerSpec: UDFWorkerSpecification,
          logger: WorkerLogger): WorkerDispatcher = {
        dispatcherCreated.set(true)
        dispatcher
      }
    }
    val env = newEnv(new SparkConf(false), Some(() => {
      entered.countDown()
      if (!release.await(30, TimeUnit.SECONDS)) {
        throw new IllegalStateException("Timed out waiting to create the test dispatcher manager")
      }
      new UDFDispatcherManager(factory, WorkerLogger.NoOp)
    }))
    val pool = ThreadUtils.newDaemonFixedThreadPool(2, "udf-dispatcher-test")
    implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutor(pool)
    val creator = Future { Try(env.getExternalUDFDispatcher(spec)) }
    try {
      assert(entered.await(10, TimeUnit.SECONDS))
      val stopStarted = new CountDownLatch(1)
      val stopFinished = new CountDownLatch(1)
      val stopper = Future {
        stopStarted.countDown()
        env.stop()
        stopFinished.countDown()
      }
      assert(stopStarted.await(10, TimeUnit.SECONDS))
      assert(!stopFinished.await(100, TimeUnit.MILLISECONDS))
      release.countDown()
      val result = ThreadUtils.awaitResult(creator, 30.seconds)
      ThreadUtils.awaitResult(stopper, 30.seconds)
      assert(result.isSuccess || result.failed.get.isInstanceOf[IllegalStateException])
      if (dispatcherCreated.get()) {
        verify(dispatcher).close()
      }
      intercept[IllegalStateException] {
        env.getExternalUDFDispatcher(spec)
      }
    } finally {
      release.countDown()
      pool.shutdownNow()
    }
  }
}
