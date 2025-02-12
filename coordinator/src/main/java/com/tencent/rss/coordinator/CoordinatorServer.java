/*
 * Tencent is pleased to support the open source community by making
 * Firestorm-Spark remote shuffle server available. 
 *
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved. 
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.tencent.rss.coordinator;

import com.tencent.rss.common.Arguments;
import com.tencent.rss.common.metrics.JvmMetrics;
import com.tencent.rss.common.rpc.ServerInterface;
import com.tencent.rss.common.util.ExitUtils;
import com.tencent.rss.common.web.CommonMetricsServlet;
import com.tencent.rss.common.web.JettyServer;
import io.prometheus.client.CollectorRegistry;
import java.io.FileNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

/**
 * The main entrance of coordinator service
 */
public class CoordinatorServer {

  private static final Logger LOG = LoggerFactory.getLogger(CoordinatorServer.class);

  private final CoordinatorConf coordinatorConf;
  private JettyServer jettyServer;
  private ServerInterface server;
  private ClusterManager clusterManager;
  private AssignmentStrategy assignmentStrategy;
  private ApplicationManager applicationManager;

  public CoordinatorServer(CoordinatorConf coordinatorConf) throws FileNotFoundException {
    this.coordinatorConf = coordinatorConf;
    initialization();

  }

  public static void main(String[] args) throws Exception {
    Arguments arguments = new Arguments();
    CommandLine commandLine = new CommandLine(arguments);
    commandLine.parseArgs(args);
    String configFile = arguments.getConfigFile();
    LOG.info("Start to init coordinator server using config {}", configFile);

    // Load configuration from config files
    final CoordinatorConf coordinatorConf = new CoordinatorConf(configFile);

    // Start the coordinator service
    final CoordinatorServer coordinatorServer = new CoordinatorServer(coordinatorConf);

    coordinatorServer.start();
    coordinatorServer.blockUntilShutdown();
  }

  public void start() throws Exception {
    jettyServer.start();
    server.start();

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        LOG.info("*** shutting down gRPC server since JVM is shutting down");
        try {
          stopServer();
        } catch (Exception e) {
          LOG.error(e.getMessage());
        }
        LOG.info("*** server shut down");
      }
    });
  }

  public void stopServer() throws Exception {
    if (jettyServer != null) {
      jettyServer.stop();
    }
    if (clusterManager != null) {
      clusterManager.shutdown();
    }
    server.stop();
  }

  private void initialization() throws FileNotFoundException {
    this.applicationManager = new ApplicationManager(coordinatorConf);

    ClusterManagerFactory clusterManagerFactory = new ClusterManagerFactory(coordinatorConf);
    this.clusterManager = clusterManagerFactory.getClusterManager();

    AssignmentStrategyFactory assignmentStrategyFactory =
        new AssignmentStrategyFactory(coordinatorConf, clusterManager);
    this.assignmentStrategy = assignmentStrategyFactory.getAssignmentStrategy();

    CoordinatorRpcServerFactory coordinatorRpcServerFactory = new CoordinatorRpcServerFactory(this);
    server = coordinatorRpcServerFactory.getServer();
    jettyServer = new JettyServer(coordinatorConf);

    registerMetrics();
    addServlet(jettyServer);
  }

  private void registerMetrics() {
    LOG.info("Register metrics");
    CollectorRegistry coordinatorCollectorRegistry = new CollectorRegistry(true);
    CollectorRegistry jvmCollectorRegistry = new CollectorRegistry(true);
    CoordinatorMetrics.register(coordinatorCollectorRegistry);
    boolean verbose = coordinatorConf.getBoolean(CoordinatorConf.RSS_JVM_METRICS_VERBOSE_ENABLE);
    JvmMetrics.register(jvmCollectorRegistry, verbose);
  }

  private void addServlet(JettyServer jettyServer) {
    LOG.info("Add metrics servlet");
    jettyServer.addServlet(
        new CommonMetricsServlet(CoordinatorMetrics.getCollectorRegistry()),
        "/metrics/server");
    jettyServer.addServlet(
        new CommonMetricsServlet(JvmMetrics.getCollectorRegistry()),
        "/metrics/jvm");
    jettyServer.addServlet(
        new CommonMetricsServlet(CoordinatorMetrics.getCollectorRegistry(), true),
        "/prometheus/metrics/server");
    jettyServer.addServlet(
        new CommonMetricsServlet(JvmMetrics.getCollectorRegistry(), true),
        "/prometheus/metrics/jvm");
  }

  public ClusterManager getClusterManager() {
    return clusterManager;
  }

  public AssignmentStrategy getAssignmentStrategy() {
    return assignmentStrategy;
  }

  public CoordinatorConf getCoordinatorConf() {
    return coordinatorConf;
  }

  public ApplicationManager getApplicationManager() {
    return applicationManager;
  }

  /**
   * Await termination on the main thread since the grpc library uses daemon threads.
   */
  private void blockUntilShutdown() throws InterruptedException {
    server.blockUntilShutdown();
  }
}
