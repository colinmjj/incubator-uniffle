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

package com.tencent.rss.server;

import com.tencent.rss.common.config.ConfigOption;
import com.tencent.rss.common.config.ConfigOptions;
import com.tencent.rss.common.config.ConfigUtils;
import com.tencent.rss.common.config.RssBaseConf;
import com.tencent.rss.common.util.RssUtils;
import java.util.List;
import java.util.Map;

public class ShuffleServerConf extends RssBaseConf {

  public static final String PREFIX_HADOOP_CONF = "rss.server.hadoop";

  public static final ConfigOption<Long> SERVER_BUFFER_CAPACITY = ConfigOptions
      .key("rss.server.buffer.capacity")
      .longType()
      .noDefaultValue()
      .withDescription("Max memory of buffer manager for shuffle server");

  public static final ConfigOption<Long> SERVER_BUFFER_SPILL_THRESHOLD = ConfigOptions
      .key("rss.server.buffer.spill.threshold")
      .longType()
      .noDefaultValue()
      .withDescription("Spill threshold for buffer manager, it must be less than rss.server.buffer.capacity");

  public static final ConfigOption<Integer> SERVER_PARTITION_BUFFER_SIZE = ConfigOptions
      .key("rss.server.partition.buffer.size")
      .intType()
      .noDefaultValue()
      .withDescription("Max size of each buffer in buffer manager");

  public static final ConfigOption<Long> SERVER_READ_BUFFER_CAPACITY = ConfigOptions
      .key("rss.server.read.buffer.capacity")
      .longType()
      .defaultValue(10000L)
      .withDescription("Max size of buffer for reading data");

  public static final ConfigOption<Long> SERVER_HEARTBEAT_DELAY = ConfigOptions
      .key("rss.server.heartbeat.delay")
      .longType()
      .defaultValue(10 * 1000L)
      .withDescription("rss heartbeat initial delay ms");

  public static final ConfigOption<Integer> SERVER_HEARTBEAT_THREAD_NUM = ConfigOptions
      .key("rss.server.heartbeat.threadNum")
      .intType()
      .defaultValue(2)
      .withDescription("rss heartbeat thread number");

  public static final ConfigOption<Long> SERVER_HEARTBEAT_INTERVAL = ConfigOptions
      .key("rss.server.heartbeat.interval")
      .longType()
      .defaultValue(10 * 1000L)
      .withDescription("Heartbeat interval to Coordinator (ms)");

  public static final ConfigOption<Long> SERVER_HEARTBEAT_TIMEOUT = ConfigOptions
      .key("rss.server.heartbeat.timeout")
      .longType()
      .defaultValue(10 * 1000L)
      .withDescription("rss heartbeat interval ms");

  public static final ConfigOption<Integer> SERVER_FLUSH_THREAD_POOL_SIZE = ConfigOptions
      .key("rss.server.flush.threadPool.size")
      .intType()
      .defaultValue(10)
      .withDescription("thread pool for flush data to file");

  public static final ConfigOption<Integer> SERVER_FLUSH_THREAD_POOL_QUEUE_SIZE = ConfigOptions
      .key("rss.server.flush.threadPool.queue.size")
      .intType()
      .defaultValue(Integer.MAX_VALUE)
      .withDescription("size of waiting queue for thread pool");

  public static final ConfigOption<Long> SERVER_FLUSH_THREAD_ALIVE = ConfigOptions
      .key("rss.server.flush.thread.alive")
      .longType()
      .defaultValue(120L)
      .withDescription("thread idle time in pool (s)");

  public static final ConfigOption<Long> SERVER_COMMIT_TIMEOUT = ConfigOptions
      .key("rss.server.commit.timeout")
      .longType()
      .defaultValue(600000L)
      .withDescription("Timeout when commit shuffle data (ms)");

  public static final ConfigOption<Integer> SERVER_WRITE_RETRY_MAX = ConfigOptions
      .key("rss.server.write.retry.max")
      .intType()
      .defaultValue(10)
      .withDescription("Retry times when write fail");

  public static final ConfigOption<Long> SERVER_APP_EXPIRED_WITHOUT_HEARTBEAT = ConfigOptions
      .key("rss.server.app.expired.withoutHeartbeat")
      .longType()
      .defaultValue(60 * 1000L)
      .withDescription("Expired time (ms) for application which has no heartbeat with coordinator");

  public static final ConfigOption<Integer> SERVER_MEMORY_REQUEST_RETRY_MAX = ConfigOptions
      .key("rss.server.memory.request.retry.max")
      .intType()
      .defaultValue(100)
      .withDescription("Max times to retry for memory request");

  public static final ConfigOption<Long> SERVER_PRE_ALLOCATION_EXPIRED = ConfigOptions
      .key("rss.server.preAllocation.expired")
      .longType()
      .defaultValue(10 * 1000L)
      .withDescription("Expired time (ms) for pre allocated buffer");

  public static final ConfigOption<Long> SERVER_COMMIT_CHECK_INTERVAL_MAX = ConfigOptions
      .key("rss.server.commit.check.interval.max.ms")
      .longType()
      .defaultValue(10000L)
      .withDescription("Max interval(ms) for check commit status");

  public static final ConfigOption<Long> SERVER_WRITE_SLOW_THRESHOLD = ConfigOptions
      .key("rss.server.write.slow.threshold")
      .longType()
      .defaultValue(10000L)
      .withDescription("Threshold for write slow defined");

  public static final ConfigOption<Long> SERVER_EVENT_SIZE_THRESHOLD_L1 = ConfigOptions
      .key("rss.server.event.size.threshold.l1")
      .longType()
      .defaultValue(200000L)
      .withDescription("Threshold for event size");

  public static final ConfigOption<Long> SERVER_EVENT_SIZE_THRESHOLD_L2 = ConfigOptions
      .key("rss.server.event.size.threshold.l2")
      .longType()
      .defaultValue(1000000L)
      .withDescription("Threshold for event size");

  public static final ConfigOption<Long> SERVER_EVENT_SIZE_THRESHOLD_L3 = ConfigOptions
      .key("rss.server.event.size.threshold.l3")
      .longType()
      .defaultValue(10000000L)
      .withDescription("Threshold for event size");

  public static final ConfigOption<Double> RSS_CLEANUP_THRESHOLD = ConfigOptions
      .key("rss.server.cleanup.threshold")
      .doubleType()
      .defaultValue(70.0)
      .withDescription("Threshold for disk cleanup");

  public static final ConfigOption<Double> RSS_HIGH_WATER_MARK_OF_WRITE = ConfigOptions
      .key("rss.server.high.watermark.write")
      .doubleType()
      .defaultValue(95.0)
      .withDescription("If disk usage is bigger than this value, disk cannot been written");

  public static final ConfigOption<Double> RSS_LOW_WATER_MARK_OF_WRITE = ConfigOptions
      .key("rss.server.low.watermark.write")
      .doubleType()
      .defaultValue(85.0)
      .withDescription("If disk usage is smaller than this value, disk can been written again");
  public static final ConfigOption<Long> RSS_PENDING_EVENT_TIMEOUT_SEC = ConfigOptions
      .key("rss.server.pending.event.timeoutSec")
      .longType()
      .defaultValue(60L)
      .withDescription("If disk cannot be written for timeout seconds, the flush data event will fail");

  public static final ConfigOption<Boolean> RSS_UPLOADER_ENABLE = ConfigOptions
      .key("rss.server.uploader.enable")
      .booleanType()
      .defaultValue(false)
      .withDescription("A switch of the uploader");

  public static final ConfigOption<Integer> RSS_UPLOADER_THREAD_NUM = ConfigOptions
      .key("rss.server.uploader.thread.number")
      .intType()
      .defaultValue(4)
      .withDescription("The thread number of the uploader");

  public static final ConfigOption<Long> RSS_UPLOADER_INTERVAL_MS = ConfigOptions
      .key("rss.server.uploader.interval.ms")
      .longType()
      .defaultValue(3000L)
      .withDescription("The interval for the uploader");

  public static final ConfigOption<Long> RSS_UPLOAD_COMBINE_THRESHOLD_MB = ConfigOptions
      .key("rss.server.uploader.combine.threshold.MB")
      .longType()
      .defaultValue(32L)
      .withDescription("The threshold of the combine mode");

  public static final ConfigOption<String> RSS_HDFS_BASE_PATH = ConfigOptions
      .key("rss.server.uploader.base.path")
      .stringType()
      .noDefaultValue()
      .withDescription("The base path of the uploader");

  public static final ConfigOption<String> RSS_UPLOAD_STORAGE_TYPE = ConfigOptions
      .key("rss.server.uploader.remote.storage.type")
      .stringType()
      .defaultValue("HDFS")
      .withDescription("The remote storage type of the uploader");

  public static final ConfigOption<Long> RSS_REFERENCE_UPLOAD_SPEED_MBS = ConfigOptions
      .key("rss.server.uploader.references.speed.mbps")
      .longType()
      .defaultValue(8L)
      .withDescription("The speed for the uploader");

  public static final ConfigOption<Long> RSS_DISK_CAPACITY = ConfigOptions
      .key("rss.server.disk.capacity")
      .longType()
      .defaultValue(1024L * 1024L * 1024L * 1024L)
      .withDescription("Disk capacity that shuffle server can use");

  public static final ConfigOption<Long> RSS_CLEANUP_INTERVAL_MS = ConfigOptions
      .key("rss.server.cleanup.interval.ms")
      .longType()
      .defaultValue(3000L)
      .withDescription("The interval for cleanup");

  public static final ConfigOption<Long> RSS_SHUFFLE_EXPIRED_TIMEOUT_MS = ConfigOptions
      .key("rss.server.shuffle.expired.timeout.ms")
      .longType()
      .defaultValue(60L * 1000 * 5)
      .withDescription("If the shuffle is not read for the long time, and shuffle is uploaded totally,"
          + " , we can delete the shuffle");

  public static final ConfigOption<Boolean> RSS_USE_MULTI_STORAGE = ConfigOptions
      .key("rss.server.use.multistorage")
      .booleanType()
      .defaultValue(false)
      .withDescription("The function switch for multiStorage");

  public static final ConfigOption<Long> RSS_SHUFFLE_MAX_UPLOAD_SIZE = ConfigOptions
      .key("rss.server.shuffle.max.upload.size")
      .longType()
      .defaultValue(1024L * 1024L * 1024L)
      .withDescription("The max value of upload shuffle size");

  public ShuffleServerConf() {
  }

  public ShuffleServerConf(String fileName) {
    super();
    boolean ret = loadConfFromFile(fileName);
    if (!ret) {
      throw new IllegalStateException("Fail to load config file " + fileName);
    }
  }

  public boolean loadConfFromFile(String fileName) {
    Map<String, String> properties = RssUtils.getPropertiesFromFile(fileName);

    if (properties == null) {
      return false;
    }

    loadCommonConf(properties);

    List<ConfigOption> configOptions = ConfigUtils.getAllConfigOptions(ShuffleServerConf.class);

    properties.forEach((k, v) -> {
      configOptions.forEach(config -> {
        if (config.key().equalsIgnoreCase(k)) {
          set(config, ConfigUtils.convertValue(v, config.getClazz()));
        }
      });

      if (k.startsWith(PREFIX_HADOOP_CONF)) {
        setString(k, v);
      }
    });

    return true;
  }
}
