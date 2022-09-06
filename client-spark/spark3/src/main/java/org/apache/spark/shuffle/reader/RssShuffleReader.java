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

package org.apache.spark.shuffle.reader;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.InterruptibleIterator;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.TaskContext;
import org.apache.spark.executor.ShuffleReadMetrics;
import org.apache.spark.serializer.Serializer;
import org.apache.spark.shuffle.RssShuffleHandle;
import org.apache.spark.shuffle.ShuffleReader;
import org.apache.spark.util.CompletionIterator$;
import org.apache.spark.util.collection.ExternalSorter;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function0;
import scala.Function1;
import scala.Option;
import scala.Product2;
import scala.collection.AbstractIterator;
import scala.collection.Iterator;
import scala.runtime.AbstractFunction0;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

import com.tencent.rss.client.api.ShuffleReadClient;
import com.tencent.rss.client.factory.ShuffleClientFactory;
import com.tencent.rss.client.request.CreateShuffleReadClientRequest;
import com.tencent.rss.common.ShuffleServerInfo;

public class RssShuffleReader<K, C> implements ShuffleReader<K, C> {
  private static final Logger LOG = LoggerFactory.getLogger(RssShuffleReader.class);
  private final Map<Integer, List<ShuffleServerInfo>> partitionToShuffleServers;

  private String appId;
  private int shuffleId;
  private int startPartition;
  private int endPartition;
  private TaskContext context;
  private ShuffleDependency<K, C, ?> shuffleDependency;
  private Serializer serializer;
  private String taskId;
  private String basePath;
  private int indexReadLimit;
  private int readBufferSize;
  private int partitionNum;
  private String storageType;
  private String clientType;
  private Map<Integer, Roaring64NavigableMap> partitionToExpectBlocks;
  private Roaring64NavigableMap taskIdBitmap;
  private Configuration hadoopConf;
  private int mapStartIndex;
  private int mapEndIndex;
  private ShuffleReadMetrics readMetrics;

  public RssShuffleReader(
      int startPartition,
      int endPartition,
      int mapStartIndex,
      int mapEndIndex,
      TaskContext context,
      RssShuffleHandle rssShuffleHandle,
      String basePath,
      int indexReadLimit,
      Configuration hadoopConf,
      String storageType,
      String clientType,
      int readBufferSize,
      int partitionNum,
      Map<Integer, Roaring64NavigableMap> partitionToExpectBlocks,
      Roaring64NavigableMap taskIdBitmap,
      ShuffleReadMetrics readMetrics) {
    this.appId = rssShuffleHandle.getAppId();
    this.startPartition = startPartition;
    this.endPartition = endPartition;
    this.mapStartIndex = mapStartIndex;
    this.mapEndIndex = mapEndIndex;
    this.context = context;
    this.shuffleDependency = rssShuffleHandle.getDependency();
    this.shuffleId = shuffleDependency.shuffleId();
    this.serializer = rssShuffleHandle.getDependency().serializer();
    this.taskId = "" + context.taskAttemptId() + "_" + context.attemptNumber();
    this.basePath = basePath;
    this.indexReadLimit = indexReadLimit;
    this.storageType = storageType;
    this.clientType = clientType;
    this.readBufferSize = readBufferSize;
    this.partitionNum = partitionNum;
    this.partitionToExpectBlocks = partitionToExpectBlocks;
    this.taskIdBitmap = taskIdBitmap;
    this.hadoopConf = hadoopConf;
    this.readMetrics = readMetrics;
    this.partitionToShuffleServers = rssShuffleHandle.getPartitionToServers();
  }

  @Override
  public Iterator<Product2<K, C>> read() {
    LOG.info("Shuffle read started:" + getReadInfo());

    Iterator<Product2<K, C>> aggrIter = null;
    Iterator<Product2<K, C>> resultIter = null;
    MultiPartitionIterator rssShuffleDataIterator = new MultiPartitionIterator<K, C>();

    if (shuffleDependency.aggregator().isDefined()) {
      if (shuffleDependency.mapSideCombine()) {
        aggrIter = shuffleDependency.aggregator().get().combineCombinersByKey(
            rssShuffleDataIterator, context);
      } else {
        aggrIter = shuffleDependency.aggregator().get().combineValuesByKey(rssShuffleDataIterator, context);
      }
    } else {
      aggrIter = rssShuffleDataIterator;
    }

    if (shuffleDependency.keyOrdering().isDefined()) {
      // Create an ExternalSorter to sort the data
      ExternalSorter sorter = new ExternalSorter<K, C, C>(context, Option.empty(), Option.empty(),
          shuffleDependency.keyOrdering(), serializer);
      LOG.info("Inserting aggregated records to sorter");
      long startTime = System.currentTimeMillis();
      sorter.insertAll(aggrIter);
      LOG.info("Inserted aggregated records to sorter: millis:" + (System.currentTimeMillis() - startTime));
      context.taskMetrics().incMemoryBytesSpilled(sorter.memoryBytesSpilled());
      context.taskMetrics().incPeakExecutionMemory(sorter.peakMemoryUsedBytes());
      context.taskMetrics().incDiskBytesSpilled(sorter.diskBytesSpilled());
      Function0<BoxedUnit> fn0 = new AbstractFunction0<BoxedUnit>() {
        @Override
        public BoxedUnit apply() {
          sorter.stop();
          return BoxedUnit.UNIT;
        }
      };
      Function1<TaskContext, Void> fn1 = new AbstractFunction1<TaskContext, Void>() {
        public Void apply(TaskContext context) {
          sorter.stop();
          return (Void) null;
        }
      };
      context.addTaskCompletionListener(fn1);
      resultIter = CompletionIterator$.MODULE$.apply(sorter.iterator(), fn0);
    } else {
      resultIter = aggrIter;
    }

    if (!(resultIter instanceof InterruptibleIterator)) {
      resultIter = new InterruptibleIterator<>(context, resultIter);
    }
    return resultIter;
  }

  private String getReadInfo() {
    return "appId=" + appId
        + ", shuffleId=" + shuffleId
        + ",taskId=" + taskId
        + ", partitions: [" + startPartition
        + ", " + endPartition + ")"
        + ", maps: [" + mapStartIndex
        + ", " + mapEndIndex + ")";
  }

  class MultiPartitionIterator<K, C> extends AbstractIterator<Product2<K, C>> {
    java.util.Iterator<RssShuffleDataIterator> iterator;
    RssShuffleDataIterator dataIterator;

    MultiPartitionIterator() {
      List<RssShuffleDataIterator> iterators = Lists.newArrayList();
      for (int partition = startPartition; partition < endPartition; partition++) {
        if (partitionToExpectBlocks.get(partition).isEmpty()) {
          LOG.info("{} partition is empty partition", partition);
          continue;
        }
        List<ShuffleServerInfo> shuffleServerInfoList = partitionToShuffleServers.get(partition);
        CreateShuffleReadClientRequest request = new CreateShuffleReadClientRequest(
            appId, shuffleId, partition, storageType, clientType, basePath, indexReadLimit, readBufferSize,
            1, partitionNum, partitionToExpectBlocks.get(partition), taskIdBitmap, shuffleServerInfoList, hadoopConf);
        ShuffleReadClient shuffleReadClient = ShuffleClientFactory.getInstance().createShuffleReadClient(request);
        RssShuffleDataIterator iterator = new RssShuffleDataIterator<K, C>(
            shuffleDependency.serializer(), shuffleReadClient,
            readMetrics);
        iterators.add(iterator);
      }
      iterator = iterators.iterator();
      if (iterator.hasNext()) {
        dataIterator = iterator.next();
      }
    }

    @Override
    public boolean hasNext() {
      if (dataIterator == null) {
        return false;
      }
      while (!dataIterator.hasNext()) {
        if (!iterator.hasNext()) {
          return false;
        }
        dataIterator = iterator.next();
      }
      return dataIterator.hasNext();
    }

    @Override
    public Product2<K, C> next() {
      Product2<K, C> result = dataIterator.next();
      return result;
    }
  }

}
