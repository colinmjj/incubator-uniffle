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

package com.tencent.rss.test;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import com.tencent.rss.client.impl.ShuffleReadClientImpl;
import com.tencent.rss.client.impl.grpc.ShuffleServerGrpcClient;
import com.tencent.rss.client.impl.grpc.ShuffleServerGrpcNettyClient;
import com.tencent.rss.client.response.CompressedShuffleBlock;
import com.tencent.rss.coordinator.CoordinatorConf;
import com.tencent.rss.server.ShuffleServerConf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ShuffleServerWithNettyTest extends ShuffleReadWriteBase {

  private ShuffleServerGrpcClient shuffleServerClient;

  @BeforeClass
  public static void setupServers() throws Exception {
    CoordinatorConf coordinatorConf = getCoordinatorConf();
    createCoordinatorServer(coordinatorConf);
    ShuffleServerConf shuffleServerConf = getShuffleServerConf();
    shuffleServerConf.setBoolean(ShuffleServerConf.SERVER_UPLOAD_ENABLE, true);
    shuffleServerConf.setBoolean(ShuffleServerConf.SERVER_UPLOAD_EPOLL_ENABLE, false);
    createShuffleServer(shuffleServerConf);
    startServers();
  }

  @Before
  public void createClient() {
    shuffleServerClient = new ShuffleServerGrpcNettyClient(
        LOCALHOST, SHUFFLE_SERVER_GRPC_PORT, SHUFFLE_SERVER_NETTY_PORT);
  }

  @After
  public void closeClient() {
    shuffleServerClient.close();
  }

    //  @Test
    //  public void uploadDataWithNettyTest() throws Exception {
    //    String appId = "app_hdfs_read_write_with_netty";
    //    String dataBasePath = HDFS_URI + "rss/test";
    //    RssRegisterShuffleRequest rrsr = new RssRegisterShuffleRequest(appId, 0, 0, 1);
    //    shuffleServerClient.registerShuffle(rrsr);
    //    rrsr = new RssRegisterShuffleRequest(appId, 0, 2, 3);
    //    shuffleServerClient.registerShuffle(rrsr);
    //
    //    Map<Long, byte[]> expectedData = Maps.newHashMap();
    //    Set<Long> expectedBlockIds1 = Sets.newHashSet();
    //    Set<Long> expectedBlockIds2 = Sets.newHashSet();
    //    Set<Long> expectedBlockIds3 = Sets.newHashSet();
    //    Set<Long> expectedBlockIds4 = Sets.newHashSet();
    //    List<ShuffleBlockInfo> blocks1 = createShuffleBlockList(
    //        0, 0, 3, 25, expectedBlockIds1, expectedData);
    //    List<ShuffleBlockInfo> blocks2 = createShuffleBlockList(
    //        0, 1, 5, 25, expectedBlockIds2, expectedData);
    //    List<ShuffleBlockInfo> blocks3 = createShuffleBlockList(
    //        0, 2, 4, 25, expectedBlockIds3, expectedData);
    //    List<ShuffleBlockInfo> blocks4 = createShuffleBlockList(
    //        0, 3, 1, 25, expectedBlockIds4, expectedData);
    //    Map<Integer, List<ShuffleBlockInfo>> partitionToBlocks = Maps.newHashMap();
    //    partitionToBlocks.put(0, blocks1);
    //    partitionToBlocks.put(1, blocks2);
    //
    //    Map<Integer, Map<Integer, List<ShuffleBlockInfo>>> shuffleToBlocks = Maps.newHashMap();
    //    shuffleToBlocks.put(0, partitionToBlocks);
    //
    //    RssSendShuffleDataRequest rssdr = new RssSendShuffleDataRequest(appId, 3, 1000, 10000, shuffleToBlocks);
    //    shuffleServerClient.sendShuffleData(rssdr);
    //    assertEquals(200, shuffleServers.get(0).getShuffleBufferManager().getUsedMemory());
    //    assertEquals(0, shuffleServers.get(0).getShuffleBufferManager().getPreAllocatedSize());
    //    RssSendCommitRequest rscr = new RssSendCommitRequest(appId, 0);
    //    shuffleServerClient.sendCommit(rscr);
    //    RssFinishShuffleRequest rfsr = new RssFinishShuffleRequest(appId, 0);
    //    try {
    //      // before call finishShuffle(), the data won't be flushed to hdfs
    //      new ShuffleReadClientImpl(StorageType.HDFS.name(),
    //          appId, 0, 0, 100, 2, 10, 1000, dataBasePath, expectedBlockIds1, Lists.newArrayList());
    //      fail("Expected exception");
    //    } catch (Exception e) {
    //      // ignore
    //    }
    //    shuffleServerClient.finishShuffle(rfsr);
    //
    //    partitionToBlocks.clear();
    //    partitionToBlocks.put(2, blocks3);
    //    shuffleToBlocks.clear();
    //    shuffleToBlocks.put(0, partitionToBlocks);
    //    rssdr = new RssSendShuffleDataRequest(appId, 3, 1000, shuffleToBlocks);
    //    shuffleServerClient.sendShuffleData(rssdr);
    //    assertEquals(0, shuffleServers.get(0).getShuffleBufferManager().getPreAllocatedSize());
    //    rscr = new RssSendCommitRequest(appId, 0);
    //    shuffleServerClient.sendCommit(rscr);
    //    rfsr = new RssFinishShuffleRequest(appId, 0);
    //    shuffleServerClient.finishShuffle(rfsr);
    //
    //    partitionToBlocks.clear();
    //    partitionToBlocks.put(3, blocks4);
    //    shuffleToBlocks.clear();
    //    shuffleToBlocks.put(0, partitionToBlocks);
    //    rssdr = new RssSendShuffleDataRequest(appId, 3, 1000, shuffleToBlocks);
    //    shuffleServerClient.sendShuffleData(rssdr);
    //    rscr = new RssSendCommitRequest(appId, 0);
    //    shuffleServerClient.sendCommit(rscr);
    //    rfsr = new RssFinishShuffleRequest(appId, 0);
    //    shuffleServerClient.finishShuffle(rfsr);

    //    ShuffleReadClientImpl readClient = new ShuffleReadClientImpl(StorageType.HDFS.name(),
    //        appId, 0, 0, 100, 2, 10, 1000, dataBasePath, expectedBlockIds1, Lists.newArrayList());
    //    validateResult(readClient, expectedData, expectedBlockIds1);
    //
    //    readClient = new ShuffleReadClientImpl(StorageType.HDFS.name(),
    //        appId, 0, 1, 100, 2, 10, 1000, dataBasePath, expectedBlockIds2, Lists.newArrayList());
    //    validateResult(readClient, expectedData, expectedBlockIds2);
    //
    //    readClient = new ShuffleReadClientImpl(StorageType.HDFS.name(),
    //        appId, 0, 2, 100, 2, 10, 1000, dataBasePath, expectedBlockIds3, Lists.newArrayList());
    //    validateResult(readClient, expectedData, expectedBlockIds3);
    //
    //    readClient = new ShuffleReadClientImpl(StorageType.HDFS.name(),
    //        appId, 0, 3, 100, 2, 10, 1000, dataBasePath, expectedBlockIds4, Lists.newArrayList());
    //    validateResult(readClient, expectedData, expectedBlockIds4);
    //  }

  protected void validateResult(ShuffleReadClientImpl readClient, Map<Long, byte[]> expectedData,
      Set<Long> expectedBlockIds) {
    CompressedShuffleBlock csb = readClient.readShuffleBlockData();
    Set<Long> matched = Sets.newHashSet();
    while (csb.getByteBuffer() != null) {
      for (Entry<Long, byte[]> entry : expectedData.entrySet()) {
        if (compareByte(entry.getValue(), csb.getByteBuffer())) {
          matched.add(entry.getKey());
          break;
        }
      }
      csb = readClient.readShuffleBlockData();
    }
    assertEquals(expectedBlockIds.size(), matched.size());
    assertTrue(expectedBlockIds.containsAll(matched));
  }
}
