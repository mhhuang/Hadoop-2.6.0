/**
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.hadoop.yarn.api.protocolrecords.*;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.*;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.Plan;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.*;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.PlannerPage.PlanBlock;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.ClusterMetricsInfo;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.UTCClock;
import org.apache.hadoop.yarn.webapp.YarnWebParams;
import org.apache.hadoop.yarn.webapp.test.WebAppTests;
import org.junit.*;
import org.mockito.Mockito;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.Map;

/**
 * This tests the PlannerPage and the DataPage
 */
public class TestPlannerPage {
  MockRM rm;
  CapacitySchedulerConfiguration conf;
  Injector injector;
  ClientRMService clientService;

  final String defaultQ = "default";
  final String default2Q = "def2";
  final String dedicatedQ = "dedicated";
  final String reservationQ = "dedicated";

  final String NODATA = "{\"config\":{\"min\":\"0\",\"max\":\"0\",\"step\":\"0\",\"user\":\"\"},\"graph\":[]}";

  @Before
  public void initTest() {
    conf = new CapacitySchedulerConfiguration();
    queueConfiguration(conf);
    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
        ResourceScheduler.class);
    conf.setBoolean(YarnConfiguration.RM_RESERVATION_SYSTEM_ENABLE, true);
    conf.addResource(conf);
    rm = new MockRM(conf);
    rm.start();
    injector = WebAppTests.createMockInjector(RMContext.class, rm.getRMContext(),
        new Module() {
          @Override
          public void configure(Binder binder) {
            try {
              binder.bind(ResourceManager.class).toInstance(rm);
            } catch (Exception e) {
              throw new IllegalStateException(e);
            }
          }
        });
  }

  @After
  public void endTest() {
    rm.stop();
    injector = null;
    conf = null;
    rm = null;
  }

  @Test
  public void testOption() {
    PlanBlock planBlock = injector.getInstance(PlanBlock.class);
    planBlock.render();
    PrintWriter writer = injector.getInstance(PrintWriter.class);
    WebAppTests.flushOutput(injector);

    // "no_data" option
    int numberOfReservableQueue = 1;
    numberOfReservableQueue += rm.getRMContext().getReservationSystem().getAllPlans().size();
    Mockito.verify(writer, Mockito.times(numberOfReservableQueue)).print("<option");
  }

  @Test
  public void testDataPageNoData() {
    DataPage dataPage = injector.getInstance(DataPage.class);
    dataPage.set(YarnWebParams.QUEUE_NAME, dedicatedQ);
    dataPage.render();
    PrintWriter writer = injector.getInstance(PrintWriter.class);
    Mockito.verify(writer).write(NODATA);
  }

  @Test
  public void testDataPageWrongUser() {
    addReservation();
    DataPage dataPage = injector.getInstance(DataPage.class);
    dataPage.set(YarnWebParams.QUEUE_NAME, "wrongUser");
    dataPage.render();
    PrintWriter writer = injector.getInstance(PrintWriter.class);
    Mockito.verify(writer).write(NODATA);
    WebAppTests.flushOutput(injector);
  }

  @Test
  public void testDataPage() {
    ReservationId rId = addReservation();
    DataPage dataPage = injector.getInstance(DataPage.class);
    dataPage.set(YarnWebParams.QUEUE_NAME, dedicatedQ);
    dataPage.render();
    PrintWriter writer = injector.getInstance(PrintWriter.class);
    String json = createJson(rId);
    Mockito.verify(writer).write(json);
    WebAppTests.flushOutput(injector);
  }

  private void queueConfiguration(CapacitySchedulerConfiguration conf) {
    conf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] {defaultQ, dedicatedQ, default2Q});

    final String defQ = CapacitySchedulerConfiguration.ROOT +
        CapacitySchedulerConfiguration.DOT +
        defaultQ;
    conf.setCapacity(defQ, 20);

    final String dedicated = CapacitySchedulerConfiguration.ROOT +
        CapacitySchedulerConfiguration.DOT +
        dedicatedQ;
    conf.setCapacity(dedicated, 50);
    conf.setReservable(dedicated, true);

    final String def2Q = CapacitySchedulerConfiguration.ROOT +
        CapacitySchedulerConfiguration.DOT +
        default2Q;
    conf.setCapacity(def2Q, 30);
  }

  private String createJson(ReservationId rId) {
    Map<String, Plan> plans = rm.getRMContext().getReservationSystem().getAllPlans();
    Plan p = plans.get(reservationQ);
    ReservationAllocation r = p.getReservationById(rId);
    QueueMetrics qm = rm.getResourceScheduler().getRootQueueMetrics();
    int maxResource = (qm.getAvailableMB() + qm.getAllocatedMB()) / 1024;
    return "{" +
        "\"config\":{" +
          "\"min\":" + p.getEarliestStartTime() + "," +
          "\"max\":" + p.getLastEndTime() + "," +
          "\"step\":" + p.getStep() + "," +
          "\"resources\":[" +
            "\"Memory\"," +
            "\"CPU\"]," +
          "\"resource\":\"Memory\"," +
          "\"maxResource\":" + maxResource + "}," +
        "\"graph\":[{" +
          "\"owner\":\"" + r.getUser() + "\"," +
          "\"key\":\"" + r.getReservationId().toString() + "\"," +
          "\"values\":[{" +
            "\"date\":" + p.getEarliestStartTime() + "," +
            "\"value\":" + (r.getResourcesAtTime(p.getEarliestStartTime()).getMemory() / 1024) + "},{" +
            "\"date\":" + p.getLastEndTime() + "," +
            "\"value\":0}" +
        "]}]}";
  }

  private ReservationId addReservation() {
    try {
      rm.registerNode("127.0.0.1:8088", 102400, 100);
      Thread.sleep(1050);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }

    clientService = rm.getClientRMService();

    Clock clock = new UTCClock();
    long arrival = clock.getTime();
    long duration = 60000;
    long deadline = (long) (arrival + 1.05 * duration);

    int numContainer = 1;
    ReservationRequest r = ReservationRequest
        .newInstance(Resource.newInstance(1024, 1), numContainer, 1, duration);
    ReservationRequests reqs = ReservationRequests
        .newInstance(Collections.singletonList(r),
            ReservationRequestInterpreter.R_ALL);
    ReservationDefinition rDef = ReservationDefinition
        .newInstance(arrival, deadline, reqs, "testPlannerPage#reservation");
    ReservationSubmissionRequest sRequest =
        ReservationSubmissionRequest.newInstance(rDef, reservationQ);

    ReservationSubmissionResponse sResponse = null;
    try {
      sResponse = clientService.submitReservation(sRequest);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }

    return sResponse.getReservationId();
  }

  private void removeReservation(ReservationId reservationID) {
    ReservationDeleteRequest dRequest = ReservationDeleteRequest.newInstance(reservationID);
    try {
      clientService.deleteReservation(dRequest);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

}
