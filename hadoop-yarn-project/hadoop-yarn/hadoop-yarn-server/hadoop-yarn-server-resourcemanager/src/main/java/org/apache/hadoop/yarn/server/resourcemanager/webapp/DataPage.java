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

import java.util.*;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.Plan;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.ReservationAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.webapp.YarnWebParams;

import com.google.inject.Inject;
import org.apache.hadoop.yarn.webapp.view.TextView;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 * This class send a json of reservations to the browser
 */
public class DataPage extends TextView {
  final Map<String, Plan> plans;
  final String NODATA = "{\"config\":{\"min\":\"0\",\"max\":\"0\",\"step\":\"0\",\"user\":\"\"},\"graph\":[]}";
  final String ALL_USER = "all";
  final String ALL_RESERVATION = "all";
  final String DEFAULT_RESOURCE = "Memory";
  final CapacitySchedulerConfiguration csConf;
  final QueueMetrics qm;

  final String TYPE_MEMORY = "Memory";
  final String TYPE_CPU= "CPU";

  @Inject
  DataPage(ViewContext ctx, String contentType, ResourceManager rm, CapacitySchedulerConfiguration csConf) {
    super(ctx, contentType);
    if (rm.getRMContext().getReservationSystem() != null) {
      plans = rm.getRMContext().getReservationSystem().getAllPlans();
    } else {
      plans = null;
    }
    this.csConf = csConf;
    qm = rm.getResourceScheduler().getRootQueueMetrics();
  }

  private JSONObject createValueRecord(Long time, int resource) throws JSONException {
    JSONObject record = new JSONObject();
    record.put("date", time);
    record.put("value", resource);
    return record;
  }

  private int getResource(Resource resource, String type) {
    if (type.equals(TYPE_MEMORY)) {
      return resource.getMemory() / 1024;
    }
    if (type.equals(TYPE_CPU)) {
      return resource.getVirtualCores();
    }
    return 0;
  }

  public String createJSON(Plan plan, String user, String rName, String resource, long from, long to)
      throws JSONException {
    if (plan.getAllReservations() == null) {
      return NODATA;
    }
    if (plan.getAllReservations().size() < 1) {
      return NODATA;
    }

    JSONArray graph = new JSONArray();
    for (ReservationAllocation reservation : plan.getAllReservations()) {
      boolean userCheck = user.equals(ALL_USER) ||
                          user.equals(reservation.getUser());
      boolean rNameCheck = rName.equals(ALL_RESERVATION) ||
                           rName.equals(reservation.getReservationId().toString());

      if (userCheck && rNameCheck) {
        JSONObject res = new JSONObject();
        JSONArray values = new JSONArray();
        int oldValue = -1;
        if (from != Long.MIN_VALUE) {
          oldValue = getResource(reservation.getResourcesAtTime(from), resource);
          values.put(createValueRecord(from, oldValue));
        }
        for (long t = plan.getEarliestStartTime();
             t < plan.getLastEndTime(); t += plan.getStep()) {
          int newValue= getResource(reservation.getResourcesAtTime(t), resource);
          if (newValue != oldValue) {
            if (t >= from) {
              values.put(createValueRecord(t, newValue));
            }
            oldValue = newValue;
          }
          if (t >= to) {
            values.put(createValueRecord(to, 0));
            break;
          }
        }
        values.put(createValueRecord(plan.getLastEndTime(), 0));
        res.put("owner", reservation.getUser());
        res.put("key", reservation.getReservationId());
        res.put("values", values);
        graph.put(res);
      }
    }

    int maxResource = 1; //default value
    if (resource.equals(TYPE_MEMORY)) {
      maxResource = (qm.getAvailableMB() + qm.getAllocatedMB()) / 1024;
    } else if (resource.equals(TYPE_CPU)) {
      maxResource =
          qm.getAvailableVirtualCores() + qm.getAllocatedVirtualCores();
    }

    JSONArray resources = new JSONArray();
    resources.put(TYPE_MEMORY);
    resources.put(TYPE_CPU);

    JSONObject json = new JSONObject();
    JSONObject config = new JSONObject();

    config.put("min", plan.getEarliestStartTime());
    config.put("max", plan.getLastEndTime());
    config.put("step", plan.getStep());
    config.put("user", request().getRemoteUser());
    config.put("resources", resources);
    config.put("resource", resource);
    config.put("maxResource", maxResource);
    json.put("config", config);
    json.put("graph", graph);

    return json.toString();
  }

  @Override
  public void render() {
    String queueName = $(YarnWebParams.QUEUE_NAME);
    String user = $(YarnWebParams.PLAN_JSON_USER);
    String rName = $(YarnWebParams.PLAN_JSON_RES_NAME);
    String resource = $(YarnWebParams.PLAN_JSON_RESOURCE);
    String fromDate = $(YarnWebParams.PLAN_JSON_FROM);
    String toDate = $(YarnWebParams.PLAN_JSON_TO);

    boolean check = false;

    if (user.isEmpty()) {
      user = ALL_USER;
    }
    if (rName.isEmpty()) {
      rName= ALL_RESERVATION;
    }
    if (resource.isEmpty()) {
      resource = DEFAULT_RESOURCE;
    }
    long from, to;
    from = (fromDate.isEmpty() ? Long.MIN_VALUE : Long.parseLong(fromDate));
    to = (toDate.isEmpty() ? Long.MAX_VALUE : Long.parseLong(toDate));

    if (!queueName.trim().equals("") || queueName.isEmpty()) {
      for (Plan plan : plans.values()) {
        if (plan.getQueueName().equals(queueName)) {
          String json;
          try {
            json = createJSON(plan, user, rName, resource, from, to);
          } catch (JSONException e) {
            json = NODATA;
          }
          puts(json);
          check = true;
          break;
        }
      }
    }
    if (!check) {
      puts(NODATA);
    }
  }
}
