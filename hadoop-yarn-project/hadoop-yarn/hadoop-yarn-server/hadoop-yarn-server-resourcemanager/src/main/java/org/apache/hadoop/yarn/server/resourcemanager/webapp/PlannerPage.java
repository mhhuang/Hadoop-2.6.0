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

import java.util.Map;

import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.Plan;
import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.YarnWebParams;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.SELECT;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.DIV;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import com.google.inject.Inject;

/**
 * This class visualizes the Plan(s)
 */
public class PlannerPage extends RmView {

  static class PlanBlock extends HtmlBlock {
    final Map<String, Plan> plans;

    @Inject PlanBlock(ResourceManager rm) {
      if (rm.getRMContext().getReservationSystem() != null) {
        plans = rm.getRMContext().getReservationSystem().getAllPlans();
      } else {
        plans = null;
      }
    }

    @Override
    public void render(Block html) {

      if (plans == null) {
        html.h2("This cluster is not configured to use a ReservationSystem, or no Plan(s) have been defined.");
        return;
      }

      if (plans.isEmpty()) {
        html.h2("No Plan(s) have been defined.");
        return;
      }

      String queueName = $(YarnWebParams.QUEUE_NAME);

      // include script and css
      html.link().$rel("stylesheet")
          .$href(root_url("static/graph/css/nv.d3.css"))._();
      html.script().$type("text/javascript")
          .$src(root_url("static/graph/js/d3.v3.js"))._();
      html.script().$type("text/javascript")
          .$src(root_url("static/graph/js/nv.d3.min.js"))._();
      html.script().$type("text/javascript")
          .$src(root_url("static/graph/js/underscore.min.js"))._();
      html.script().$type("text/javascript")
          .$src(root_url("static/graph/js/graph.js"))._();

      html.style("input[type=range] { width: 100%; }\n"
          + "#controlBox td.e { width: 100px;}\n"
          + "div#controlBox input[type=\"text\"] { border: 1px solid black; width: 250px; }");

      // create select and div for the graph
      DIV planbox = html.div().$id("planbox").
          _("Choose the queue to visualize its plan: ");
      SELECT select = planbox.select().$id("queueName").$onchange("showGraph()");
      select.option().$value("no_data")._("------")._();
      for (Plan plan : plans.values()) {
        String qName = plan.getQueueName();
        if (qName.equals(queueName))
          select.option().$selected().$value(qName)._(qName)._();
        else
          select.option().$value(qName)._(qName)._();
      }
      select._();
      planbox.div().$id("controlBox")._();
      planbox.div().$id("graph")._();
      html.script()._("if ($(\"#queueName\").val() != \"no_data\") showGraph()")._();
      planbox._();
    }

  }

  @Override
  protected Class<? extends SubView> content() {
    return PlanBlock.class;
  }

}
