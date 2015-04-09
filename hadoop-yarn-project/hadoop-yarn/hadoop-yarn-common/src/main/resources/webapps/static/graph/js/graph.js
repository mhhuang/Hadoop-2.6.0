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

function getDates(data) {
  "use strict";
  var simpleSort = function (data) {
      return _.sortBy(data, function (d) { return d; });
    };
  return simpleSort(_.chain(data)
                     .pluck("values")
                     .flatten()
                     .pluck("date")
                     .uniq()
                     .value()
                     .map(function (d) { return d; }));
}

function buildData(data) {
  "use strict";
  var keySort = function (data) {
      return _.sortBy(data, function (d) { return d.key; });
    },
    dateSort  = function (data) {
      return _.sortBy(data, function (d) { return d.date; });
    },
    allDates  = getDates(data);

  _.each(data, function (d) {
    d.valuesTmp = dateSort(d.values);
    d.values = [];
    var i, x, pDate,
      oldValue = 0,
      indexValues = 0,
      dateLength = allDates.length,
      valuesLength = d.valuesTmp.length;
    if (valuesLength > 0) {
      for (i = 0; i < dateLength; i++) {
        pDate = d.valuesTmp[indexValues].date;
        if (allDates[i] < pDate) {
          d.values.push({date: allDates[i], value: oldValue});
        } else {
          oldValue = +d.valuesTmp[indexValues].value;
          d.values.push({date: allDates[i], value: oldValue});
          indexValues++;
          if (indexValues >= valuesLength) {
            for (x = i + 1; x < dateLength; x++) {
              d.values.push({date: allDates[x], value: oldValue});
            }
            break;
          }
        }
      }
    } else {
      for (x = 0; x < dateLength; x++) {
        d.values.push({date: allDates[x], value: oldValue});
      }
    }
    d.values = dateSort(d.values);
    d.valuesTmp = null;
  });
  data = keySort(data);

  return data;
}

function showGraph(refresh) {
  "use strict";
  var from, to, dataLink, reservationUser, reservationName, reservationResource,
    chart, queueName = $("#queueName").val(), grayOut = false;

  refresh = refresh === "undefined" ? false : refresh;
  if (refresh) {
    grayOut = ($("#grayOut").attr("checked") !== undefined) ? true : false;

    from = $("#fromRange").val();
    to = $("#toRange").val();
    from = (from === undefined) ? "" : from;
    to = (to === undefined) ? "" : to;

    if (from !== "" && to !== "" && +from >= +to) {
      alert("(to: " + d3.time.format("%X %x")(new Date(+to)) + ") " +
            " must be greater than " +
            "(from: " + d3.time.format("%X %x")(new Date(+from)) + ")");
      return;
    }

    reservationName = $("#reservation_name").val();
    if (reservationName === "") {
      reservationName = "all";
    }

    reservationUser = $("#reservation_user").val();
    if (reservationUser === "" || grayOut) {
      reservationUser = "all";
    }

    reservationResource = $("input[name='resource']:checked").val();
    if (reservationUser === "") {
      reservationUser = "Memory";
    }

    dataLink = "/cluster/data/" +
           queueName + "/" +
           reservationUser + "/" +
           reservationName + "/" +
           reservationResource + "/" +
           from + "/" + to;
  } else {
    dataLink = "/cluster/data/" + queueName + "/all/all/Memory";
  }

  d3.json(dataLink, function (error, data) {
    if (error !== null) {
      alert(error.statusText);
      return;
    }
    if (data === null || data === "") {
      alert("Error: no data");
      return;
    }

    var max = data.config.max,
      min = data.config.min,
      step = data.config.step,
      user = data.config.user,
      userGrayOut = user,
      resourcesList = data.config.resources,
      resource = data.config.resource,
      maxValueResource = data.config.maxResource,
      realData = buildData(data.graph);

    if (realData == null || realData == "") {
      alert("No data available.");
      return;
    }

    nv.addGraph(function () {
      chart = nv.models.stackedAreaChart()
                .x(function (d) { return d.date; })
                .y(function (d) { return +d.value; })
                .interpolate("step-after")
                .showControls(false)
                .clipEdge(false)
                .margin({right: 43, left: 40})
                .transitionDuration(0);

      userGrayOut = $("#reservation_user").val();
      if (userGrayOut == undefined || userGrayOut == "") {
        userGrayOut = user;
      }
      if (grayOut) {
        var colors = d3.scale.category20().range();
        chart.color(function (d, defIndex) {
          if (d.owner !== userGrayOut) {
            return "#505050";
          }
          return colors[defIndex % colors.length];
        });
      }

      chart.xAxis
           .axisLabel("Time (sec)")
           .tickFormat(function (d) {
                         return d3.time.format("%X")(new Date(d));
                       });

      var labelY = "";
      switch (resource) {
        case "Memory": labelY = "Allocation Memory (GB)"; break;
        case "CPU": labelY = "Virutal Cores (#)"; break;
        default:
          labelY = "Allocation Memory (GB)";
      }
      chart.yAxis
           .tickFormat(d3.format("0,0d"))
           .axisLabel(labelY)
           .axisLabelDistance(50);
      // chart.yDomain([0, maxValueResource])

      d3.selectAll("svg").remove();
      d3.select('#planbox #graph')
        .append("svg")
        .attr("class", "viewGraph")
        .style("height", "600px")
        .style("width", "100%")
        .datum(realData)
        .call(chart);

      // gray background fot the past jobs
      var r = chart.xAxis.range(),
        d = chart.xAxis.domain(),
        now = new Date().getTime(),
        hElement = d3.select("svg .nv-axis").node(),
        w = 0,
        h = (hElement !== null) ? (hElement.getBBox().height - 39) + "px" : "100%";
      if (now <= d[0]) {
        w = "0px";
      } else if (now >= d[1]) {
        w = d3.select("#planbox #graph svg .nv-area").node().getBBox().width + "px"
      } else {
        w = (r[1] - r[0]) / (d[1] - d[0]) * (now - d[0]) + "px";
      }

      var formatValue = d3.format("0f");
      var y2 = d3.scale.linear()
                 .domain([0, d3.max(chart.yAxis.scale().domain())])
                 .range([h, 0]);
      var yAxis2 = d3.svg.axis()
                     .scale(y2)
                     .orient("right")
                     .tickFormat(function (d) { return formatValue(d / maxValueResource * 100); });

      d3.select("#planbox #graph svg .nv-areaWrap")
        .append("svg:rect")
        .attr("height", h)
        .attr("width", w)
        .attr("style", "fill:rgba(215, 215, 215, .5)");

      d3.select("#planbox #graph svg .nv-areaWrap")
        .append("g")
        .attr("class", "y axis yAxis2")
        .attr("transform", "translate(" + (hElement.getBBox().width - 48) + ",0)")
        .call(yAxis2);

      d3.select("#planbox #graph svg .nv-areaWrap .yAxis2")
        .append("text")
        .attr("class", "y label")
        .attr("text-anchor", "end")
        .attr("transform", "translate(40," + ((hElement.getBBox().height / 2) - 50) + ") rotate(-90)")
        .text("% of resource");

      // remove some useless things
      d3.selectAll(".nv-controlsWrap").remove();
      d3.selectAll(".nv-interactive").remove();
      d3.selectAll(".nv-scatterWrap").remove();

      // set and print filter
      var oldFromText = $("#fromText").text(),
        oldToText = $("#toText").text(),
        oldFromRange = $("#fromRange").val(),
        oldToRange = $("#toRange").val();
      d3.selectAll("div#controlBox *").remove();

      // control box
      var resTxt = ""
      for (var i = 0; i < resourcesList.length; i++) {
        resTxt += "<input type='radio' name='resource' value='";
        resTxt += resourcesList[i] + "'";
        if (resourcesList[i] == resource)
          resTxt += " checked";
        resTxt += " onclick='showGraph(true)'> " + resourcesList[i] + " ";
      }
      $("div#controlBox").html('<fieldset style="border: 1px solid black; margin: 10px 0;"><legend>Control Box</legend><table><tr><td>Reservation id</td><td><input type="text" id="reservation_name"></td></tr><tr><td>User</td><td><input type="text" id="reservation_user"></td></tr><tr><td></td><td><input type="checkbox" id="grayOut"> "gray-out" everything beside "<label id="userGrayOut">' + userGrayOut + '</label>" jobs</td></tr><tr><td>From:</td><td><input type="range" step="' + step + '" id="fromRange" /></td><td><label id="fromText"></label></td></tr><tr><td class="e">To:</td><td><input type="range" step="10000" id="toRange" /></td><td><label id="toText"></label></td></tr><tr><td>Resource</td><td>' + resTxt + '</td></tr></table><button onclick="showGraph(true)">Refresh Queue</button></fieldset>');
      // from slider
      $("#fromRange").attr("max", max).attr("min", min);
      if (oldFromRange === undefined) {
        $("#fromRange").attr("value", min);
      } else {
        $("#fromRange").attr("value", oldFromRange);
      }
      if (oldFromText !== "") {
        $("#fromText").text(oldFromText);
      } else {
        $("#fromText").text(d3.time.format("%X %x")(new Date(+$("#fromRange").attr("min"))));
      }
      // to slider
      $("#toRange").attr("max", max).attr("min", min);
      if (oldToRange === undefined) {
        $("#toRange").attr("value", max);
      } else {
        $("#toRange").attr("value", oldToRange);
      }
      if (oldToText !== "") {
        $("#toText").text(oldToText);
      } else {
        $("#toText").text(d3.time.format("%X %x")(new Date(+$("#toRange").attr("max"))));
      }
      // update label
      if (reservationName !== undefined && reservationName !== "all") {
        $("#reservation_name").val(reservationName);
      }
      if (reservationUser !== undefined && reservationUser !== "all") {
        $("#reservation_user").val(reservationUser);
      } else if (grayOut) {
        $("#reservation_user").val(userGrayOut);
      }
      if (grayOut) { $("#grayOut").attr("checked", true); }
      $("#fromRange").on("input", function () { 
            $("#fromText").text(
                d3.time.format("%X %x")(new Date(+$("#fromRange").val()))
            );
      });
      $("#toRange").on("input", function () {
            $("#toText").text(
                d3.time.format("%X %x")(new Date(+$("#toRange").val()))
            );
      });
      $("#grayOut").on("click", function () { showGraph(true); });
      $("#reservation_user").on("input", function () {
            $("#userGrayOut").text(
                (this.value !== "") ? this.value : user
            );
      });

      var enterRefresh = function (obj, e) {
        if (e.which === 13) {
          e.preventDefault();
          e.stopPropagation();
          if (obj.value !== undefined) {
            showGraph(true);
          }
        }
      };
      $("#reservation_user").keypress(function (e) { enterRefresh(this, e); });
      $("#reservation_name").keypress(function (e) { enterRefresh(this, e); });

      return chart;
    });
  });
}
