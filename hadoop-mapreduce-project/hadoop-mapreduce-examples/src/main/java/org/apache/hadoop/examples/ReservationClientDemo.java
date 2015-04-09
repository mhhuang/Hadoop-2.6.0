package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ReservationSubmissionResponse;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.ReservationSubmissionRequestPBImpl;
import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.ReservationRequestInterpreter;
import org.apache.hadoop.yarn.api.records.ReservationRequests;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.impl.pb.ReservationDefinitionPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ReservationRequestsPBImpl;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.UTCClock;

/**
 * Hello World reservation! It submits an atomic request of 60sec duration, and
 * a deadline 80sec from now. The reservation is N containers "wide" (where N is
 * specified in input). Upon acceptance it runs a Pi computation within the
 * reservation.
 *
 * The only purpose of this class is to showcase how to submit a reservation and
 * launch jobs in it.
 */
public class ReservationClientDemo extends Configured implements Tool {

  private YarnClient yarnClient;
  private static Clock clock = new UTCClock();
  private Configuration conf;

  private ReservationSubmissionRequest createSimpleReservationRequest(
      int numContainers, long arrival, long deadline, long duration) {
    // create a request with a single atomic ask
    ReservationSubmissionRequest request =
        new ReservationSubmissionRequestPBImpl();
    ReservationDefinition rDef = new ReservationDefinitionPBImpl();
    rDef.setArrival(arrival);
    rDef.setDeadline(deadline);
    // add 2GB total capacity to the reservation for AM
    ReservationRequest r =
        ReservationRequest.newInstance(Resource.newInstance(conf.getInt(
            MRJobConfig.MAP_MEMORY_MB, MRJobConfig.DEFAULT_MAP_MEMORY_MB), conf
            .getInt(MRJobConfig.MAP_CPU_VCORES,
                MRJobConfig.DEFAULT_MAP_CPU_VCORES)), numContainers + 2, 1,
            duration);
    ReservationRequests reqs = new ReservationRequestsPBImpl();
    reqs.setReservationResources(Collections.singletonList(r));
    reqs.setInterpreter(ReservationRequestInterpreter.R_ALL);
    rDef.setReservationRequests(reqs);
    request.setQueue(this.getConf().get(MRJobConfig.QUEUE_NAME));
    request.setReservationDefinition(rDef);
    return request;
  }

  public ReservationId submitReservation(int numContainers, long arrival,
      long deadline, long duration) throws YarnException, IOException {

    // submit a reservation request to the ReservationSystem in the RM
    yarnClient = YarnClient.createYarnClient();
    yarnClient.init(conf);
    yarnClient.start();

    ReservationSubmissionRequest request =
        createSimpleReservationRequest(numContainers, arrival, deadline,
            duration);
    ReservationSubmissionResponse response = null;
    response = yarnClient.submitReservation(request);
    System.out.println("Response from ReservationSystem: "
        + response.getReservationId());
    return response.getReservationId();
  }

  @Override
  public int run(String[] args) throws Exception {

    conf = this.getConf();
    long currentTime = clock.getTime();

    // ask for a reservation with N containers (number of maps from args),
    // for 1 min and a 20% deadline slack (i.e., how much flexibility is
    // exposed for placement)
    long duration = 60000L;

    ReservationId rid =
        submitReservation(Integer.parseInt(args[0]), currentTime,
            (long) (currentTime + 1.2 * duration), duration);

    int res = -1;
    if (rid == null) {
      System.err.println("Reservation did not succeed");
    } else {
      // submit a Pi computation "within" the reservation
      conf.set(MRJobConfig.RESERVATION_ID, rid.toString());
      res = ToolRunner.run(conf, new QuasiMonteCarlo(), args);
    }
    return res;
  }

  public static void main(String[] argv) throws Exception {
    System.exit(ToolRunner.run(null, new ReservationClientDemo(), argv));
  }

}
