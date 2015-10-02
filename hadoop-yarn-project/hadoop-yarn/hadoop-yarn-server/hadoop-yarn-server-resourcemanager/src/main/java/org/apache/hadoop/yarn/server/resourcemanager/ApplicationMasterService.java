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

package org.apache.hadoop.yarn.server.resourcemanager;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** Amber import starts here */
import accUCLA.accAPI.SocketConnector;
import accUCLA.TaskInfo.AppShareRatePackage;
import accUCLA.TaskInfo.AppUtilizationPackage;
import accUCLA.TaskInfo.MsgNode2GAM;
import accUCLA.TaskInfo.MsgGAM2Node;
import accUCLA.TaskInfo.ShareRatePackage;
import accUCLA.TaskInfo.UtilizationPackage;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.hadoop.yarn.api.records.ContainerShareRate;
/** Amber import ends here */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.PreemptionContainer;
import org.apache.hadoop.yarn.api.records.PreemptionContract;
import org.apache.hadoop.yarn.api.records.PreemptionMessage;
import org.apache.hadoop.yarn.api.records.PreemptionResourceRequest;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceBlacklistRequest;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.StrictPreemptionContract;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationAttemptNotFoundException;
import org.apache.hadoop.yarn.exceptions.ApplicationMasterNotRegisteredException;
import org.apache.hadoop.yarn.exceptions.InvalidApplicationMasterRequestException;
import org.apache.hadoop.yarn.exceptions.InvalidContainerReleaseException;
import org.apache.hadoop.yarn.exceptions.InvalidResourceBlacklistRequestException;
import org.apache.hadoop.yarn.exceptions.InvalidResourceRequestException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.AMLivelinessMonitor;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptRegistrationEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptStatusupdateEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptUnregistrationEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.security.authorize.RMPolicyProvider;
import org.apache.hadoop.yarn.server.security.MasterKeyData;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;

import com.google.common.annotations.VisibleForTesting;

@SuppressWarnings("unchecked")
@Private
public class ApplicationMasterService extends AbstractService implements
    ApplicationMasterProtocol {
  private static final Log LOG = LogFactory.getLog(ApplicationMasterService.class);
  private final AMLivelinessMonitor amLivelinessMonitor;
  private YarnScheduler rScheduler;
  private InetSocketAddress bindAddress;
  private Server server;
  private final RecordFactory recordFactory =
      RecordFactoryProvider.getRecordFactory(null);
  private final ConcurrentMap<ApplicationAttemptId, AllocateResponseLock> responseMap =
      new ConcurrentHashMap<ApplicationAttemptId, AllocateResponseLock>();
  private final RMContext rmContext;

  public ApplicationMasterService(RMContext rmContext, YarnScheduler scheduler) {
    super(ApplicationMasterService.class.getName());
    this.amLivelinessMonitor = rmContext.getAMLivelinessMonitor();
    this.rScheduler = scheduler;
    this.rmContext = rmContext;
  }

  /** Amber code starts here */
  static class ContainerRuntimeInfo {
    private Container container;
    private float shareRate; // set it to either 1 (reserved) or 0 (best-effort)
    private float usageRate;
    private String location; // cpu, fpga, gpu
    private boolean notifyAMShareRate;  // true should notify AM when heartbeating 
    private boolean notifyNMShareRate;
    private boolean notifyAMLocation;  // true should notify AM when heartbeating 
    private boolean notifyNMLocation;

    public ContainerRuntimeInfo (Container c) {
      container = c;
      shareRate = 0.0f;
      usageRate = 0.0f; // actual usage
      location = "";    // cpu fpga 
      notifyAMShareRate = false;
      notifyNMShareRate = false;
      notifyAMLocation = false;
      notifyNMLocation = false;
    }

    public Container getContainer() {
      return container;
    }

    public void setShareRate(float rate) {
      if (rate > shareRate + 0.001f || rate < shareRate - 0.001f) {
        notifyAMShareRate = true;
        notifyNMShareRate = true;
      }
      shareRate = rate;

      setUsageRate(rate);
    }
    public float getShareRate() {
      return shareRate;
    }

    public void setUsageRate(float rate) {
      usageRate = rate;
    }
    public float getUsageRate() {
      return usageRate;
    }

    public void setLocation(String l) {
      if (!l.equals(location)) {
        notifyAMLocation = true;
        notifyNMLocation = true;
      }
      location = l;
    }
    public String getLocation() {
      return location;
    }

    public boolean pullNotifyAMShareRate() {
      boolean ret = notifyAMShareRate;
      notifyAMShareRate = false;
      return ret;
    }
    public boolean pullNotifyNMShareRate() {
      boolean ret = notifyNMShareRate;
      notifyNMShareRate = false;
      return ret;
    }
    public boolean pullNotifyAMLocation() { 
      boolean ret = notifyAMLocation;
      notifyAMLocation = false;
      return ret;
    }
    public boolean pullNotifyNMLocation() { 
      boolean ret = notifyNMLocation;
      notifyNMLocation = false;
      return ret;
    }

  }

  // note down allocated Accs
  // fixme: should note down cpus as well
  private static Map<String, Map<ApplicationId, ContainerRuntimeInfo>> mAccToContainer =
    new HashMap<String, Map<ApplicationId, ContainerRuntimeInfo>>();

  private static Map<String, Float> mAccToUtilization = 
    new HashMap<String, Float>();

  private static List<String> mNodeList = new ArrayList<String>();

  private static List<String> mAccList = new ArrayList<String>();

  //private static Map<ApplicationId, Float> mAppIdToRequestedShares = 
  //  new HashMap<ApplicationId, Float>(); 

  //private static Map<ApplicationId, Float> mAppIdToLaggingIndicator = 
  //  new HashMap<ApplicationId, Float>();

  //private static Map<ContainerId, Float> mContainerIdToEnergyGain = 
  //  new HashMap<ContainerId, Float>();

  //// workaround, currently spark sends the same energyGain for each app
  //private static Map<ApplicationId, Float> mAppIdToEnergyGain = 
  //  new HashMap<ApplicationId, Float>();

  private void addContainer(Container c, String type, boolean reserved) {
    // add to nodeToAccContainerList
    String location = c.getNodeId().getHost();
    LOG.debug("GAM: add Acc Container, location: " + location + " container: " +
        c.toString());
    ApplicationId appId = c.getId().getApplicationAttemptId().getApplicationId();

    // add location --> appId --> container
    Map<ApplicationId, ContainerRuntimeInfo> appToContainer = 
      mAccToContainer.get(location);
    if (appToContainer == null) {
      appToContainer = new HashMap<ApplicationId, ContainerRuntimeInfo>();
      mAccToContainer.put(location, appToContainer);
    }

    ContainerRuntimeInfo cRTInfo = appToContainer.get(appId);
    if (cRTInfo == null) {
      cRTInfo = new ContainerRuntimeInfo(c);
      cRTInfo.setLocation(type);
      cRTInfo.setShareRate(reserved ? 1.0f : 0.0f);
      appToContainer.put(appId, cRTInfo);
    }
  }

  private void deleteContainer(ContainerId cId) {
    // it may not be ACC container
    LOG.info("YARN delete/completed container with ID: " + cId.toString());
    for(Map<ApplicationId, ContainerRuntimeInfo> appToContainer :
        mAccToContainer.values()) {
      for(Iterator<Map.Entry<ApplicationId, ContainerRuntimeInfo>> it 
          = appToContainer.entrySet().iterator(); it.hasNext(); ) {
        Map.Entry<ApplicationId, ContainerRuntimeInfo> entry = it.next();
        if (entry.getValue().getContainer().getId().equals(cId)) {
          it.remove();
    // TODO, notify NAM?
          break;
        }
      }
    }
  }
 /** Amber code ends here */

  @Override
  protected void serviceStart() throws Exception {
    Configuration conf = getConfig();
    YarnRPC rpc = YarnRPC.create(conf);

    InetSocketAddress masterServiceAddress = conf.getSocketAddr(
        YarnConfiguration.RM_BIND_HOST,
        YarnConfiguration.RM_SCHEDULER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS,
        YarnConfiguration.DEFAULT_RM_SCHEDULER_PORT);

    Configuration serverConf = conf;
    // If the auth is not-simple, enforce it to be token-based.
    serverConf = new Configuration(conf);
    serverConf.set(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        SaslRpcServer.AuthMethod.TOKEN.toString());
    this.server =
      rpc.getServer(ApplicationMasterProtocol.class, this, masterServiceAddress,
          serverConf, this.rmContext.getAMRMTokenSecretManager(),
          serverConf.getInt(YarnConfiguration.RM_SCHEDULER_CLIENT_THREAD_COUNT, 
              YarnConfiguration.DEFAULT_RM_SCHEDULER_CLIENT_THREAD_COUNT));
    
    // Enable service authorization?
    if (conf.getBoolean(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, 
        false)) {
      InputStream inputStream =
          this.rmContext.getConfigurationProvider()
              .getConfigurationInputStream(conf,
                  YarnConfiguration.HADOOP_POLICY_CONFIGURATION_FILE);
      if (inputStream != null) {
        conf.addResource(inputStream);
      }
      refreshServiceAcls(conf, RMPolicyProvider.getInstance());
    }
    
    this.server.start();
    this.bindAddress =
        conf.updateConnectAddr(YarnConfiguration.RM_BIND_HOST,
                               YarnConfiguration.RM_SCHEDULER_ADDRESS,
                               YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS,
                               server.getListenerAddress());
    super.serviceStart();
  }

  @Private
  public InetSocketAddress getBindAddress() {
    return this.bindAddress;
  }

  // Obtain the needed AMRMTokenIdentifier from the remote-UGI. RPC layer
  // currently sets only the required id, but iterate through anyways just to be
  // sure.
  private AMRMTokenIdentifier selectAMRMTokenIdentifier(
      UserGroupInformation remoteUgi) throws IOException {
    AMRMTokenIdentifier result = null;
    Set<TokenIdentifier> tokenIds = remoteUgi.getTokenIdentifiers();
    for (TokenIdentifier tokenId : tokenIds) {
      if (tokenId instanceof AMRMTokenIdentifier) {
        result = (AMRMTokenIdentifier) tokenId;
        break;
      }
    }

    return result;
  }

  private AMRMTokenIdentifier authorizeRequest()
      throws YarnException {

    UserGroupInformation remoteUgi;
    try {
      remoteUgi = UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      String msg =
          "Cannot obtain the user-name for authorizing ApplicationMaster. "
              + "Got exception: " + StringUtils.stringifyException(e);
      LOG.warn(msg);
      throw RPCUtil.getRemoteException(msg);
    }

    boolean tokenFound = false;
    String message = "";
    AMRMTokenIdentifier appTokenIdentifier = null;
    try {
      appTokenIdentifier = selectAMRMTokenIdentifier(remoteUgi);
      if (appTokenIdentifier == null) {
        tokenFound = false;
        message = "No AMRMToken found for user " + remoteUgi.getUserName();
      } else {
        tokenFound = true;
      }
    } catch (IOException e) {
      tokenFound = false;
      message =
          "Got exception while looking for AMRMToken for user "
              + remoteUgi.getUserName();
    }

    if (!tokenFound) {
      LOG.warn(message);
      throw RPCUtil.getRemoteException(message);
    }

    return appTokenIdentifier;
  }

  @Override
  public RegisterApplicationMasterResponse registerApplicationMaster(
      RegisterApplicationMasterRequest request) throws YarnException,
      IOException {

    AMRMTokenIdentifier amrmTokenIdentifier = authorizeRequest();
    ApplicationAttemptId applicationAttemptId =
        amrmTokenIdentifier.getApplicationAttemptId();

    ApplicationId appID = applicationAttemptId.getApplicationId();
    AllocateResponseLock lock = responseMap.get(applicationAttemptId);
    if (lock == null) {
      RMAuditLogger.logFailure(this.rmContext.getRMApps().get(appID).getUser(),
          AuditConstants.REGISTER_AM, "Application doesn't exist in cache "
              + applicationAttemptId, "ApplicationMasterService",
          "Error in registering application master", appID,
          applicationAttemptId);
      throwApplicationDoesNotExistInCacheException(applicationAttemptId);
    }

    // Allow only one thread in AM to do registerApp at a time.
    synchronized (lock) {
      AllocateResponse lastResponse = lock.getAllocateResponse();
      if (hasApplicationMasterRegistered(applicationAttemptId)) {
        String message =
            "Application Master is already registered : "
                + appID;
        LOG.warn(message);
        RMAuditLogger.logFailure(
          this.rmContext.getRMApps()
            .get(appID).getUser(),
          AuditConstants.REGISTER_AM, "", "ApplicationMasterService", message,
          appID, applicationAttemptId);
        throw new InvalidApplicationMasterRequestException(message);
      }
      
      this.amLivelinessMonitor.receivedPing(applicationAttemptId);
      RMApp app = this.rmContext.getRMApps().get(appID);
      
      // Setting the response id to 0 to identify if the
      // application master is register for the respective attemptid
      lastResponse.setResponseId(0);
      lock.setAllocateResponse(lastResponse);
      LOG.info("AM registration " + applicationAttemptId);
      this.rmContext
        .getDispatcher()
        .getEventHandler()
        .handle(
          new RMAppAttemptRegistrationEvent(applicationAttemptId, request
            .getHost(), request.getRpcPort(), request.getTrackingUrl()));
      RMAuditLogger.logSuccess(app.getUser(), AuditConstants.REGISTER_AM,
        "ApplicationMasterService", appID, applicationAttemptId);

      // Pick up min/max resource from scheduler...
      RegisterApplicationMasterResponse response = recordFactory
          .newRecordInstance(RegisterApplicationMasterResponse.class);
      response.setMaximumResourceCapability(rScheduler
          .getMaximumResourceCapability());
      response.setApplicationACLs(app.getRMAppAttempt(applicationAttemptId)
          .getSubmissionContext().getAMContainerSpec().getApplicationACLs());
      response.setQueue(app.getQueue());
      if (UserGroupInformation.isSecurityEnabled()) {
        LOG.info("Setting client token master key");
        response.setClientToAMTokenMasterKey(java.nio.ByteBuffer.wrap(rmContext
            .getClientToAMTokenSecretManager()
            .getMasterKey(applicationAttemptId).getEncoded()));        
      }

      // For work-preserving AM restart, retrieve previous attempts' containers
      // and corresponding NM tokens.
      List<Container> transferredContainers =
          ((AbstractYarnScheduler) rScheduler)
            .getTransferredContainers(applicationAttemptId);
      if (!transferredContainers.isEmpty()) {
        response.setContainersFromPreviousAttempts(transferredContainers);
        List<NMToken> nmTokens = new ArrayList<NMToken>();
        for (Container container : transferredContainers) {
          try {
            NMToken token = rmContext.getNMTokenSecretManager()
                .createAndGetNMToken(app.getUser(), applicationAttemptId,
                    container);
            if (null != token) {
              nmTokens.add(token);
            }
          } catch (IllegalArgumentException e) {
            // if it's a DNS issue, throw UnknowHostException directly and that
            // will be automatically retried by RMProxy in RPC layer.
            if (e.getCause() instanceof UnknownHostException) {
              throw (UnknownHostException) e.getCause();
            }
          }
        }
        response.setNMTokensFromPreviousAttempts(nmTokens);
        LOG.info("Application " + appID + " retrieved "
            + transferredContainers.size() + " containers from previous"
            + " attempts and " + nmTokens.size() + " NM tokens.");
      }

      response.setSchedulerResourceTypes(rScheduler
        .getSchedulingResourceTypes());

      return response;
    }
  }

  @Override
  public FinishApplicationMasterResponse finishApplicationMaster(
      FinishApplicationMasterRequest request) throws YarnException,
      IOException {

    ApplicationAttemptId applicationAttemptId =
        authorizeRequest().getApplicationAttemptId();
    ApplicationId appId = applicationAttemptId.getApplicationId();

    RMApp rmApp =
        rmContext.getRMApps().get(applicationAttemptId.getApplicationId());
    // checking whether the app exits in RMStateStore at first not to throw
    // ApplicationDoesNotExistInCacheException before and after
    // RM work-preserving restart.
    if (rmApp.isAppFinalStateStored()) {
      LOG.info(rmApp.getApplicationId() + " unregistered successfully. ");
      return FinishApplicationMasterResponse.newInstance(true);
    }

    AllocateResponseLock lock = responseMap.get(applicationAttemptId);
    if (lock == null) {
      throwApplicationDoesNotExistInCacheException(applicationAttemptId);
    }

    // Allow only one thread in AM to do finishApp at a time.
    synchronized (lock) {
      if (!hasApplicationMasterRegistered(applicationAttemptId)) {
        String message =
            "Application Master is trying to unregister before registering for: "
                + appId;
        LOG.error(message);
        RMAuditLogger.logFailure(
            this.rmContext.getRMApps()
                .get(appId).getUser(),
            AuditConstants.UNREGISTER_AM, "", "ApplicationMasterService",
            message, appId,
            applicationAttemptId);
        throw new ApplicationMasterNotRegisteredException(message);
      }

      this.amLivelinessMonitor.receivedPing(applicationAttemptId);

      rmContext.getDispatcher().getEventHandler().handle(
          new RMAppAttemptUnregistrationEvent(applicationAttemptId, request
              .getTrackingUrl(), request.getFinalApplicationStatus(), request
              .getDiagnostics()));

      // For UnmanagedAMs, return true so they don't retry
      return FinishApplicationMasterResponse.newInstance(
          rmApp.getApplicationSubmissionContext().getUnmanagedAM());
    }
  }

  private void throwApplicationDoesNotExistInCacheException(
      ApplicationAttemptId appAttemptId)
      throws InvalidApplicationMasterRequestException {
    String message = "Application doesn't exist in cache "
        + appAttemptId;
    LOG.error(message);
    throw new InvalidApplicationMasterRequestException(message);
  }
  
  /**
   * @param appAttemptId
   * @return true if application is registered for the respective attemptid
   */
  public boolean hasApplicationMasterRegistered(
      ApplicationAttemptId appAttemptId) {
    boolean hasApplicationMasterRegistered = false;
    AllocateResponseLock lastResponse = responseMap.get(appAttemptId);
    if (lastResponse != null) {
      synchronized (lastResponse) {
        if (lastResponse.getAllocateResponse() != null
            && lastResponse.getAllocateResponse().getResponseId() >= 0) {
          hasApplicationMasterRegistered = true;
        }
      }
    }
    return hasApplicationMasterRegistered;
  }

  @Override
  public AllocateResponse allocate(AllocateRequest request)
      throws YarnException, IOException {

    AMRMTokenIdentifier amrmTokenIdentifier = authorizeRequest();

    ApplicationAttemptId appAttemptId =
        amrmTokenIdentifier.getApplicationAttemptId();
    ApplicationId applicationId = appAttemptId.getApplicationId();

    this.amLivelinessMonitor.receivedPing(appAttemptId);

    /* check if its in cache */
    AllocateResponseLock lock = responseMap.get(appAttemptId);
    if (lock == null) {
      String message =
          "Application attempt " + appAttemptId
              + " doesn't exist in ApplicationMasterService cache.";
      LOG.error(message);
      throw new ApplicationAttemptNotFoundException(message);
    }
    synchronized (lock) {
      AllocateResponse lastResponse = lock.getAllocateResponse();
      if (!hasApplicationMasterRegistered(appAttemptId)) {
        String message =
            "AM is not registered for known application attempt: " + appAttemptId
                + " or RM had restarted after AM registered . AM should re-register.";
        LOG.info(message);
        RMAuditLogger.logFailure(
          this.rmContext.getRMApps().get(appAttemptId.getApplicationId())
            .getUser(), AuditConstants.AM_ALLOCATE, "",
          "ApplicationMasterService", message, applicationId, appAttemptId);
        throw new ApplicationMasterNotRegisteredException(message);
      }

      if ((request.getResponseId() + 1) == lastResponse.getResponseId()) {
        /* old heartbeat */
        return lastResponse;
      } else if (request.getResponseId() + 1 < lastResponse.getResponseId()) {
        String message =
            "Invalid responseId in AllocateRequest from application attempt: "
                + appAttemptId + ", expect responseId to be "
                + (lastResponse.getResponseId() + 1);
        throw new InvalidApplicationMasterRequestException(message);
      }

      //filter illegal progress values
      float filteredProgress = request.getProgress();
      if (Float.isNaN(filteredProgress) || filteredProgress == Float.NEGATIVE_INFINITY
        || filteredProgress < 0) {
         request.setProgress(0);
      } else if (filteredProgress > 1 || filteredProgress == Float.POSITIVE_INFINITY) {
        request.setProgress(1);
      }

      // Send the status update to the appAttempt.
      this.rmContext.getDispatcher().getEventHandler().handle(
          new RMAppAttemptStatusupdateEvent(appAttemptId, request
              .getProgress()));

      List<ResourceRequest> ask = request.getAskList();
      List<ContainerId> release = request.getReleaseList();

      ResourceBlacklistRequest blacklistRequest =
          request.getResourceBlacklistRequest();
      List<String> blacklistAdditions =
          (blacklistRequest != null) ?
              blacklistRequest.getBlacklistAdditions() : Collections.EMPTY_LIST;
      List<String> blacklistRemovals =
          (blacklistRequest != null) ?
              blacklistRequest.getBlacklistRemovals() : Collections.EMPTY_LIST;
      RMApp app =
          this.rmContext.getRMApps().get(applicationId);
      
      // set label expression for Resource Requests
      ApplicationSubmissionContext asc = app.getApplicationSubmissionContext();
      for (ResourceRequest req : ask) {
        if (null == req.getNodeLabelExpression()) {
          req.setNodeLabelExpression(asc.getNodeLabelExpression());
        }
      }
              
      // sanity check
      try {
        RMServerUtils.validateResourceRequests(ask,
            rScheduler.getMaximumResourceCapability(), app.getQueue(),
            rScheduler);
      } catch (InvalidResourceRequestException e) {
        LOG.warn("Invalid resource ask by application " + appAttemptId, e);
        throw e;
      }
      
      try {
        RMServerUtils.validateBlacklistRequest(blacklistRequest);
      }  catch (InvalidResourceBlacklistRequestException e) {
        LOG.warn("Invalid blacklist request by application " + appAttemptId, e);
        throw e;
      }

      // In the case of work-preserving AM restart, it's possible for the
      // AM to release containers from the earlier attempt.
      if (!app.getApplicationSubmissionContext()
        .getKeepContainersAcrossApplicationAttempts()) {
        try {
          RMServerUtils.validateContainerReleaseRequest(release, appAttemptId);
        } catch (InvalidContainerReleaseException e) {
          LOG.warn("Invalid container release by application " + appAttemptId, e);
          throw e;
        }
      }

      /** Amber code starts here */
      if (ask.size() > 0) {
        LOG.info("Amber: YARN received resource requests:");
        for(ResourceRequest rRequest: ask) {
          LOG.info(rRequest.toString());
        }
      }

      // update mNodeList
      mNodeList = this.rScheduler.getSlaveIps();
      for(String ip : mNodeList) {
        LOG.info("Amber: Alive node ip " + ip);
      }


      // talking to nodeAM
      // update mAccList & mAccToUtilization
      getUtilizationFromAllNodeAMs();


      // Parse requests
      List<ResourceRequest> cpuAsk = new ArrayList<ResourceRequest>();
      List<ResourceRequest> accAsk = new ArrayList<ResourceRequest>();

      for(ResourceRequest rRequest: ask) {
        if (rRequest.getCapability().getAccs() == 0) {
          cpuAsk.add(rRequest);
        }
      }
      for(ResourceRequest rRequest: ask) {
        if (rRequest.getCapability().getAccs() > 0) {
          accAsk.add(rRequest);
        }
      }

      int numRequestedAccs = 0;
      for(ResourceRequest rRequest: accAsk) {
        numRequestedAccs += rRequest.getNumContainers();
      }
      if (numRequestedAccs > mAccList.size()) {
        LOG.warn("Amber GAM WARN: Requested number of accs is more than available. " + 
            "requested: " + numRequestedAccs + " available: " + mAccList.size());
      }

      List<ResourceRequest> nodeSpecificResourceRequests = 
        getNodeSpecificAccRequests(accAsk, request.getReserved());

      // combine node specific requests with cpu requests
      ask.clear();
      ask.addAll(cpuAsk);
      ask.addAll(nodeSpecificResourceRequests);

      for(ResourceRequest rRequest: ask) {
        LOG.debug("Amber GAM: Refined requests: " + rRequest.toString());
      }

      LOG.info("Amber GAM: Proceed to yarn");
      /** Amber code ends here */

      // Send new requests to appAttempt.
      Allocation allocation =
          this.rScheduler.allocate(appAttemptId, ask, release, 
              blacklistAdditions, blacklistRemovals);

      if (!blacklistAdditions.isEmpty() || !blacklistRemovals.isEmpty()) {
        LOG.info("blacklist are updated in Scheduler." +
            "blacklistAdditions: " + blacklistAdditions + ", " +
            "blacklistRemovals: " + blacklistRemovals);
      }
      RMAppAttempt appAttempt = app.getRMAppAttempt(appAttemptId);
      AllocateResponse allocateResponse =
          recordFactory.newRecordInstance(AllocateResponse.class);
      if (!allocation.getContainers().isEmpty()) {
        allocateResponse.setNMTokens(allocation.getNMTokens());
      }

      // update the response with the deltas of node status changes
      List<RMNode> updatedNodes = new ArrayList<RMNode>();
      if(app.pullRMNodeUpdates(updatedNodes) > 0) {
        List<NodeReport> updatedNodeReports = new ArrayList<NodeReport>();
        for(RMNode rmNode: updatedNodes) {
          SchedulerNodeReport schedulerNodeReport =  
              rScheduler.getNodeReport(rmNode.getNodeID());
          Resource used = BuilderUtils.newResource(0, 0);
          int numContainers = 0;
          if (schedulerNodeReport != null) {
            used = schedulerNodeReport.getUsedResource();
            numContainers = schedulerNodeReport.getNumContainers();
          }
          NodeId nodeId = rmNode.getNodeID();
          NodeReport report =
              BuilderUtils.newNodeReport(nodeId, rmNode.getState(),
                  rmNode.getHttpAddress(), rmNode.getRackName(), used,
                  rmNode.getTotalCapability(), numContainers,
                  rmNode.getHealthReport(), rmNode.getLastHealthReportTime(),
                  rmNode.getNodeLabels());

          updatedNodeReports.add(report);
        }
        allocateResponse.setUpdatedNodes(updatedNodeReports);
      }
						/** Amber code starts here */

						// remove just finished containers
						for(ContainerId cId: this.rScheduler.pullJustFinishedAccContainers()) {
								deleteContainer(cId);
						}
						// update Acc Containers, reserved container get shareRate = 1.0
						List<Container> allocatedContainers = allocation.getContainers();
						for(Container c : allocatedContainers) {
								if (c.getResource().getAccs() > 0) {
										addContainer(c, "fpga", request.getReserved());
								} 
						}

						//updateAllAccShareRates_EVEN(appAttemptId.getApplicationId(), 
						//				mAppIdToRequestedShares.get(appAttemptId.getApplicationId()) == null ? 0.0f :
						//				mAppIdToRequestedShares.get(appAttemptId.getApplicationId()));

      //// no longer notify AM about share rate
						//// Find containers location changes 
						//// Matched by ApplicationId
						//List<ContainerShareRate> appShareRate = new ArrayList<ContainerShareRate>();
						//for(Map<ApplicationId, ContainerRuntimeInfo> appToContainer : 
						//				mAccToContainer.values()) {
						//		ContainerRuntimeInfo cRTInfo = appToContainer.get(
						//						appAttemptId.getApplicationId());
						//		if (cRTInfo != null) {
						//				if (cRTInfo.pullNotifyAMShareRate()) {
						//						ContainerShareRate cSR = ContainerShareRate.newInstance(
						//										cRTInfo.getContainer().getId(), cRTInfo.getShareRate());
						//						appShareRate.add(cSR);
						//						LOG.debug("Amber GAM: AppId: " + appAttemptId.getApplicationId() +
						//										" container: " + cSR.getContainerId() +
						//										" share rate: " + Float.toString(cSR.getShareRate()));
						//				}
						//		}
						//}
						sendAppShareRateToNodeAMs();

						//// add cpu share rate = 0
						//for(Container c : allocatedContainers) {
						//		if (c.getResource().getAccs() == 0) {
						//				ContainerShareRate cSR = ContainerShareRate.newInstance(c.getId(), 0);
						//				appShareRate.add(cSR);
						//		} 
						//}

						//allocateResponse.setContainerShareRates(appShareRate);

			   /** Amber code ends here */


      allocateResponse.setAllocatedContainers(allocation.getContainers());
      allocateResponse.setCompletedContainersStatuses(appAttempt
          .pullJustFinishedContainers());
      allocateResponse.setResponseId(lastResponse.getResponseId() + 1);
      allocateResponse.setAvailableResources(allocation.getResourceLimit());

      allocateResponse.setNumClusterNodes(this.rScheduler.getNumClusterNodes());

      // add preemption to the allocateResponse message (if any)
      allocateResponse
          .setPreemptionMessage(generatePreemptionMessage(allocation));

      // update AMRMToken if the token is rolled-up
      MasterKeyData nextMasterKey =
          this.rmContext.getAMRMTokenSecretManager().getNextMasterKeyData();

      if (nextMasterKey != null
          && nextMasterKey.getMasterKey().getKeyId() != amrmTokenIdentifier
            .getKeyId()) {
        Token<AMRMTokenIdentifier> amrmToken =
            rmContext.getAMRMTokenSecretManager().createAndGetAMRMToken(
              appAttemptId);
        ((RMAppAttemptImpl)appAttempt).setAMRMToken(amrmToken);
        allocateResponse.setAMRMToken(org.apache.hadoop.yarn.api.records.Token
          .newInstance(amrmToken.getIdentifier(), amrmToken.getKind()
            .toString(), amrmToken.getPassword(), amrmToken.getService()
            .toString()));
        LOG.info("The AMRMToken has been rolled-over. Send new AMRMToken back"
            + " to application: " + applicationId);
      }

      /*
       * As we are updating the response inside the lock object so we don't
       * need to worry about unregister call occurring in between (which
       * removes the lock object).
       */
      lock.setAllocateResponse(allocateResponse);
      return allocateResponse;
    }    
  }
		/** Amber code starts here */

		private List<ResourceRequest>	getNodeSpecificAccRequests(
						List<ResourceRequest> accAsk,
      boolean reserved) {
				if (accAsk.size() == 0)
						return accAsk;
				// TODO sort accAsk via priority & speedup so we can process them in order 

				// syntax check
				int total_requested = 0;
				int i;
				for(i = 0; i < accAsk.size(); i++) {
						total_requested += accAsk.get(i).getNumContainers();
						if (total_requested > mAccList.size())
								break;
				}
				if (i < accAsk.size()) {
						accAsk = accAsk.subList(0, i);
						LOG.error("GAM: app requesting too many accs, truncating requests");
				}

				// Sort mAcclist via utilization
				List<String> sortedAccList = new ArrayList<String>();
    if (reserved) {
      sortedAccList = sortAccsByReservation();
						LOG.info("GAM: sorted acc by reservation ");
						for(String acc : sortedAccList)
								LOG.info("GAM: " + acc);
    } else {
      sortedAccList = sortAccsByUtilization();
				}

    // TODO if already reserved, do not allocate
      

				List<ResourceRequest> nodeRequestList = 
						new ArrayList<ResourceRequest>();

				i = 0;
				for(ResourceRequest rRequest : accAsk) { 
						int numRequestedAccs = rRequest.getNumContainers();

						List<ResourceRequest> nodeRequestsSingleAsk = 
								getNodeSpecificSingleAccRequest(rRequest, 
												sortedAccList.subList(i, i + numRequestedAccs));

						// TODO multiple requests may needs merge? 
						nodeRequestList.addAll(nodeRequestsSingleAsk);

						i += numRequestedAccs;
				}
				return nodeRequestList;
		}

		// sort mAccList by utilization statistics in mAccToUtilization
		private List<String> sortAccsByUtilization() {
				List<String> ret = new ArrayList<String>(mAccList);
				Comparator<String> cmp = new Comparator<String>() {
						public int compare(String o1, String o2) {
								// TODO better check map null
								return mAccToUtilization.get(o1).compareTo(mAccToUtilization.get(o2));
						}
				};
				Collections.sort(ret, cmp);
				return ret;
		}

		// sort mAccList by reservation from 0 to 1 
		private List<String> sortAccsByReservation() {
				List<String> ret = new ArrayList<String>(mAccList);
    final Map<String, Float> accReservedRate = new HashMap<String, Float>();
    for(String acc : ret) {
      float totalRate = 0;

						Map<ApplicationId, ContainerRuntimeInfo> appToContainer 
								= mAccToContainer.get(acc);
						if (appToContainer != null) {
								for(Map.Entry<ApplicationId, ContainerRuntimeInfo> entry :
												appToContainer.entrySet()) {
										totalRate += entry.getValue().getShareRate(); 
								}
						}

      accReservedRate.put(acc, totalRate);
				}
				Comparator<String> cmp = new Comparator<String>() {
						public int compare(String o1, String o2) {
								return accReservedRate.get(o2).compareTo(accReservedRate.get(o1));
						}
				};
				Collections.sort(ret, cmp);
				return ret;
		}


		// Generate refined resource requests on specified locations
		private List<ResourceRequest> getNodeSpecificSingleAccRequest(
						ResourceRequest rRequest, List<String> locations) {
				List<ResourceRequest> nodeRequestList = new ArrayList<ResourceRequest>();

				Resource accCapability = rRequest.getCapability();
				ResourceRequest accRackRequest = ResourceRequest.newInstance(
								rRequest.getPriority(), "/default-rack", accCapability,
								0, false);
				ResourceRequest accAnyRequest = ResourceRequest.newInstance(
								rRequest.getPriority(), "*", accCapability,
								0, false);

				int numRequestedAccs = rRequest.getNumContainers();
				int numFoundAccs = 0;
				for(int i = 0; i < numRequestedAccs; i++) {
						ResourceRequest nodeRequest = ResourceRequest.newInstance(
										rRequest.getPriority(), locations.get(i), accCapability,
										1, true);
						nodeRequestList.add(nodeRequest);
						numFoundAccs++;
				}

				addRackAndAnyRequest(accRackRequest, accAnyRequest,
								numFoundAccs, nodeRequestList);

				return nodeRequestList;
		}

		private void addRackAndAnyRequest(
						ResourceRequest rack,	ResourceRequest any,
						int numContainers, List<ResourceRequest> requestList) {
				rack.setNumContainers(numContainers);
				any.setNumContainers(numContainers);
				requestList.add(rack);
				requestList.add(any);
		}
		/** Amber code ends here */

  private PreemptionMessage generatePreemptionMessage(Allocation allocation){
    PreemptionMessage pMsg = null;
    // assemble strict preemption request
    if (allocation.getStrictContainerPreemptions() != null) {
       pMsg =
        recordFactory.newRecordInstance(PreemptionMessage.class);
      StrictPreemptionContract pStrict =
          recordFactory.newRecordInstance(StrictPreemptionContract.class);
      Set<PreemptionContainer> pCont = new HashSet<PreemptionContainer>();
      for (ContainerId cId : allocation.getStrictContainerPreemptions()) {
        PreemptionContainer pc =
            recordFactory.newRecordInstance(PreemptionContainer.class);
        pc.setId(cId);
        pCont.add(pc);
      }
      pStrict.setContainers(pCont);
      pMsg.setStrictContract(pStrict);
    }

    // assemble negotiable preemption request
    if (allocation.getResourcePreemptions() != null &&
        allocation.getResourcePreemptions().size() > 0 &&
        allocation.getContainerPreemptions() != null &&
        allocation.getContainerPreemptions().size() > 0) {
      if (pMsg == null) {
        pMsg =
            recordFactory.newRecordInstance(PreemptionMessage.class);
      }
      PreemptionContract contract =
          recordFactory.newRecordInstance(PreemptionContract.class);
      Set<PreemptionContainer> pCont = new HashSet<PreemptionContainer>();
      for (ContainerId cId : allocation.getContainerPreemptions()) {
        PreemptionContainer pc =
            recordFactory.newRecordInstance(PreemptionContainer.class);
        pc.setId(cId);
        pCont.add(pc);
      }
      List<PreemptionResourceRequest> pRes = new ArrayList<PreemptionResourceRequest>();
      for (ResourceRequest crr : allocation.getResourcePreemptions()) {
        PreemptionResourceRequest prr =
            recordFactory.newRecordInstance(PreemptionResourceRequest.class);
        prr.setResourceRequest(crr);
        pRes.add(prr);
      }
      contract.setContainers(pCont);
      contract.setResourceRequest(pRes);
      pMsg.setContract(contract);
    }
    
    return pMsg;
  }

  public void registerAppAttempt(ApplicationAttemptId attemptId) {
    AllocateResponse response =
        recordFactory.newRecordInstance(AllocateResponse.class);
    // set response id to -1 before application master for the following
    // attemptID get registered
    response.setResponseId(-1);
    LOG.info("Registering app attempt : " + attemptId);
    responseMap.put(attemptId, new AllocateResponseLock(response));
    rmContext.getNMTokenSecretManager().registerApplicationAttempt(attemptId);
  }

  public void unregisterAttempt(ApplicationAttemptId attemptId) {
    LOG.info("Unregistering app attempt : " + attemptId);
    responseMap.remove(attemptId);
    rmContext.getNMTokenSecretManager().unregisterApplicationAttempt(attemptId);
  }

  public void refreshServiceAcls(Configuration configuration, 
      PolicyProvider policyProvider) {
    this.server.refreshServiceAclWithLoadedConfiguration(configuration,
        policyProvider);
  }
  
  @Override
  protected void serviceStop() throws Exception {
    if (this.server != null) {
      this.server.stop();
    }
    super.serviceStop();
  }
  
  public static class AllocateResponseLock {
    private AllocateResponse response;
    
    public AllocateResponseLock(AllocateResponse response) {
      this.response = response;
    }
    
    public synchronized AllocateResponse getAllocateResponse() {
      return response;
    }
    
    public synchronized void setAllocateResponse(AllocateResponse response) {
      this.response = response;
    }
  }

  @VisibleForTesting
  public Server getServer() {
    return this.server;
  }
		/** Amber code starts here */
		private void getUtilizationFromAllNodeAMs() throws IOException {
				LOG.info("GAM: get utilization from NodeAMs");

				Set<String> originalAccSet = new HashSet<String>(mAccList);
				mAccList.clear();

				for(String node : mNodeList) {
						SocketConnector connector = new SocketConnector(node,5000);
						connector.buildConnection( 0 );
						connector.send(1);
						int msg_length = connector.receive( );
						byte[] msg_response = connector.receive_byte( msg_length );
						connector.closeConnection( );

						MsgNode2GAM response = MsgNode2GAM.parseFrom(msg_response);
						if (response.getPackageList().size() == 0) // no acc on this node
								continue; 

						LOG.info("GAM: responses from node: " + node);

						// fixme assume only one acc on each node
						UtilizationPackage utilizationPkg = response.getPackage(0);

						List<AppUtilizationPackage> appUtiPkg = utilizationPkg.getPackageList(); 
						float nodeTotalUsage = 0;
						for(AppUtilizationPackage pkg : appUtiPkg) {
								String appIdStr = pkg.getApplicationId();
								float usage = (float)pkg.getUtilization();
								ContainerRuntimeInfo cRTInfo = getContainerInfoByAppIdOnNode(
												appIdStr, node);
								if(cRTInfo == null) {
										LOG.error("GAM: cannot found appId " + appIdStr + " on node ");
										continue;
								}
								cRTInfo.setUsageRate(usage);

								nodeTotalUsage += usage;
								LOG.info("GAM: container: " + cRTInfo.getContainer().getId() +
												": usage: "+ String.valueOf(usage));
						}


						// update accs & add its utilization on the node
						mAccList.add(node);
						mAccToUtilization.put(node, nodeTotalUsage);

				}
				Set<String> newAccSet = new HashSet<String>(mAccList);
				if (!newAccSet.equals(originalAccSet) && (originalAccSet.size() != 0))
						LOG.error("GAM: Do NOT support acc configuration changes at run-time!");
		}

		private ContainerRuntimeInfo getContainerInfoByAppIdOnNode(String appIdStr,
						String node) {
				Map<ApplicationId, ContainerRuntimeInfo> appToContainer = 
						mAccToContainer.get(node);

				if (appToContainer == null) return null;
				for(ApplicationId appId : appToContainer.keySet()) {
						if (appId.toString().equals(appIdStr))
								return appToContainer.get(appId);
				}
				return null;
		}

		private void sendAppShareRateToNodeAMs() throws IOException {
				for(String acc : mAccList) {
						LOG.info("GAM: sending share rates to nodeAM @ " + acc);
						Map<ApplicationId, Float> appIdToShareRate = 
								getAppShareRateOnNode(acc);
						sendAppShareRateToNodeAM(acc, appIdToShareRate);
				}
		}

		private Map<ApplicationId, Float> getAppShareRateOnNode(String acc) {
				Map<ApplicationId, Float> appToShareRate =
						new HashMap<ApplicationId, Float>();
				Map<ApplicationId, ContainerRuntimeInfo> appToContainer 
						= mAccToContainer.get(acc);
				if (appToContainer == null)
						return appToShareRate;
				for(Map.Entry<ApplicationId, ContainerRuntimeInfo> entry : 
								appToContainer.entrySet()) {
						if (entry.getValue().pullNotifyNMShareRate())
								appToShareRate.put(entry.getKey(), entry.getValue().getShareRate());
				}
				return appToShareRate;
		}

		private void sendAppShareRateToNodeAM(String accIP, 
						Map<ApplicationId, Float> appToShareRate) throws IOException {

				MsgGAM2Node.Builder msg = MsgGAM2Node.newBuilder();
				for(Map.Entry<ApplicationId, Float> entry : appToShareRate.entrySet()) {
						AppShareRatePackage.Builder appShareRatePkg = AppShareRatePackage.newBuilder();

						// fixme assume only one acc on each node
						ShareRatePackage pkg = ShareRatePackage.newBuilder()
								.setAccType("fpga")
								.setSharingRate((double)entry.getValue())
								.build();

						appShareRatePkg.setApplicationId(entry.getKey().toString());
						appShareRatePkg.addPackage(pkg);

						LOG.info("GAM: AppId: " + entry.getKey().toString() + 
										"shareRate: " + String.valueOf(entry.getValue()));
						msg.addPackage(appShareRatePkg.build());
				}

				byte[] msg_byte = msg.build().toByteArray();
				SocketConnector connector = new SocketConnector(accIP,5006);
				connector.buildConnection( 0 );
				connector.send(msg_byte.length);
				connector.send(msg_byte);
				connector.closeConnection( );
		}

		//private void updateAllAccShareRates_EVEN(ApplicationId appId, 
		//				float requestedShare) {
		//		LOG.info("GAM: updateAllAccShareRates_EVEN");

		//		for(Map<ApplicationId, ContainerRuntimeInfo> appIdToContainer :
		//						mAccToContainer.values()) {
		//				int nofContainers = appIdToContainer.size();
		//				if (nofContainers == 0) continue;
		//				float averageShare = 1/(float)nofContainers;
		//				for(ContainerRuntimeInfo cRTInfo : appIdToContainer.values())
		//						cRTInfo.setShareRate(averageShare);
		//		}


		//}
	/** Amber code ends here */
}
