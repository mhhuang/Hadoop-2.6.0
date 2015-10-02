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

/** NEW FILE by Amber */

package org.apache.hadoop.yarn.api.records;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p>The request items sent by the <code>ApplicationMaster</code> to the 
 * <code>ResourceManager</code> to provide energy gain of running tasks
 * on each containers.</p> 
 *
 * <p>The item includes:
 *   <ul>
 *     <li>{@link ContainerId}</li>
 *     <li>share rate</li>
 *   </ul>
 * </p>
 * 
 * @see ApplicationMasterProtocol#allocate(AllocateRequest)
 */
@Public
@Stable
public abstract class ContainerShareRate {

  @Public
  @Stable
  public static ContainerShareRate newInstance(ContainerId containerId,
			float shareRate) {
    ContainerShareRate rate = Records.newRecord(ContainerShareRate.class);
		rate.setContainerId(containerId);
		rate.setShareRate(shareRate);
		rate.build();
    return rate;
  }

  @Public
  @Stable
  public abstract ContainerId getContainerId();

  @Private
  @Unstable
  public abstract void setContainerId(ContainerId containerId);

  @Public
  @Stable
  public abstract float getShareRate();

  @Private
  @Unstable
  public abstract void setShareRate(float shareRate);

	protected abstract void build();
}
