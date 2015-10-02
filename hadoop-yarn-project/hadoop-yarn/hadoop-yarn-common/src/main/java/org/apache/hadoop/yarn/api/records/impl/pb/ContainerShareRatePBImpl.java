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

/** 
 * New File By Amber
 */
package org.apache.hadoop.yarn.api.records.impl.pb;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerShareRate;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerShareRateProto;

import com.google.common.base.Preconditions;

@Private
@Unstable
public class ContainerShareRatePBImpl extends ContainerShareRate {

	ContainerShareRateProto proto = null;
	ContainerShareRateProto.Builder builder = null;
	private ContainerId containerId = null;

	public ContainerShareRatePBImpl() {
		builder = ContainerShareRateProto.newBuilder();
	}

  public ContainerShareRatePBImpl(ContainerShareRateProto proto) {
    this.proto = proto;
    this.containerId = convertFromProtoFormat(proto.getContainerId());
  }
  
  public ContainerShareRateProto getProto() {
    return proto;
  }

  @Override
  public ContainerId getContainerId() {
    return this.containerId;
  }

  @Override
  public void setContainerId(ContainerId id) {
		if (id != null) {
			Preconditions.checkNotNull(builder);
			builder.setContainerId(convertToProtoFormat(id));
		}
		this.containerId = id;
  }

  @Override
  public float getShareRate() {
    Preconditions.checkNotNull(proto);
    return proto.getShareRate();
  }

  @Override
  public void setShareRate(float rate) {
    Preconditions.checkNotNull(builder);
    builder.setShareRate((rate));
  }

  private ContainerIdPBImpl convertFromProtoFormat(
      ContainerIdProto p) {
    return new ContainerIdPBImpl(p);
  }

  private ContainerIdProto convertToProtoFormat(
      ContainerId t) {
    return ((ContainerIdPBImpl)t).getProto();
  }

  @Override
  protected void build() {
    proto = builder.build();
    builder = null;
  }
}  
