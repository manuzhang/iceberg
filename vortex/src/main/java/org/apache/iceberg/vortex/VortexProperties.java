/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.vortex;

final class VortexProperties {
  static final String READ_WORKER_THREADS = "read.vortex.worker-threads";
  static final String WRITE_WORKER_THREADS = "write.vortex.worker-threads";
  static final int WORKER_THREADS_DEFAULT = 4;

  static final String WRITE_SPLIT_SIZE = "write.vortex.split-size-bytes";
  static final long WRITE_SPLIT_SIZE_DEFAULT = 64L * 1024 * 1024;

  private VortexProperties() {}
}
