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
package org.apache.iceberg.formats;

import java.io.Serializable;
import java.util.Locale;
import java.util.Objects;
import java.util.regex.Pattern;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/** Metadata stored in a {@code custom} file to identify the underlying file format. */
public final class CustomFileFormat implements Serializable {
  private static final Pattern VALID_NAME = Pattern.compile("[a-z][a-z0-9._-]*");

  private final String name;
  private final String metadataLocation;

  public static CustomFileFormat of(String name, String metadataLocation) {
    Preconditions.checkArgument(
        metadataLocation != null && !metadataLocation.isEmpty(),
        "Invalid custom file format metadata location: %s",
        metadataLocation);
    return new CustomFileFormat(normalize(name), metadataLocation);
  }

  static String normalize(String name) {
    Preconditions.checkArgument(name != null, "Invalid custom file format name: null");
    String normalized = name.toLowerCase(Locale.ROOT);
    Preconditions.checkArgument(
        VALID_NAME.matcher(normalized).matches(), "Invalid custom file format name: %s", name);
    return normalized;
  }

  private CustomFileFormat(String name, String metadataLocation) {
    this.name = name;
    this.metadataLocation = metadataLocation;
  }

  /** Returns the canonical lower-case name of the underlying file format. */
  public String name() {
    return name;
  }

  /** Returns the location of the metadata needed to read the underlying file format. */
  public String metadataLocation() {
    return metadataLocation;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }

    if (other == null || getClass() != other.getClass()) {
      return false;
    }

    CustomFileFormat that = (CustomFileFormat) other;
    return name.equals(that.name) && metadataLocation.equals(that.metadataLocation);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, metadataLocation);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("name", name)
        .add("metadataLocation", metadataLocation)
        .toString();
  }
}
