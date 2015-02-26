/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.audit;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class AuditInfo
{
  private final String author;
  private final String emailId;
  private final String comment;

  @JsonCreator
  public AuditInfo(
      @JsonProperty("author") String author,
      @JsonProperty("emailId") String emailId,
      @JsonProperty("comment") String comment
  )
  {
    this.author = author;
    this.emailId = emailId;
    this.comment = comment;
  }

  @JsonProperty
  public String getAuthor()
  {
    return author;
  }

  @JsonProperty
  public String getEmailId()
  {
    return emailId;
  }

  @JsonProperty
  public String getComment()
  {
    return comment;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    AuditInfo that = (AuditInfo) o;

    if (!author.equals(that.author)) {
      return false;
    }
    if (!comment.equals(that.comment)) {
      return false;
    }
    if (!emailId.equals(that.emailId)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = author.hashCode();
    result = 31 * result + emailId.hashCode();
    result = 31 * result + comment.hashCode();
    return result;
  }
}
