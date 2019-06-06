/*
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
package com.facebook.presto.iceberg;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class CommitTaskData
{
    private String path;
    private String metricsJson;
    private String partitionPath;
    private String partitionDataJson;

    @JsonCreator
    public CommitTaskData(
            @JsonProperty("path") String path,
            @JsonProperty("metricsJson") String metricsJson,
            @JsonProperty("partitionPath") String partitionPath,
            @JsonProperty("partitionDataJson") String partitionDataJson)
    {
        this.path = path;
        this.metricsJson = metricsJson;
        this.partitionPath = partitionPath;
        this.partitionDataJson = partitionDataJson;
    }

    @JsonProperty
    public String getPath()
    {
        return path;
    }

    @JsonProperty
    public String getMetricsJson()
    {
        return metricsJson;
    }

    @JsonProperty
    public String getPartitionPath()
    {
        return partitionPath;
    }

    @JsonProperty
    public String getPartitionDataJson()
    {
        return partitionDataJson;
    }
}
