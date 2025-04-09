/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System.Text.Json.Serialization;

namespace DataStax.AstraDB.DataApi.Core.Results;

/// <summary>
/// The results of an update operation
/// </summary>
public class UpdateResult
{
    /// <summary>
    /// If an insert was performed, the id of the inserted document
    /// </summary>
    [JsonPropertyName("upsertedId")]
    public object UpsertedId { get; set; }

    /// <summary>
    /// The number of documents that matched the query
    /// </summary>
    [JsonPropertyName("matchedCount")]
    public int MatchedCount { get; set; }

    /// <summary>
    /// The number of documents that were modified
    /// </summary>
    [JsonPropertyName("modifiedCount")]
    public int ModifiedCount { get; set; }
}

internal class PagedUpdateResult : UpdateResult
{
    [JsonPropertyName("nextPageState")]
    public string NextPageState { get; set; }
}