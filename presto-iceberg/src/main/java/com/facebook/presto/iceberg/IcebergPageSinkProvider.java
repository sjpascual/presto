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

import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.type.TypeManager;
import io.airlift.json.JsonCodec;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;

import javax.inject.Inject;

public class IcebergPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final HdfsEnvironment hdfsEnvironment;
    private JsonCodec<CommitTaskData> jsonCodec;
    private TypeManager typeManager;

    @Inject
    public IcebergPageSinkProvider(HdfsEnvironment hdfsEnvironment,
            JsonCodec<CommitTaskData> jsonCodec,
            TypeManager typeManager)
    {
        this.hdfsEnvironment = hdfsEnvironment;
        this.jsonCodec = jsonCodec;
        this.typeManager = typeManager;
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle)
    {
        return createPageSink(transactionHandle, session, (ConnectorInsertTableHandle) outputTableHandle);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle)
    {
        IcebergInsertTableHandle tableHandle = (IcebergInsertTableHandle) insertTableHandle;
        HdfsEnvironment.HdfsContext hdfsContext = new HdfsEnvironment.HdfsContext(session, tableHandle.getSchemaName(), tableHandle.getTableName());
        Schema schema = SchemaParser.fromJson(tableHandle.getSchemaAsJson());
        return new IcebergPageSink(
                schema,
                PartitionSpecParser.fromJson(schema, tableHandle.getPartitionSpecAsJson()),
                tableHandle.getFilePrefix(),
                hdfsEnvironment.getConfiguration(hdfsContext, new Path(tableHandle.getFilePrefix())),
                tableHandle.getInputColumns(),
                typeManager,
                jsonCodec,
                session,
                tableHandle.getFileFormat());
    }
}
