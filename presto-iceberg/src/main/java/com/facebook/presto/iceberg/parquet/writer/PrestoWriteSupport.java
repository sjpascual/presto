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
package com.facebook.presto.iceberg.parquet.writer;

import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveErrorCode;
import com.facebook.presto.iceberg.type.TypeConverter;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DateTimeEncoding;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Decimals;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.RealType;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.SqlDecimal;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.TimestampWithTimeZoneType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.iceberg.type.TypeConverter.convert;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toList;
import static org.apache.iceberg.types.Type.TypeID.BINARY;
import static org.apache.iceberg.types.Type.TypeID.BOOLEAN;
import static org.apache.iceberg.types.Type.TypeID.DATE;
import static org.apache.iceberg.types.Type.TypeID.DOUBLE;
import static org.apache.iceberg.types.Type.TypeID.FIXED;
import static org.apache.iceberg.types.Type.TypeID.FLOAT;
import static org.apache.iceberg.types.Type.TypeID.INTEGER;
import static org.apache.iceberg.types.Type.TypeID.LONG;
import static org.apache.iceberg.types.Type.TypeID.STRING;
import static org.apache.iceberg.types.Type.TypeID.UUID;

public class PrestoWriteSupport
        extends WriteSupport<Page>
{
    // The page is ordered based on the hive column order.
    private final List<HiveColumnHandle> columns;
    private final MessageType schema;
    private final TypeManager typeManager;
    private final ConnectorSession session;
    private final Schema icebergSchema;
    private final List<ColumnWriter> writers;
    private final List<Boolean> isNullable;

    private RecordConsumer recordConsumer;

    public PrestoWriteSupport(List<HiveColumnHandle> columns, MessageType schema, Schema icebergSchema, TypeManager typeManager, ConnectorSession session, List<Boolean> isNullable)
    {
        this.columns = columns;
        this.schema = schema;
        this.typeManager = typeManager;
        this.session = session;
        this.icebergSchema = icebergSchema;
        this.writers = getPrestoType(columns).stream().map(t -> getWriter(t)).collect(toList());
        this.isNullable = isNullable;
    }

    @Override
    public WriteContext init(Configuration configuration)
    {
        return new WriteContext(schema, emptyMap());
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer)
    {
        this.recordConsumer = recordConsumer;
    }

    @Override
    public void write(Page page)
    {
        final int numRows = page.getPositionCount();
        for (int rowNum = 0; rowNum < numRows; rowNum++) {
            recordConsumer.startMessage();
            for (int columnIndex = 0; columnIndex < page.getChannelCount(); columnIndex++) {
                final Block block = page.getBlock(columnIndex);
                if (!block.isNull(rowNum)) {
                    consumeField(columns.get(columnIndex).getName(), columnIndex, writers.get(columnIndex), block, rowNum);
                }
                else if (!isNullable.get(columnIndex)) {
                    throw new PrestoException(HiveErrorCode.HIVE_BAD_DATA, String.format("%s column can not be null", columns.get(columnIndex).getName()));
                }
            }
            recordConsumer.endMessage();
        }
    }

    public List<Type> getPrestoType(List<HiveColumnHandle> columns)
    {
        return columns.stream().filter(column -> !column.isHidden()).map(col -> convert(icebergSchema.findType(col.getName()), typeManager)).collect(toList());
    }

    private interface ColumnWriter<T>
    {
        void write(Block block, int rownum);

        void write(T obj);
    }

    // TODO instead of Type.get***() method we can use com.facebook.presto.spi.type.TypeUtils.readNativeValue
    private class IntWriter
            implements ColumnWriter<Integer>
    {
        @Override
        public void write(Block block, int rownum)
        {
            write(toIntExact(IntegerType.INTEGER.getLong(block, rownum)));
        }

        @Override
        public void write(Integer obj)
        {
            recordConsumer.addInteger(obj);
        }
    }

    private class BooleanWriter
            implements ColumnWriter<Boolean>
    {
        @Override
        public void write(Block block, int rownum)
        {
            write(BooleanType.BOOLEAN.getBoolean(block, rownum));
        }

        @Override
        public void write(Boolean obj)
        {
            recordConsumer.addBoolean(obj);
        }
    }

    private class LongWriter
            implements ColumnWriter<Long>
    {
        @Override
        public void write(Block block, int rownum)
        {
            write(BigintType.BIGINT.getLong(block, rownum));
        }

        @Override
        public void write(Long obj)
        {
            recordConsumer.addLong(obj);
        }
    }

    private class BinaryWriter
            implements ColumnWriter<byte[]>
    {
        @Override
        public void write(Block block, int rownum)
        {
            write(VarbinaryType.VARBINARY.getSlice(block, rownum).getBytes());
        }

        @Override
        public void write(byte[] obj)
        {
            recordConsumer.addBinary(Binary.fromConstantByteArray(obj));
        }
    }

    private class FloatWriter
            implements ColumnWriter<Float>
    {
        @Override
        public void write(Block block, int rownum)
        {
            write(intBitsToFloat((int) RealType.REAL.getLong(block, rownum)));
        }

        @Override
        public void write(Float obj)
        {
            recordConsumer.addFloat(obj);
        }
    }

    private class DoubleWriter
            implements ColumnWriter<Double>
    {
        @Override
        public void write(Block block, int rownum)
        {
            write(DoubleType.DOUBLE.getDouble(block, rownum));
        }

        @Override
        public void write(Double obj)
        {
            recordConsumer.addDouble(obj);
        }
    }

    private class StringWriter
            implements ColumnWriter<String>
    {
        @Override
        public void write(Block block, int rownum)
        {
            write((String) VarcharType.VARCHAR.getObjectValue(session, block, rownum));
        }

        @Override
        public void write(String obj)
        {
            recordConsumer.addBinary(Binary.fromReusedByteArray(obj.getBytes()));
        }
    }

    private class DateWriter
            implements ColumnWriter<Long>
    {
        @Override
        public void write(Block block, int rownum)
        {
            write(DateType.DATE.getLong(block, rownum));
        }

        @Override
        public void write(Long obj)
        {
            recordConsumer.addInteger(obj.intValue());
        }
    }

    private class TimeStampWriter
            implements ColumnWriter<Long>
    {
        private boolean hasTimezone;

        public TimeStampWriter(boolean hasTimezone)
        {
            this.hasTimezone = hasTimezone;
        }

        @Override
        public void write(Block block, int rownum)
        {
            write(TimestampType.TIMESTAMP.getLong(block, rownum));
        }

        @Override
        public void write(Long obj)
        {
            long timestamp = hasTimezone ? DateTimeEncoding.unpackMillisUtc(obj) : obj;
            recordConsumer.addLong(TimeUnit.MILLISECONDS.toMicros(timestamp));
        }
    }

    private class DecimalWriter
            implements ColumnWriter<SqlDecimal>
    {
        private static final int MAX_INT_PRECISION = 8;
        private final DecimalType decimalType;

        public DecimalWriter(DecimalType decimalType)
        {
            this.decimalType = decimalType;
        }

        @Override
        public void write(Block block, int rownum)
        {
            write((SqlDecimal) decimalType.getObjectValue(session, block, rownum));
        }

        @Override
        public void write(SqlDecimal sqlDecimal)
        {
            if (decimalType.getPrecision() <= MAX_INT_PRECISION) {
                recordConsumer.addInteger(sqlDecimal.getUnscaledValue().intValueExact());
            }
            else if (decimalType.getPrecision() <= Decimals.MAX_SHORT_PRECISION) {
                recordConsumer.addLong(sqlDecimal.getUnscaledValue().longValueExact());
            }
            else {
                recordConsumer.addBinary(Binary.fromReusedByteArray(sqlDecimal.getUnscaledValue().toByteArray()));
            }
        }
    }

    private class ListWriter
            implements ColumnWriter<Object[]>
    {
        private final ArrayType arrayType;
        private final ColumnWriter baseTypeWriter;

        private ListWriter(ArrayType arrayType)
        {
            this.arrayType = arrayType;
            this.baseTypeWriter = getWriter(arrayType.getElementType());
        }

        @Override
        public void write(Block block, int rownum)
        {
            final List<Object> elements = (List<Object>) arrayType.getObjectValue(session, block, rownum);
            write(elements.toArray());
        }

        @Override
        public void write(Object[] elements)
        {
            recordConsumer.startGroup();
            if (elements != null && elements.length != 0) {
                recordConsumer.startField("list", 0);
                for (int i = 0; i < elements.length; i++) {
                    recordConsumer.startGroup();
                    if (elements[i] != null) {
                        consumeField("element", 0, baseTypeWriter, elements[i]);
                    }
                    recordConsumer.endGroup();
                }
                recordConsumer.endField("list", 0);
            }
            recordConsumer.endGroup();
        }
    }

    private class MapWriter
            implements ColumnWriter<Map>
    {
        private final MapType mapType;
        private final ColumnWriter keyWriter;
        private final ColumnWriter valueWriter;

        private MapWriter(MapType mapType)
        {
            this.mapType = mapType;
            this.keyWriter = getWriter(mapType.getKeyType());
            this.valueWriter = getWriter(mapType.getValueType());
        }

        @Override
        public void write(Block block, int rownum)
        {
            write((Map) this.mapType.getObjectValue(session, block, rownum));
        }

        @Override
        public void write(Map map)
        {
            recordConsumer.startGroup();
            if (map != null && map.size() != 0) {
                recordConsumer.startField("key_value", 0);
                int i = 0;
                for (Object entry : map.entrySet()) {
                    recordConsumer.startGroup();
                    consumeField("key", 0, keyWriter, ((Map.Entry) entry).getKey());

                    if (((Map.Entry) entry).getValue() != null) {
                        consumeField("value", 1, valueWriter, ((Map.Entry) entry).getValue());
                    }
                    i++;
                    recordConsumer.endGroup();
                }
                recordConsumer.endField("key_value", 0);
            }
            recordConsumer.endGroup();
        }
    }

    private class RowWriter
            implements ColumnWriter<List<Object>>
    {
        private final List<ColumnWriter> columnWriters;
        private final RowType rowType;

        private RowWriter(RowType rowType)
        {
            this.rowType = rowType;
            this.columnWriters = rowType.getFields().stream().map(f -> getWriter(f.getType())).collect(toList());
        }

        @Override
        public void write(Block block, int rownum)
        {
            write((List<Object>) rowType.getObjectValue(session, block, rownum));
        }

        @Override
        public void write(List<Object> fields)
        {
            recordConsumer.startGroup();
            for (int i = 0; i < fields.size(); i++) {
                final String name = rowType.getFields().get(i).getName().orElseThrow(() -> new IllegalArgumentException("parquet requires row type fields to have names"));
                if (fields.get(i) != null) {
                    consumeField(name, i, columnWriters.get(i), fields.get(i));
                }
            }
            recordConsumer.endGroup();
        }
    }

    private void consumeField(String fieldName, int index, ColumnWriter writer, Block block, int rowNum)
    {
        recordConsumer.startField(fieldName, index);
        writer.write(block, rowNum);
        recordConsumer.endField(fieldName, index);
    }

    private void consumeField(String fieldName, int index, ColumnWriter writer, Object value)
    {
        recordConsumer.startField(fieldName, index);
        writer.write(value);
        recordConsumer.endField(fieldName, index);
    }

    private final Map<Type, ColumnWriter> writerMap = new HashMap()
    {{
            put(BOOLEAN, new BooleanWriter());
            put(LONG, new LongWriter());
            put(FLOAT, new FloatWriter());
            put(DOUBLE, new DoubleWriter());
            put(INTEGER, new IntWriter());
            put(DATE, new DateWriter());
            //TODO put(org.apache.iceberg.types.Type.TypeID.TIME, new TimeWriter());
            put(STRING, new StringWriter());
            put(UUID, new StringWriter());
            put(FIXED, new BinaryWriter());
            put(BINARY, new BinaryWriter());
        }};

    private final ColumnWriter getWriter(Type type)
    {
        final org.apache.iceberg.types.Type icebergType = TypeConverter.convert(type);
        if (writerMap.containsKey(icebergType.typeId())) {
            return writerMap.get(icebergType.typeId());
        }
        else {
            switch (icebergType.typeId()) {
                case DECIMAL:
                    return new DecimalWriter((DecimalType) type);
                case LIST:
                    return new ListWriter((ArrayType) type);
                case MAP:
                    return new MapWriter((MapType) type);
                case STRUCT:
                    return new RowWriter((RowType) type);
                case TIMESTAMP:
                    boolean hasTimezone = type instanceof TimestampWithTimeZoneType;
                    return new TimeStampWriter(hasTimezone);
                default:
                    throw new UnsupportedOperationException(" presto does not support " + icebergType);
            }
        }
    }
}
