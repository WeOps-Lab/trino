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
package io.trino.plugin.datainsight;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.trino.plugin.datainsight.decoders.ArrayDecoder;
import io.trino.plugin.datainsight.decoders.BigintDecoder;
import io.trino.plugin.datainsight.decoders.BooleanDecoder;
import io.trino.plugin.datainsight.decoders.Decoder;
import io.trino.plugin.datainsight.decoders.DoubleDecoder;
import io.trino.plugin.datainsight.decoders.IdColumnDecoder;
import io.trino.plugin.datainsight.decoders.IntegerDecoder;
import io.trino.plugin.datainsight.decoders.IpAddressDecoder;
import io.trino.plugin.datainsight.decoders.RawJsonDecoder;
import io.trino.plugin.datainsight.decoders.RealDecoder;
import io.trino.plugin.datainsight.decoders.RowDecoder;
import io.trino.plugin.datainsight.decoders.ScoreColumnDecoder;
import io.trino.plugin.datainsight.decoders.SmallintDecoder;
import io.trino.plugin.datainsight.decoders.SourceColumnDecoder;
//import io.trino.plugin.datainsight.decoders.TimestampDecoder;
import io.trino.plugin.datainsight.decoders.TinyintDecoder;
import io.trino.plugin.datainsight.decoders.VarbinaryDecoder;
import io.trino.plugin.datainsight.decoders.VarcharDecoder;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        property = "@type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = BooleanDecoder.Descriptor.class, name = "boolean"),
        @JsonSubTypes.Type(value = SmallintDecoder.Descriptor.class, name = "smallint"),
        @JsonSubTypes.Type(value = TinyintDecoder.Descriptor.class, name = "tinyint"),
        @JsonSubTypes.Type(value = IntegerDecoder.Descriptor.class, name = "integer"),
        @JsonSubTypes.Type(value = BigintDecoder.Descriptor.class, name = "bigint"),
//        @JsonSubTypes.Type(value = TimestampDecoder.Descriptor.class, name = "timestamp"),
        @JsonSubTypes.Type(value = RealDecoder.Descriptor.class, name = "real"),
        @JsonSubTypes.Type(value = DoubleDecoder.Descriptor.class, name = "double"),
        @JsonSubTypes.Type(value = VarcharDecoder.Descriptor.class, name = "varchar"),
        @JsonSubTypes.Type(value = VarbinaryDecoder.Descriptor.class, name = "varbinary"),
        @JsonSubTypes.Type(value = IpAddressDecoder.Descriptor.class, name = "ipAddress"),
        @JsonSubTypes.Type(value = RowDecoder.Descriptor.class, name = "row"),
        @JsonSubTypes.Type(value = ArrayDecoder.Descriptor.class, name = "array"),
        @JsonSubTypes.Type(value = RawJsonDecoder.Descriptor.class, name = "rawJson"),
        @JsonSubTypes.Type(value = IdColumnDecoder.Descriptor.class, name = "idColumn"),
        @JsonSubTypes.Type(value = ScoreColumnDecoder.Descriptor.class, name = "scoreColumn"),
        @JsonSubTypes.Type(value = SourceColumnDecoder.Descriptor.class, name = "sourceColumn"),
})
public interface DecoderDescriptor
{
    Decoder createDecoder();
}
