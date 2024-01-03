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

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.datainsight.ptf.Query;
import io.trino.plugin.datainsight.ptf.RawQuery;

import io.trino.spi.function.table.ConnectorTableFunction;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.datainsight.DatainsightConfig.Security.AWS;
import static io.trino.plugin.datainsight.DatainsightConfig.Security.PASSWORD;
import static java.util.function.Predicate.isEqual;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class DatainsightConnectorModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(DatainsightConnector.class).in(Scopes.SINGLETON);
        binder.bind(DatainsightMetadata.class).in(Scopes.SINGLETON);
        binder.bind(DatainsightSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(DatainsightRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(DatainsightClient.class).in(Scopes.SINGLETON);
        binder.bind(NodesSystemTable.class).in(Scopes.SINGLETON);
        binder.bind(Cache.class).in(Scopes.SINGLETON);

        newExporter(binder).export(DatainsightClient.class).withGeneratedName();

        configBinder(binder).bindConfig(DatainsightConfig.class);

        newOptionalBinder(binder, AwsSecurityConfig.class);
        newOptionalBinder(binder, PasswordConfig.class);

        newSetBinder(binder, ConnectorTableFunction.class).addBinding().toProvider(RawQuery.class).in(Scopes.SINGLETON);
        newSetBinder(binder, ConnectorTableFunction.class).addBinding().toProvider(Query.class).in(Scopes.SINGLETON);
//        newSetBinder(binder, ConnectorTableFunction.class).addBinding().toProvider(RawQuery.class).in(Scopes.SINGLETON);


//        install(conditionalModule(
//                DatainsightConfig.class,
//                config -> config.getSecurity()
//                        .filter(isEqual(AWS))
//                        .isPresent(),
//                conditionalBinder -> configBinder(conditionalBinder).bindConfig(AwsSecurityConfig.class)));
//
//        install(conditionalModule(
//                DatainsightConfig.class,
//                config -> config.getSecurity()
//                        .filter(isEqual(PASSWORD))
//                        .isPresent(),
//                conditionalBinder -> configBinder(conditionalBinder).bindConfig(PasswordConfig.class)));
    }
}
