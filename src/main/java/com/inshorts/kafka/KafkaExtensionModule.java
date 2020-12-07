package com.inshorts.kafka;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import org.apache.druid.initialization.DruidModule;
import com.inshorts.kafka.indexer.*;

import java.util.List;

public class KafkaExtensionModule implements DruidModule {
    @Override
    public List<? extends Module> getJacksonModules() {
        // Register Jackson module for any classes we need to be able to use in JSON queries or ingestion specs.
        return ImmutableList.of(
                new SimpleModule(getClass().getSimpleName()).registerSubtypes(
                        new NamedType(NewBatchInputRowParser.class, NewBatchInputRowParser.TYPE_NAME)
                )
        );
    }

    @Override
    public void configure(Binder binder) {
    }
}
