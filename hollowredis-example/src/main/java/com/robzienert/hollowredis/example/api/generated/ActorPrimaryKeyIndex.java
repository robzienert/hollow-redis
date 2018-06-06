package com.robzienert.hollowredis.example.api.generated;

import com.netflix.hollow.api.consumer.HollowConsumer;
import com.netflix.hollow.api.consumer.index.AbstractHollowUniqueKeyIndex;
import com.netflix.hollow.core.schema.HollowObjectSchema;

@SuppressWarnings("all")
public class ActorPrimaryKeyIndex extends AbstractHollowUniqueKeyIndex<MovieAPI, Actor> {

    public ActorPrimaryKeyIndex(HollowConsumer consumer) {
        this(consumer, true);    }

    public ActorPrimaryKeyIndex(HollowConsumer consumer, boolean isListenToDataRefresh) {
        this(consumer, isListenToDataRefresh, ((HollowObjectSchema)consumer.getStateEngine().getSchema("Actor")).getPrimaryKey().getFieldPaths());
    }

    public ActorPrimaryKeyIndex(HollowConsumer consumer, String... fieldPaths) {
        this(consumer, true, fieldPaths);
    }

    public ActorPrimaryKeyIndex(HollowConsumer consumer, boolean isListenToDataRefresh, String... fieldPaths) {
        super(consumer, "Actor", isListenToDataRefresh, fieldPaths);
    }

    public Actor findMatch(Object... keys) {
        int ordinal = idx.getMatchingOrdinal(keys);
        if(ordinal == -1)
            return null;
        return api.getActor(ordinal);
    }

}