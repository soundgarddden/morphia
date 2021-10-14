package dev.morphia.aggregation.experimental;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Collation;
import com.mongodb.lang.Nullable;
import dev.morphia.internal.ReadConfigurable;
import dev.morphia.internal.SessionConfigurable;
import dev.morphia.internal.WriteConfigurable;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Defines options to be applied to an aggregation pipeline.
 */
@SuppressWarnings("unused")
public class AggregationOptions implements SessionConfigurable<AggregationOptions>, ReadConfigurable<AggregationOptions>,
                                               WriteConfigurable<AggregationOptions> {
    private boolean allowDiskUse;
    private Integer batchSize;
    private boolean bypassDocumentValidation;
    private Collation collation;
    private long maxTime;
    private TimeUnit maxTimeUnit;
    private ClientSession clientSession;
    private ReadPreference readPreference;
    private ReadConcern readConcern;
    private WriteConcern writeConcern;
    private Document hint;
    private String comment;
    private long maxAwaitTime;
    private TimeUnit maxAwaitTimeUnit;

    /**
     * @return the configuration value
     */
    public boolean allowDiskUse() {
        return allowDiskUse;
    }

    /**
     * Enables writing to temporary files.
     *
     * @param allowDiskUse true to enable
     * @return this
     */
    public AggregationOptions allowDiskUse(boolean allowDiskUse) {
        this.allowDiskUse = allowDiskUse;
        return this;
    }

    /**
     * Applies the configured options to the collection.
     *
     * @param documents  the stage documents
     * @param collection the collection to configure
     * @param resultType the result type
     * @param <T>        the collection type
     * @param <S>        the result type
     * @return the updated collection
     * @morphia.internal
     */
    public <S, T> AggregateIterable<S> apply(List<Document> documents, MongoCollection<T> collection,
                                             Class<S> resultType) {
        MongoCollection<T> bound = collection;
        if (readConcern != null) {
            bound = bound.withReadConcern(readConcern);
        }
        if (readPreference != null) {
            bound = bound.withReadPreference(readPreference);
        }
        AggregateIterable<S> aggregate = bound.aggregate(documents, resultType)
                                              .allowDiskUse(allowDiskUse)
                                              .bypassDocumentValidation(bypassDocumentValidation);
        if (batchSize != null) {
            aggregate.batchSize(batchSize);
        }
        if (collation != null) {
            aggregate.collation(collation);
        }
        if (maxAwaitTimeUnit != null) {
            aggregate.maxAwaitTime(maxAwaitTime, maxAwaitTimeUnit);
        }
        if (maxTimeUnit != null) {
            aggregate.maxTime(maxTime, maxTimeUnit);
        }
        if (hint != null) {
            aggregate.hint(hint);
        }

        return aggregate;
    }

    /**
     * @return the configuration value
     */
    public int batchSize() {
        return batchSize;
    }

    /**
     * Sets the batch size for fetching results.
     *
     * @param batchSize the size
     * @return this
     */
    public AggregationOptions batchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    /**
     * @return the configuration value
     */
    public boolean bypassDocumentValidation() {
        return bypassDocumentValidation;
    }

    /**
     * Enables the aggregation to bypass document validation during the operation. This lets you insert documents that do not
     * meet the validation requirements.
     * <p>
     * Applicable only if you specify the $out or $merge aggregation stages.
     *
     * @param bypassDocumentValidation true to enable the bypass
     * @return this
     */
    public AggregationOptions bypassDocumentValidation(boolean bypassDocumentValidation) {
        this.bypassDocumentValidation = bypassDocumentValidation;
        return this;
    }

    @Override
    public AggregationOptions clientSession(ClientSession clientSession) {
        this.clientSession = clientSession;
        return this;
    }

    @Override
    public ClientSession clientSession() {
        return clientSession;
    }

    /**
     * @return the configuration value
     */
    public Collation collation() {
        return collation;
    }

    /**
     * Specifies the collation to use for the operation.
     * <p>
     * Collation allows users to specify language-specific rules for string comparison, such as rules for letter case and accent marks.
     *
     * @param collation the collation to use
     * @return this
     */
    public AggregationOptions collation(Collation collation) {
        this.collation = collation;
        return this;
    }

    /**
     * @return the comment on the aggregation.  Might be null;
     * @since 2.3
     */
    @Nullable
    public String comment() {
        return comment;
    }

    /**
     * Sets the comment to the aggregation. A null value means no comment is set.
     *
     * @param comment the comment
     * @return this
     * @since 2.3
     */
    public AggregationOptions comment(@Nullable String comment) {
        this.comment = comment;
        return this;
    }

    /**
     * @return the configuration value
     */
    public boolean getAllowDiskUse() {
        return allowDiskUse;
    }

    /**
     * @return the configuration value
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * @return the configuration value
     */
    public boolean getBypassDocumentValidation() {
        return bypassDocumentValidation;
    }

    /**
     * @return the configuration value
     */
    public Collation getCollation() {
        return collation;
    }

    /**
     * @return the configuration value
     */
    public long getMaxTimeMS() {
        return TimeUnit.MILLISECONDS.convert(maxTime, maxTimeUnit);
    }

    /**
     * @return the configuration value
     */
    public ReadConcern getReadConcern() {
        return readConcern;
    }

    /**
     * @return the configuration value
     */
    public ReadPreference getReadPreference() {
        return readPreference;
    }

    @Override
    public <C> MongoCollection<C> prepare(MongoCollection<C> collection) {
        MongoCollection<C> updated = collection;
        WriteConcern writeConcern = writeConcern();
        if (writeConcern != null) {
            updated = updated.withWriteConcern(writeConcern);
        }
        ReadConcern readConcern = getReadConcern();
        if (readConcern != null) {
            updated = updated.withReadConcern(readConcern);
        }
        ReadPreference readPreference = getReadPreference();
        if (readPreference != null) {
            updated = updated.withReadPreference(readPreference);
        }

        return updated;
    }

    /**
     * Specifies the read concern.
     *
     * @param readConcern the read concern to use
     * @return this
     */
    public AggregationOptions readConcern(ReadConcern readConcern) {
        this.readConcern = readConcern;
        return this;
    }

    /**
     * Sets the read preference to use
     *
     * @param readPreference the read preference
     * @return this
     */
    public AggregationOptions readPreference(ReadPreference readPreference) {
        this.readPreference = readPreference;
        return this;
    }

    /**
     * @return the hint for which index to use. A null value means no hint is set.
     * @mongodb.server.release 3.6
     * @since 2.0
     */
    public Document hint() {
        return hint;
    }

    /**
     * Sets the hint for which index to use. A null value means no hint is set.
     *
     * @param hint the hint
     * @return this
     * @mongodb.server.release 3.6
     * @since 2.0
     */
    public AggregationOptions hint(String hint) {
        this.hint = new Document("hint", hint);
        return this;
    }

    /**
     * The maximum amount of time to wait on new documents to satisfy a {@code $changeStream} aggregation.
     *
     * @param maxAwaitTime the max await time
     * @param timeUnit     the time unit to return the result in
     * @return this
     * @since 2.3
     */
    public AggregationOptions maxAwaitTime(long maxAwaitTime, TimeUnit timeUnit) {
        this.maxAwaitTime = maxAwaitTime;
        this.maxAwaitTimeUnit = timeUnit;
        return this;
    }

    /**
     * @param unit the unit to represent the max await time in
     * @return the max await time in the unit provided
     * @since 2.3
     */
    public long maxAwaitTime(TimeUnit unit) {
        return unit.convert(maxAwaitTime, maxAwaitTimeUnit);
    }

    /**
     * Sets the maximum execution time for this operation.
     *
     * @param maxTime  the max time
     * @param timeUnit the time unit
     * @return this
     * @since 2.3
     */
    public AggregationOptions maxTime(long maxTime, TimeUnit timeUnit) {
        this.maxTime = maxTime;
        this.maxTimeUnit = timeUnit;
        return this;
    }

    /**
     * @param unit the unit to represent the max time in
     * @return the max time in the unit provided
     * @since 2.3
     */
    public long maxTime(TimeUnit unit) {
        return unit.convert(maxTime, maxTimeUnit);
    }

    /**
     * @return the configuration value
     * @deprecated use {@link #maxTime(TimeUnit)}
     */
    @Deprecated(forRemoval = true)
    public long maxTimeMS() {
        return TimeUnit.MILLISECONDS.convert(maxTime, maxTimeUnit);
    }

    /**
     * Specifies a time limit in milliseconds for processing operations on a cursor. If you do not specify a value for maxTimeMS,
     * operations will not time out. A value of 0 explicitly specifies the default unbounded behavior.
     *
     * @param maxTimeMS the max time in milliseconds
     * @return this
     * @deprecated use {@link #maxTime(long, TimeUnit)}
     */
    @Deprecated(forRemoval = true)
    public AggregationOptions maxTimeMS(long maxTimeMS) {
        return maxTime(maxTimeMS, TimeUnit.MILLISECONDS);
    }

    /**
     * @return the configuration value
     */
    public ReadConcern readConcern() {
        return readConcern;
    }

    /**
     * @return the configuration value
     */
    public ReadPreference readPreference() {
        return readPreference;
    }

    /**
     * Sets the write concern to use
     *
     * @param writeConcern the write concern
     * @return this
     */
    public AggregationOptions writeConcern(@Nullable WriteConcern writeConcern) {
        this.writeConcern = writeConcern;
        return this;
    }

    /**
     * @return the configuration value
     */
    @Nullable
    public WriteConcern writeConcern() {
        return writeConcern;
    }

    AggregationOptions allowDiskUse(Boolean allowDiskUse) {
        throw new UnsupportedOperationException();
    }

    AggregationOptions bypassDocumentValidation(Boolean bypassDocumentValidation) {
        throw new UnsupportedOperationException();
    }

    AggregationOptions hint(Bson hint) {
        throw new UnsupportedOperationException();
    }
}
