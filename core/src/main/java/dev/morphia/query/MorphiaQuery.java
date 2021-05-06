package dev.morphia.query;

import com.mongodb.ExplainVerbosity;
import com.mongodb.client.ClientSession;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.geojson.Point;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.lang.Nullable;
import dev.morphia.Datastore;
import dev.morphia.DeleteOptions;
import dev.morphia.aggregation.experimental.Aggregation;
import dev.morphia.aggregation.experimental.AggregationOptions;
import dev.morphia.aggregation.experimental.stages.Lookup;
import dev.morphia.aggregation.experimental.stages.Sort;
import dev.morphia.annotations.Reference;
import dev.morphia.internal.MorphiaInternals.DriverVersion;
import dev.morphia.mapping.Mapper;
import dev.morphia.mapping.codec.pojo.EntityModel;
import dev.morphia.mapping.codec.writer.DocumentWriter;
import dev.morphia.query.experimental.filters.Filter;
import dev.morphia.query.experimental.filters.Filters;
import dev.morphia.query.experimental.filters.NearFilter;
import dev.morphia.query.experimental.updates.UpdateOperator;
import dev.morphia.query.internal.MorphiaCursor;
import dev.morphia.query.internal.MorphiaKeyCursor;
import dev.morphia.sofia.Sofia;
import org.bson.Document;
import org.bson.codecs.EncoderContext;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;

import static com.mongodb.CursorType.NonTailable;
import static dev.morphia.aggregation.experimental.codecs.ExpressionHelper.document;
import static dev.morphia.aggregation.experimental.stages.Projection.of;
import static dev.morphia.internal.MorphiaInternals.tryInvoke;
import static dev.morphia.query.Meta.META;
import static dev.morphia.query.experimental.filters.Filters.text;
import static java.lang.String.format;

/**
 * @param <T> the type
 * @morphia.internal
 */
class MorphiaQuery<T> implements Query<T> {
    private static final Logger LOG = LoggerFactory.getLogger(MorphiaQuery.class);
    private final Datastore datastore;
    private final Class<T> type;
    private final Mapper mapper;
    private final String collectionName;
    private final MongoCollection<T> collection;
    private final List<Filter> filters = new ArrayList<>();
    private final Document seedQuery;
    private boolean validate = true;

    protected MorphiaQuery(Datastore datastore, @Nullable String collectionName, Class<T> type) {
        this.type = type;
        this.datastore = datastore;
        mapper = this.datastore.getMapper();
        seedQuery = null;
        if (collectionName != null) {
            this.collection = datastore.getDatabase().getCollection(collectionName, type);
            this.collectionName = collectionName;
        } else if (mapper.isMappable(type)) {
            this.collection = mapper.getCollection(type);
            this.collectionName = this.collection.getNamespace().getCollectionName();
        } else {
            this.collection = null;
            this.collectionName = null;
        }
    }

    protected MorphiaQuery(Datastore datastore, Class<T> type, Document query) {
        this.type = type;
        this.datastore = datastore;
        this.seedQuery = query;
        mapper = this.datastore.getMapper();
        collection = mapper.getCollection(type);
        collectionName = collection.getNamespace().getCollectionName();
    }

    static <V> V legacyOperation() {
        throw new UnsupportedOperationException(Sofia.legacyOperation());
    }

    @Override
    public long count() {
        return count(new CountOptions());
    }

    @Override
    public long count(CountOptions options) {
        ClientSession session = datastore.findSession(options);
        Document query = getQueryDocument();
        return session == null ? getCollection().countDocuments(query, options)
                               : getCollection().countDocuments(session, query, options);
    }

    @Override
    public DeleteResult delete(DeleteOptions options) {
        MongoCollection<T> collection = options.prepare(getCollection());
        ClientSession session = datastore.findSession(options);
        if (options.isMulti()) {
            return session == null
                   ? collection.deleteMany(getQueryDocument(), options)
                   : collection.deleteMany(session, getQueryDocument(), options);
        } else {
            return session == null
                   ? collection.deleteOne(getQueryDocument(), options)
                   : collection.deleteOne(session, getQueryDocument(), options);
        }
    }

    @Override
    public Query<T> disableValidation() {
        validate = false;
        return this;
    }

    @Override
    public Query<T> enableValidation() {
        validate = true;
        return this;
    }

    @Override
    public Map<String, Object> explain(FindOptions options, @Nullable ExplainVerbosity verbosity) {
        return tryInvoke(DriverVersion.v4_2_0,
            () -> {
                return verbosity == null
                       ? iterable(options, collection).explain()
                       : iterable(options, collection).explain(verbosity);
            },
            () -> {
                return new LinkedHashMap<>(datastore.getDatabase()
                                                    .runCommand(new Document("explain",
                                                        new Document("find", getCollection().getNamespace().getCollectionName())
                                                            .append("filter", getQueryDocument()))));
            });
    }

    @Override
    @SuppressWarnings({"removal", "unchecked"})
    public FieldEnd<? extends Query<T>> field(String name) {
        return new MorphiaQueryFieldEnd(name);
    }

    @Override
    @SuppressWarnings({"removal"})
    public Query<T> filter(String condition, Object value) {
        final String[] parts = condition.trim().split(" ");
        if (parts.length < 1 || parts.length > 6) {
            throw new IllegalArgumentException("'" + condition + "' is not a legal filter condition");
        }

        final FilterOperator op = (parts.length == 2) ? FilterOperator.fromString(parts[1]) : FilterOperator.EQUAL;

        return filter(op.apply(parts[0].trim(), value));
    }

    @Override
    public Query<T> filter(Filter... additional) {
        for (Filter filter : additional) {
            filters.add(filter
                            .entityType(getEntityClass())
                            .isValidating(validate));
        }
        return this;
    }

    @Override
    public T findAndDelete(FindAndDeleteOptions options) {
        MongoCollection<T> mongoCollection = options.prepare(getCollection());
        ClientSession session = datastore.findSession(options);
        return session == null
               ? mongoCollection.findOneAndDelete(getQueryDocument(), options)
               : mongoCollection.findOneAndDelete(session, getQueryDocument(), options);
    }

    @Override
    public T first() {
        return first(new FindOptions());
    }

    @Override
    public T first(FindOptions options) {
        try (MongoCursor<T> it = iterator(options.copy().limit(1))) {
            return it.tryNext();
        }
    }

    @Override
    public Class<T> getEntityClass() {
        return type;
    }

    @Override
    public MorphiaCursor<T> iterator(FindOptions options) {
        EntityModel entityModel = getEntityModel();
        if (mapper.getOptions().isFetchReferencesViaAggregation()
            && entityModel != null && entityModel.hasReferences()) {
            return aggregate(options);
        } else {
            return new MorphiaCursor<>(prepareCursor(options, getCollection()));
        }
    }

    @NotNull
    private MorphiaCursor<T> aggregate(FindOptions options) {
        Aggregation<T> aggregation = datastore.aggregate(getEntityClass());
        if (!filters.isEmpty()) {
            aggregation.match(filters.toArray(new Filter[0]));
        }

        Projection projection = options.projection();
        List<String> includes = projection.includes();
        if (includes != null) {
            includes.forEach(include -> {
                aggregation.project(of().include(include));
            });
        }
        List<String> excludes = projection.excludes();
        if (excludes != null) {
            excludes.forEach(exclude -> {
                aggregation.project(of().exclude(exclude));
            });
        }
        if (options.getSort() != null) {
            Document document = options.getSort();
            Sort sort = Sort.on();
            for (Entry<String, Object> entry : document.entrySet()) {
                Object value = entry.getValue();
                if (value.equals(1)) {
                    sort.ascending(entry.getKey());
                } else if (value.equals(-1)) {
                    sort.descending(entry.getKey());
                } else if (value instanceof Document && ((Document) value).keySet().equals(Set.of(META))) {
                    sort.meta(entry.getKey());
                } else {
                    throw new UnsupportedOperationException("unmapped sort option: " + value);
                }
            }

            aggregation.sort(sort);

        }

        getEntityModel().references().forEach(model -> {
            Reference reference = model.getAnnotation(Reference.class);
            if (reference != null) {
                boolean lazy = reference.lazy();
                if (!lazy) {
                    aggregation.lookup(Lookup.from(model.getNormalizedType())
                                             .foreignField("_id")
                                             .localField(model.getMappedName() + (reference.idOnly() ? "" : ".$id"))
                                             .as(model.getMappedName()));
                }
            }
        });
        //        Document document = aggregation.execute(Document.class, new AggregationOptions(options)).tryNext();
        return aggregation.execute(getEntityClass(), new AggregationOptions(options));
    }

    @Nullable
    private EntityModel getEntityModel() {
        return mapper.isMappable(type) ? mapper.getEntityModel(type) : null;
    }

    @Override
    public MorphiaKeyCursor<T> keys() {
        return keys(new FindOptions());
    }

    @Override
    public MorphiaKeyCursor<T> keys(FindOptions options) {
        FindOptions includeId = new FindOptions().copy(options)
                                                 .projection()
                                                 .include("_id");

        return new MorphiaKeyCursor<>(prepareCursor(includeId,
            datastore.getDatabase().getCollection(getCollectionName())), datastore.getMapper(),
            type, getCollectionName());
    }

    @Override
    public Modify<T> modify(UpdateOperator first, UpdateOperator... updates) {
        return new Modify<>(datastore, mapper, getCollection(), this, getEntityClass(), first, updates);
    }

    @Override
    public Query<T> search(String searchText) {
        return filter(text(searchText));
    }

    @Override
    public Query<T> search(String searchText, String language) {
        return filter(text(searchText).language(language));
    }

    /**
     * Converts the query to a Document and updates for any discriminator values as my be necessary
     *
     * @return the query
     * @morphia.internal
     */
    @Override
    public Document toDocument() {
        return getQueryDocument();
    }

    @Override
    public Update<T> update(UpdateOperator first, UpdateOperator... updates) {
        return new Update<>(datastore, mapper, getCollection(), this, type, first, updates);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, validate, getCollectionName());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MorphiaQuery)) {
            return false;
        }
        final MorphiaQuery<?> query20 = (MorphiaQuery<?>) o;
        return validate == query20.validate
               && Objects.equals(type, query20.type)
               && Objects.equals(getCollectionName(), query20.getCollectionName());
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", MorphiaQuery.class.getSimpleName() + "[", "]")
                   .add("clazz=" + type.getSimpleName())
                   .add("query=" + getQueryDocument())
                   .toString();
    }

    /**
     * @return the collection this query targets
     * @morphia.internal
     */
    private MongoCollection<T> getCollection() {
        return collection;
    }

    private String getCollectionName() {
        return collectionName;
    }

    @NotNull
    private <E> FindIterable<E> iterable(FindOptions findOptions, MongoCollection<E> collection) {
        final Document query = toDocument();

        if (LOG.isTraceEnabled()) {
            LOG.trace(format("Running query(%s) : %s, options: %s,", getCollectionName(), query, findOptions));
        }

        if ((findOptions.getCursorType() != null && findOptions.getCursorType() != NonTailable)
            && (findOptions.getSort() != null)) {
            LOG.warn("Sorting on tail is not allowed.");
        }

        ClientSession clientSession = datastore.findSession(findOptions);

        MongoCollection<E> updated = findOptions.prepare(collection);

        return clientSession != null
               ? updated.find(clientSession, query)
               : updated.find(query);
    }

    @SuppressWarnings("ConstantConditions")
    private <E> MongoCursor<E> prepareCursor(FindOptions findOptions, MongoCollection<E> collection) {
        Document oldProfile = null;
        if (findOptions.isLogQuery()) {
            oldProfile = datastore.getDatabase().runCommand(new Document("profile", 2).append("slowms", 0));
        }
        try {
            return findOptions
                       .apply(iterable(findOptions, collection), mapper, type)
                       .iterator();
        } finally {
            if (findOptions.isLogQuery()) {
                datastore.getDatabase().runCommand(new Document("profile", oldProfile.get("was"))
                                                       .append("slowms", oldProfile.get("slowms"))
                                                       .append("sampleRate", oldProfile.get("sampleRate")));
            }

        }
    }

    Document getQueryDocument() {
        DocumentWriter writer = new DocumentWriter(seedQuery);
        document(writer, () -> {
            EncoderContext context = EncoderContext.builder().build();
            for (Filter filter : filters) {
                filter.encode(mapper, writer, context);
            }
        });

        Document query = writer.getDocument();
        if (mapper.isMappable(getEntityClass())) {
            mapper.updateQueryWithDiscriminators(mapper.getEntityModel(getEntityClass()), query);
        }

        return query;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Deprecated(since = "2.0", forRemoval = true)
    private class MorphiaQueryFieldEnd extends FieldEndImpl {
        private final String name;

        private MorphiaQueryFieldEnd(String name) {
            super(mapper, name, MorphiaQuery.this, mapper.getEntityModel(getEntityClass()), validate);
            this.name = name;
        }

        @Override
        @SuppressWarnings("removal")
        public CriteriaContainer within(Shape shape) {
            Filter converted;
            if (shape instanceof dev.morphia.query.Shape.Center) {
                final dev.morphia.query.Shape.Center center = (dev.morphia.query.Shape.Center) shape;
                converted = Filters.center(getField(), center.getCenter(), center.getRadius());
            } else if (shape.getGeometry().equals("$box")) {
                Point[] points = shape.getPoints();
                converted = Filters.box(getField(), points[0], points[1]);
            } else if (shape.getGeometry().equals("$polygon")) {
                converted = Filters.polygon(getField(), shape.getPoints());
            } else {
                throw new UnsupportedOperationException(Sofia.conversionNotSupported(shape.getGeometry()));
            }
            if (isNot()) {
                converted.not();
            }
            filter(converted);
            return MorphiaQuery.this;
        }

        @Override
        @SuppressWarnings("removal")
        protected MorphiaQuery<T> addCriteria(FilterOperator op, Object val, boolean not) {
            Filter converted = op.apply(name, val);
            if (not) {
                converted.not();
            }
            filter(converted);
            return MorphiaQuery.this;
        }

        @Override
        @SuppressWarnings("removal")
        protected CriteriaContainer addGeoCriteria(FilterOperator op, Object val, Map opts) {
            NearFilter apply = (NearFilter) op.apply(name, val);
            apply.applyOpts(opts);
            filter(apply);
            return MorphiaQuery.this;
        }
    }
}
