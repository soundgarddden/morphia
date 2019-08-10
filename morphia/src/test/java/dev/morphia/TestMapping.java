/*
  Copyright (C) 2010 Olafur Gauti Gudmundsson
  <p/>
  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may
  obtain a copy of the License at
  <p/>
  http://www.apache.org/licenses/LICENSE-2.0
  <p/>
  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
  and limitations under the License.
 */


package dev.morphia;


import com.mongodb.DBRef;
import com.mongodb.client.MongoCollection;
import dev.morphia.TestInheritanceMappings.MapLike;
import dev.morphia.annotations.AlsoLoad;
import dev.morphia.annotations.Embedded;
import dev.morphia.annotations.Entity;
import dev.morphia.annotations.Id;
import dev.morphia.mapping.DefaultCreator;
import dev.morphia.mapping.Mapper;
import dev.morphia.mapping.MapperOptions;
import dev.morphia.mapping.MappingException;
import dev.morphia.query.FindOptions;
import dev.morphia.testmodel.Address;
import dev.morphia.testmodel.Article;
import dev.morphia.testmodel.Circle;
import dev.morphia.testmodel.Hotel;
import dev.morphia.testmodel.PhoneNumber;
import dev.morphia.testmodel.Rectangle;
import dev.morphia.testmodel.RecursiveChild;
import dev.morphia.testmodel.RecursiveParent;
import dev.morphia.testmodel.Translation;
import dev.morphia.testmodel.TravelAgency;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.Ignore;
import org.junit.Test;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.Vector;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


@SuppressWarnings("unchecked")
public class TestMapping extends TestBase {

    @Test
    public void testAlsoLoad() {
        final ContainsIntegerList cil = new ContainsIntegerList();
        cil.intList.add(1);
        getDs().save(cil);
        final ContainsIntegerList cilLoaded = getDs().get(cil);
        assertNotNull(cilLoaded);
        assertNotNull(cilLoaded.intList);
        assertEquals(cilLoaded.intList.size(), cil.intList.size());
        assertEquals(cilLoaded.intList.get(0), cil.intList.get(0));

        final ContainsIntegerListNew cilNew = getDs().find(ContainsIntegerListNew.class).filter("_id", cil.id).first();
        assertNotNull(cilNew);
        assertNotNull(cilNew.integers);
        assertEquals(1, cilNew.integers.size());
        assertEquals(1, (int) cil.intList.get(0));
    }

    @Test
    public void testBadMappings() {
        try {
            getMapper().map(MissingId.class);
            fail("Validation: Missing @Id field not caught");
        } catch (MappingException e) {
            // good
        }

        try {
            getMapper().map(IdOnEmbedded.class);
            fail("Validation: @Id field on @Embedded not caught");
        } catch (MappingException e) {
            // good
        }

        try {
            getMapper().map(RenamedEmbedded.class);
            fail("Validation: @Embedded(\"name\") not caught on Class");
        } catch (MappingException e) {
            // good
        }

        try {
            getMapper().map(MissingIdStill.class);
            fail("Validation: Missing @Id field not not caught");
        } catch (MappingException e) {
            // good
        }

        try {
            getMapper().map(MissingIdRenamed.class);
            fail("Validation: Missing @Id field not not caught");
        } catch (MappingException e) {
            // good
        }

        try {
            getMapper().map(NonStaticInnerClass.class);
            fail("Validation: Non-static inner class allowed");
        } catch (MappingException e) {
            // good
        }
    }

    @Test
    public void testBaseEntityValidity() {
        getMapper().map(UsesBaseEntity.class);
    }

    @Test
    public void testBasicMapping() {
        performBasicMappingTest();
        final DefaultCreator objectFactory = (DefaultCreator) getMapper().getOptions().getCreator();
        assertTrue(objectFactory.getClassNameCache().isEmpty());
    }

    @Test
    public void testBasicMappingWithCachedClasses() {
        MapperOptions options = MapperOptions.builder(getMapper().getOptions())
                                             .cacheClassLookups(true)
                                             .build();
        final Datastore datastore = Morphia.createDatastore(getMongoClient(), getDatabase().getName(), options);
        performBasicMappingTest();

        final DefaultCreator objectFactory = (DefaultCreator) getMapper().getOptions().getCreator();
        assertTrue(objectFactory.getClassNameCache().containsKey(Hotel.class.getName()));
        assertTrue(objectFactory.getClassNameCache().containsKey(TravelAgency.class.getName()));
    }

    @Test
    public void testByteArrayMapping() {
        getMapper().map(ContainsByteArray.class);
        final Key<ContainsByteArray> savedKey = getDs().save(new ContainsByteArray());
        final ContainsByteArray loaded = getDs().find(ContainsByteArray.class).filter("_id", savedKey.getId()).first();
        assertEquals(new String((new ContainsByteArray()).bytes), new String(loaded.bytes));
        assertNotNull(loaded.id);
    }

    @Test
    public void testCollectionMapping() {
        getMapper().map(ContainsCollection.class);
        final Key<ContainsCollection> savedKey = getDs().save(new ContainsCollection());
        final ContainsCollection loaded = getDs().find(ContainsCollection.class).filter("_id", savedKey.getId()).first();
        assertEquals(loaded.coll, (new ContainsCollection()).coll);
        assertNotNull(loaded.id);
    }

    @Test
    public void testDbRefMapping() {
        getMapper().map(Rectangle.class);
        final MongoCollection<Document> stuff = getDatabase().getCollection("stuff");
        final MongoCollection<Document> rectangles = getDatabase().getCollection("rectangles");

        final Rectangle r = new Rectangle(1, 1);
        final Document rDocument = getMapper().toDocument(r);
        rDocument.put("_ns", rectangles.getNamespace().getCollectionName());
        rectangles.insertOne(rDocument);

        final ContainsRef cRef = new ContainsRef();
        cRef.rect = new DBRef((String) rDocument.get("_ns"), rDocument.get("_id"));
        final Document cRefDocument = getMapper().toDocument(cRef);
        stuff.insertOne(cRefDocument);
        final Document cRefDocumentLoaded = (Document) stuff.find(new Document("_id", cRefDocument.get("_id")));
        final ContainsRef cRefLoaded = getMapper().fromDocument(ContainsRef.class, cRefDocumentLoaded);
        assertNotNull(cRefLoaded);
        assertNotNull(cRefLoaded.rect);
        assertNotNull(cRefLoaded.rect.getId());
        assertNotNull(cRefLoaded.rect.getCollectionName());
        assertEquals(cRefLoaded.rect.getId(), cRef.rect.getId());
        assertEquals(cRefLoaded.rect.getCollectionName(), cRef.rect.getCollectionName());
    }

    @Test
    public void testEmbeddedArrayElementHasNoClassname() {
        getMapper().map(ContainsEmbeddedArray.class);
        final ContainsEmbeddedArray cea = new ContainsEmbeddedArray();
        cea.res = new RenamedEmbedded[]{new RenamedEmbedded()};

        final Document document = getMapper().toDocument(cea);
        List<Document> res = (List<Document>) document.get("res");
        assertFalse(res.get(0).containsKey(getMapper().getOptions().getDiscriminatorField()));
    }

    @Test
    public void testEmbeddedDocument() {
        getMapper().map(ContainsDocument.class);
        getDs().save(new ContainsDocument());
        assertNotNull(getDs().find(ContainsDocument.class)
                             .execute(new FindOptions().limit(1))
                             .next());
    }

    @Test
    public void testEmbeddedEntity() {
        getMapper().map(ContainsEmbeddedEntity.class);
        getDs().save(new ContainsEmbeddedEntity());
        final ContainsEmbeddedEntity ceeLoaded = getDs().find(ContainsEmbeddedEntity.class)
                                                        .execute(new FindOptions().limit(1))
                                                        .next();
        assertNotNull(ceeLoaded);
        assertNotNull(ceeLoaded.id);
        assertNotNull(ceeLoaded.cil);
        assertNull(ceeLoaded.cil.id);

    }

    @Test
    public void testEmbeddedEntityDocumentHasNoClassname() {
        getMapper().map(ContainsEmbeddedEntity.class);
        final ContainsEmbeddedEntity cee = new ContainsEmbeddedEntity();
        cee.cil = new ContainsIntegerList();
        cee.cil.intList = Collections.singletonList(1);
        final Document document = getMapper().toDocument(cee);
        assertFalse(((Document) document.get("cil")).containsKey(getMapper().getOptions().getDiscriminatorField()));
    }

    @Test
    public void testEnumKeyedMap() {
        final ContainsEnum1KeyMap map = new ContainsEnum1KeyMap();
        map.values.put(Enum1.A, "I'm a");
        map.values.put(Enum1.B, "I'm b");
        map.embeddedValues.put(Enum1.A, "I'm a");
        map.embeddedValues.put(Enum1.B, "I'm b");

        final Key<?> mapKey = getDs().save(map);

        final ContainsEnum1KeyMap mapLoaded = getDs().find(ContainsEnum1KeyMap.class).filter("_id", mapKey.getId()).first();

        assertNotNull(mapLoaded);
        assertEquals(2, mapLoaded.values.size());
        assertNotNull(mapLoaded.values.get(Enum1.A));
        assertNotNull(mapLoaded.values.get(Enum1.B));
        assertEquals(2, mapLoaded.embeddedValues.size());
        assertNotNull(mapLoaded.embeddedValues.get(Enum1.A));
        assertNotNull(mapLoaded.embeddedValues.get(Enum1.B));
    }

    @Test
    public void testFinalField() {
        getMapper().map(ContainsFinalField.class);
        final Key<ContainsFinalField> savedKey = getDs().save(new ContainsFinalField("blah"));
        final ContainsFinalField loaded = getDs().find(ContainsFinalField.class).filter("_id", savedKey.getId()).first();
        assertNotNull(loaded);
        assertNotNull(loaded.name);
        assertEquals("blah", loaded.name);
    }

    @Test
    public void testFinalFieldNotPersisted() {
        MapperOptions options = MapperOptions.builder(getMapper().getOptions())
                                             .ignoreFinals(true)
                                             .build();
        final Datastore datastore = Morphia.createDatastore(getMongoClient(), getDatabase().getName(), options);

        getMapper().map(ContainsFinalField.class);
        final Key<ContainsFinalField> savedKey = datastore.save(new ContainsFinalField("blah"));
        final ContainsFinalField loaded = datastore.find(ContainsFinalField.class).filter("_id", savedKey.getId()).first();
        assertNotNull(loaded);
        assertNotNull(loaded.name);
        assertEquals("foo", loaded.name);
    }

    @Test
    public void testFinalIdField() {
        getMapper().map(HasFinalFieldId.class);
        final Key<HasFinalFieldId> savedKey = getDs().save(new HasFinalFieldId(12));
        final HasFinalFieldId loaded = getDs().find(HasFinalFieldId.class).filter("_id", savedKey.getId()).first();
        assertNotNull(loaded);
        assertNotNull(loaded.id);
        assertEquals(12, loaded.id);
    }

    @Test
    @Ignore("need to add this feature")
    @SuppressWarnings("unchecked")
    public void testGenericKeyedMap() {
        final ContainsXKeyMap<Integer> map = new ContainsXKeyMap<>();
        map.values.put(1, "I'm 1");
        map.values.put(2, "I'm 2");

        final Key<ContainsXKeyMap<Integer>> mapKey = getDs().save(map);

        final ContainsXKeyMap<Integer> mapLoaded = getDs().find(ContainsXKeyMap.class).filter("_id", mapKey.getId()).first();

        assertNotNull(mapLoaded);
        assertEquals(2, mapLoaded.values.size());
        assertNotNull(mapLoaded.values.get(1));
        assertNotNull(mapLoaded.values.get(2));
    }

    @Test
    public void testIdFieldWithUnderscore() {
        getMapper().map(StrangelyNamedIdField.class);
    }

    @Test
    public void testIntKeySetStringMap() {
        final ContainsIntKeySetStringMap map = new ContainsIntKeySetStringMap();
        map.values.put(1, Collections.singleton("I'm 1"));
        map.values.put(2, Collections.singleton("I'm 2"));

        final Key<?> mapKey = getDs().save(map);

        final ContainsIntKeySetStringMap mapLoaded = getDs().find(ContainsIntKeySetStringMap.class)
                                                            .filter("_id", mapKey.getId())
                                                            .first();

        assertNotNull(mapLoaded);
        assertEquals(2, mapLoaded.values.size());
        assertNotNull(mapLoaded.values.get(1));
        assertNotNull(mapLoaded.values.get(2));
        assertEquals(1, mapLoaded.values.get(1).size());

        assertNotNull(getDs().find(ContainsIntKeyMap.class).field("values.2").exists());
        assertEquals(0, getDs().find(ContainsIntKeyMap.class).field("values.2").doesNotExist().count());
        assertNotNull(getDs().find(ContainsIntKeyMap.class).field("values.4").doesNotExist());
        assertEquals(0, getDs().find(ContainsIntKeyMap.class).field("values.4").exists().count());
    }

    @Test
    public void testIntKeyedMap() {
        final ContainsIntKeyMap map = new ContainsIntKeyMap();
        map.values.put(1, "I'm 1");
        map.values.put(2, "I'm 2");

        final Key<?> mapKey = getDs().save(map);

        final ContainsIntKeyMap mapLoaded = getDs().find(ContainsIntKeyMap.class).filter("_id", mapKey.getId()).first();

        assertNotNull(mapLoaded);
        assertEquals(2, mapLoaded.values.size());
        assertNotNull(mapLoaded.values.get(1));
        assertNotNull(mapLoaded.values.get(2));

        assertNotNull(getDs().find(ContainsIntKeyMap.class).field("values.2").exists());
        assertEquals(0, getDs().find(ContainsIntKeyMap.class).field("values.2").doesNotExist().count());
        assertNotNull(getDs().find(ContainsIntKeyMap.class).field("values.4").doesNotExist());
        assertEquals(0, getDs().find(ContainsIntKeyMap.class).field("values.4").exists().count());
    }

    @Test
    public void testIntLists() {
        ContainsIntegerList cil = new ContainsIntegerList();
        getDs().save(cil);
        ContainsIntegerList cilLoaded = getDs().get(cil);
        assertNotNull(cilLoaded);
        assertNotNull(cilLoaded.intList);
        assertEquals(cilLoaded.intList.size(), cil.intList.size());


        cil = new ContainsIntegerList();
        cil.intList = null;
        getDs().save(cil);
        cilLoaded = getDs().get(cil);
        assertNotNull(cilLoaded);
        assertNotNull(cilLoaded.intList);
        assertEquals(0, cilLoaded.intList.size());

        cil = new ContainsIntegerList();
        cil.intList.add(1);
        getDs().save(cil);
        cilLoaded = getDs().get(cil);
        assertNotNull(cilLoaded);
        assertNotNull(cilLoaded.intList);
        assertEquals(1, cilLoaded.intList.size());
        assertEquals(1, (int) cilLoaded.intList.get(0));
    }

    @Test
    public void testLongArrayMapping() {
        getMapper().map(ContainsLongAndStringArray.class);
        getDs().save(new ContainsLongAndStringArray());
        ContainsLongAndStringArray loaded = getDs().find(ContainsLongAndStringArray.class)
                                                   .execute(new FindOptions().limit(1))
                                                   .next();
        assertArrayEquals(loaded.longs, (new ContainsLongAndStringArray()).longs);
        assertArrayEquals(loaded.strings, (new ContainsLongAndStringArray()).strings);

        final ContainsLongAndStringArray array = new ContainsLongAndStringArray();
        array.strings = new String[]{"a", "B", "c"};
        array.longs = new Long[]{4L, 5L, 4L};
        final Key<ContainsLongAndStringArray> k1 = getDs().save(array);
        loaded = getDs().getByKey(ContainsLongAndStringArray.class, k1);
        assertArrayEquals(loaded.longs, array.longs);
        assertArrayEquals(loaded.strings, array.strings);

        assertNotNull(loaded.id);
    }

    @Test
    public void testMapLike() {
        final ContainsMapLike ml = new ContainsMapLike();
        ml.m.put("first", "test");
        getDs().save(ml);
        final ContainsMapLike mlLoaded = getDs().find(ContainsMapLike.class)
                                                .execute(new FindOptions().limit(1))
                                                .next();
        assertNotNull(mlLoaded);
        assertNotNull(mlLoaded.m);
        assertNotNull(mlLoaded.m.containsKey("first"));
    }

    @Test
    public void testMapWithEmbeddedInterface() {
        final ContainsMapWithEmbeddedInterface aMap = new ContainsMapWithEmbeddedInterface();
        final Foo f1 = new Foo1();
        final Foo f2 = new Foo2();

        aMap.embeddedValues.put("first", f1);
        aMap.embeddedValues.put("second", f2);
        getDs().save(aMap);

        final ContainsMapWithEmbeddedInterface mapLoaded = getDs().find(ContainsMapWithEmbeddedInterface.class)
                                                                  .execute(new FindOptions().limit(1))
                                                                  .next();

        assertNotNull(mapLoaded);
        assertEquals(2, mapLoaded.embeddedValues.size());
        assertTrue(mapLoaded.embeddedValues.get("first") instanceof Foo1);
        assertTrue(mapLoaded.embeddedValues.get("second") instanceof Foo2);

    }

    @Test
    public void testMaps() {
        final MongoCollection<Document> articles = getDatabase().getCollection("articles");
        getMapper().map(Circle.class);

        final Article related = new Article();
        final Document relatedDocument = getMapper().toDocument(related);
        articles.insertOne(relatedDocument);

        final Article relatedLoaded = getMapper().fromDocument(Article.class,
            articles.find(new Document("_id", relatedDocument.get("_id"))).first());

        final Article article = new Article();
        article.setTranslation("en", new Translation("Hello World", "Just a test"));
        article.setTranslation("is", new Translation("Halló heimur", "Bara að prófa"));

        article.setAttribute("myDate", new Date());
        article.setAttribute("myString", "Test");
        article.setAttribute("myInt", 123);

        article.putRelated("test", relatedLoaded);

        final Document articleDocument = getMapper().toDocument(article);
        articles.insertOne(articleDocument);

        final Article articleLoaded = getMapper().fromDocument(Article.class,
            articles.find(new Document("_id", articleDocument.get("_id"))).first());

        assertEquals(article.getTranslations().size(), articleLoaded.getTranslations().size());
        assertEquals(article.getTranslation("en").getTitle(), articleLoaded.getTranslation("en").getTitle());
        assertEquals(article.getTranslation("is").getBody(), articleLoaded.getTranslation("is").getBody());
        assertEquals(article.getAttributes().size(), articleLoaded.getAttributes().size());
        assertEquals(article.getAttribute("myDate"), articleLoaded.getAttribute("myDate"));
        assertEquals(article.getAttribute("myString"), articleLoaded.getAttribute("myString"));
        assertEquals(article.getAttribute("myInt"), articleLoaded.getAttribute("myInt"));
        assertEquals(article.getRelated().size(), articleLoaded.getRelated().size());
        assertEquals(article.getRelated("test").getId(), articleLoaded.getRelated("test").getId());
    }

    @Test
    public void testObjectIdKeyedMap() {
        getMapper().map(ContainsObjectIdKeyMap.class);
        final ContainsObjectIdKeyMap map = new ContainsObjectIdKeyMap();
        final ObjectId o1 = new ObjectId("111111111111111111111111");
        final ObjectId o2 = new ObjectId("222222222222222222222222");
        map.values.put(o1, "I'm 1s");
        map.values.put(o2, "I'm 2s");

        final Key<?> mapKey = getDs().save(map);

        final ContainsObjectIdKeyMap mapLoaded = getDs().find(ContainsObjectIdKeyMap.class).filter("_id", mapKey.getId()).first();

        assertNotNull(mapLoaded);
        assertEquals(2, mapLoaded.values.size());
        assertNotNull(mapLoaded.values.get(o1));
        assertNotNull(mapLoaded.values.get(o2));

        assertNotNull(getDs().find(ContainsIntKeyMap.class).field("values.111111111111111111111111").exists());
        assertEquals(0, getDs().find(ContainsIntKeyMap.class).field("values.111111111111111111111111").doesNotExist().count());
        assertNotNull(getDs().find(ContainsIntKeyMap.class).field("values.4").doesNotExist());
        assertEquals(0, getDs().find(ContainsIntKeyMap.class).field("values.4").exists().count());
    }

    @Test
    public void testPrimMap() {
        final ContainsPrimitiveMap primMap = new ContainsPrimitiveMap();
        primMap.embeddedValues.put("first", 1L);
        primMap.embeddedValues.put("second", 2L);
        primMap.values.put("first", 1L);
        primMap.values.put("second", 2L);
        final Key<ContainsPrimitiveMap> primMapKey = getDs().save(primMap);

        final ContainsPrimitiveMap primMapLoaded = getDs().find(ContainsPrimitiveMap.class)
                                                          .filter("_id", primMapKey.getId())
                                                          .first();

        assertNotNull(primMapLoaded);
        assertEquals(2, primMapLoaded.embeddedValues.size());
        assertEquals(2, primMapLoaded.values.size());
    }

    @Test
    public void testPrimMapWithNullValue() {
        final ContainsPrimitiveMap primMap = new ContainsPrimitiveMap();
        primMap.embeddedValues.put("first", null);
        primMap.embeddedValues.put("second", 2L);
        primMap.values.put("first", null);
        primMap.values.put("second", 2L);
        final Key<ContainsPrimitiveMap> primMapKey = getDs().save(primMap);

        final ContainsPrimitiveMap primMapLoaded = getDs().find(ContainsPrimitiveMap.class)
                                                          .filter("_id", primMapKey.getId())
                                                          .first();

        assertNotNull(primMapLoaded);
        assertEquals(2, primMapLoaded.embeddedValues.size());
        assertEquals(2, primMapLoaded.values.size());
    }

    @Test
    public void testRecursiveReference() {
        final MongoCollection<Document> stuff = getDatabase().getCollection("stuff");

        getMapper().map(RecursiveChild.class);

        final RecursiveParent parent = new RecursiveParent();
        final Document parentDocument = getMapper().toDocument(parent);
        stuff.insertOne(parentDocument);

        final RecursiveChild child = new RecursiveChild();
        final Document childDocument = getMapper().toDocument(child);
        stuff.insertOne(childDocument);

        final RecursiveParent parentLoaded = getMapper().fromDocument(RecursiveParent.class,
            stuff.find(new Document("_id", parentDocument.get("_id"))).first());
        final RecursiveChild childLoaded = getMapper().fromDocument(RecursiveChild.class,
            stuff.find(new Document("_id", childDocument.get("_id"))).first());

        parentLoaded.setChild(childLoaded);
        childLoaded.setParent(parentLoaded);

        stuff.insertOne(getMapper().toDocument(parentLoaded));
        stuff.insertOne(getMapper().toDocument(childLoaded));

        final RecursiveParent finalParentLoaded = getMapper().fromDocument(RecursiveParent.class,
            stuff.find(new Document("_id", parentDocument.get("_id")))
                 .first());
        final RecursiveChild finalChildLoaded = getMapper().fromDocument(RecursiveChild.class,
            stuff.find(new Document("_id", childDocument.get("_id"))).first());

        assertNotNull(finalParentLoaded.getChild());
        assertNotNull(finalChildLoaded.getParent());
    }

    @Test(expected = MappingException.class)
    public void testReferenceWithoutIdValue() {
        final RecursiveParent parent = new RecursiveParent();
        final RecursiveChild child = new RecursiveChild();
        child.setId(null);
        parent.setChild(child);
        getDs().save(parent);

    }

    @Test
    public void testTimestampMapping() {
        getMapper().map(ContainsTimestamp.class);
        final ContainsTimestamp cts = new ContainsTimestamp();
        final Key<ContainsTimestamp> savedKey = getDs().save(cts);
        final ContainsTimestamp loaded = getDs().find(ContainsTimestamp.class).filter("_id", savedKey.getId()).first();
        assertNotNull(loaded.ts);
        assertEquals(loaded.ts.getTime(), cts.ts.getTime());

    }

    @Test
    public void testUUID() {
        //       getMorphia().map(ContainsUUID.class);
        final ContainsUUID uuid = new ContainsUUID();
        final UUID before = uuid.uuid;
        getDs().save(uuid);
        final ContainsUUID loaded = getDs().find(ContainsUUID.class)
                                           .execute(new FindOptions().limit(1))
                                           .next();
        assertNotNull(loaded);
        assertNotNull(loaded.id);
        assertNotNull(loaded.uuid);
        assertEquals(before, loaded.uuid);
    }

    @Test
    public void testUuidId() {
        getMapper().map(ContainsUuidId.class);
        final ContainsUuidId uuidId = new ContainsUuidId();
        final UUID before = uuidId.id;
        getDs().save(uuidId);
        final ContainsUuidId loaded = getDs().find(ContainsUuidId.class).filter("_id", before).first();
        assertNotNull(loaded);
        assertNotNull(loaded.id);
        assertEquals(before, loaded.id);
    }

    @SuppressWarnings("unchecked")
    private void performBasicMappingTest() {
        final MongoCollection<Document> hotels = getDatabase().getCollection("hotels");
        final MongoCollection<Document> agencies = getDatabase().getCollection("agencies");

        Mapper mapper = getDs().getMapper();
        mapper.map(Hotel.class);
        mapper.map(TravelAgency.class);

        final Hotel borg = new Hotel();
        borg.setName("Hotel Borg");
        borg.setStars(4);
        borg.setTakesCreditCards(true);
        borg.setStartDate(new Date());
        borg.setType(Hotel.Type.LEISURE);
        borg.getTags().add("Swimming pool");
        borg.getTags().add("Room service");
        borg.setTemp("A temporary transient value");
        borg.getPhoneNumbers().add(new PhoneNumber(354, 5152233, PhoneNumber.Type.PHONE));
        borg.getPhoneNumbers().add(new PhoneNumber(354, 5152244, PhoneNumber.Type.FAX));

        final Address address = new Address();
        address.setStreet("Posthusstraeti 11");
        address.setPostCode("101");
        borg.setAddress(address);

        Document hotelDocument = mapper.toDocument(borg);
        List<Document> numbers = (List<Document>) hotelDocument.get("phoneNumbers");
        assertFalse(numbers.get(0).containsKey(
            mapper.getOptions().getDiscriminatorField()));


        hotels.insertOne(hotelDocument);

        Hotel borgLoaded = mapper.fromDocument(Hotel.class, hotelDocument);

        assertEquals(borg.getName(), borgLoaded.getName());
        assertEquals(borg.getStars(), borgLoaded.getStars());
        assertEquals(borg.getStartDate(), borgLoaded.getStartDate());
        assertEquals(borg.getType(), borgLoaded.getType());
        assertEquals(borg.getAddress().getStreet(), borgLoaded.getAddress().getStreet());
        assertEquals(borg.getTags().size(), borgLoaded.getTags().size());
        assertEquals(borg.getTags(), borgLoaded.getTags());
        assertEquals(borg.getPhoneNumbers().size(), borgLoaded.getPhoneNumbers().size());
        assertEquals(borg.getPhoneNumbers().get(1), borgLoaded.getPhoneNumbers().get(1));
        assertNull(borgLoaded.getTemp());
        assertTrue(borgLoaded.getPhoneNumbers() instanceof Vector);
        assertNotNull(borgLoaded.getId());

        final TravelAgency agency = new TravelAgency();
        agency.setName("Lastminute.com");
        agency.getHotels().add(borgLoaded);

        final Document agencyDocument = mapper.toDocument(agency);
        agencies.insertOne(agencyDocument);

        final TravelAgency agencyLoaded = mapper.fromDocument(TravelAgency.class,
            agencies.find(new Document("_id", agencyDocument.get("_id"))).first());

        assertEquals(agency.getName(), agencyLoaded.getName());
        assertEquals(1, agency.getHotels().size());
        assertEquals(agency.getHotels().get(0).getName(), borg.getName());

        // try clearing values
        borgLoaded.setAddress(null);
        borgLoaded.getPhoneNumbers().clear();
        borgLoaded.setName(null);

        hotelDocument = mapper.toDocument(borgLoaded);
        hotels.insertOne(hotelDocument);

        hotelDocument = (Document) hotels.find(new Document("_id", hotelDocument.get("_id")));

        borgLoaded = mapper.fromDocument(Hotel.class, hotelDocument);
        assertNull(borgLoaded.getAddress());
        assertEquals(0, borgLoaded.getPhoneNumbers().size());
        assertNull(borgLoaded.getName());
    }

    public enum Enum1 {
        A,
        B
    }

    private interface Foo {
    }

    @Entity
    public abstract static class BaseEntity {
        @Id
        private ObjectId id;

        public String getId() {
            return id.toString();
        }

        public void setId(final String id) {
            this.id = new ObjectId(id);
        }
    }

    @Entity
    public static class MissingId {
        private String id;
    }

    @Entity
    private static class MissingIdStill {
        private String id;
    }

    @Entity("no-id")
    private static class MissingIdRenamed {
        private String id;
    }

    @Embedded
    private static class IdOnEmbedded {
        @Id
        private ObjectId id;
    }

    @Embedded("no-id")
    private static class RenamedEmbedded {
        private String name;
    }

    @Entity
    private static class StrangelyNamedIdField {
        //CHECKSTYLE:OFF
        @Id
        private ObjectId id_ = new ObjectId();
        //CHECKSTYLE:ON
    }

    @Entity
    private static class ContainsEmbeddedArray {
        @Id
        private ObjectId id = new ObjectId();
        private RenamedEmbedded[] res;
    }

    private static class NotEmbeddable {
        private String noImNot = "no, I'm not";
    }

    private static class SerializableClass implements Serializable {
        private final String someString = "hi, from the ether.";
    }

    @Entity
    private static class ContainsRef {
        @Id
        private ObjectId id;
        private DBRef rect;
    }

    @Entity
    private static class HasFinalFieldId {
        @Id
        private final long id;
        private String name = "some string";

        //only called when loaded by the persistence framework.
        protected HasFinalFieldId() {
            id = -1;
        }

        HasFinalFieldId(final long id) {
            this.id = id;
        }
    }

    @Entity
    private static class ContainsFinalField {
        @Id
        private ObjectId id;
        private final String name;

        protected ContainsFinalField() {
            name = "foo";
        }

        ContainsFinalField(final String name) {
            this.name = name;
        }
    }

    @Entity
    private static class ContainsTimestamp {
        @Id
        private ObjectId id;
        private final Timestamp ts = new Timestamp(System.currentTimeMillis());
    }

    @Entity
    private static class ContainsDocument {
        @Id
        private ObjectId id;
        private Document document = new Document("field", "val");
    }

    @Entity
    private static class ContainsByteArray {
        @Id
        private ObjectId id;
        private final byte[] bytes = "Scott".getBytes();
    }

    @Entity
    private static class ContainsLongAndStringArray {
        @Id
        private ObjectId id;
        private Long[] longs = {0L, 1L, 2L};
        private String[] strings = {"Scott", "Rocks"};
    }

    @Entity
    private static final class ContainsCollection {
        @Id
        private ObjectId id;
        private final Collection<String> coll = new ArrayList<>();

        private ContainsCollection() {
            coll.add("hi");
            coll.add("Scott");
        }
    }

    @Entity
    private static class ContainsPrimitiveMap {
        @Id
        private ObjectId id;
        private final Map<String, Long> embeddedValues = new HashMap<>();
        private final Map<String, Long> values = new HashMap<>();
    }

    private static class Foo1 implements Foo {
        private String s;
    }

    private static class Foo2 implements Foo {
        private int i;
    }

    @Entity
    private static class ContainsMapWithEmbeddedInterface {
        @Id
        private ObjectId id;
        private final Map<String, Foo> embeddedValues = new HashMap<>();
    }

    @Entity
    private static class ContainsEmbeddedEntity {
        @Id
        private final ObjectId id = new ObjectId();
        private ContainsIntegerList cil = new ContainsIntegerList();
    }

    @Entity(value = "cil", useDiscriminator = false)
    private static class ContainsIntegerList {
        @Id
        private ObjectId id;
        private List<Integer> intList = new ArrayList<>();
    }

    @Entity(value = "cil", useDiscriminator = false)
    private static class ContainsIntegerListNew {
        @Id
        private ObjectId id;
        @AlsoLoad("intList")
        private final List<Integer> integers = new ArrayList<>();
    }

    @Entity(useDiscriminator = false)
    private static class ContainsUUID {
        @Id
        private ObjectId id;
        private final UUID uuid = UUID.randomUUID();
    }

    @Entity(useDiscriminator = false)
    private static class ContainsUuidId {
        @Id
        private final UUID id = UUID.randomUUID();
    }

    @Entity
    private static class ContainsEnum1KeyMap {
        @Id
        private ObjectId id;
        private final Map<Enum1, String> values = new HashMap<>();
        private final Map<Enum1, String> embeddedValues = new HashMap<>();
    }

    @Entity
    private static class ContainsIntKeyMap {
        @Id
        private ObjectId id;
        private final Map<Integer, String> values = new HashMap<>();
    }

    @Entity
    private static class ContainsIntKeySetStringMap {
        @Id
        private ObjectId id;
        private final Map<Integer, Set<String>> values = new HashMap<>();
    }

    @Entity
    private static class ContainsObjectIdKeyMap {
        @Id
        private ObjectId id;
        private final Map<ObjectId, String> values = new HashMap<>();
    }

    @Entity
    private static class ContainsXKeyMap<T> {
        @Id
        private ObjectId id;
        private final Map<T, String> values = new HashMap<>();
    }

    @Entity
    private static class ContainsMapLike {
        @Id
        private ObjectId id;
        private final MapLike m = new MapLike();
    }

    @Entity
    private static class UsesBaseEntity extends BaseEntity {

    }

    @Entity
    private static class MapSubclass extends LinkedHashMap<String, Object> {
        @Id
        private ObjectId id;
    }

    @Entity
    private class NonStaticInnerClass {
        @Id
        private long id = 1;
    }
}
