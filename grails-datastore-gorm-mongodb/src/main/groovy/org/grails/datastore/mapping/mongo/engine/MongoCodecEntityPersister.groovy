/*
 * Copyright 2015 original authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.grails.datastore.mapping.mongo.engine

import com.mongodb.MongoClient
import com.mongodb.client.MongoCollection
import com.mongodb.client.model.FindOneAndUpdateOptions
import com.mongodb.client.model.ReturnDocument
import grails.gorm.DetachedCriteria
import grails.util.GrailsNameUtils
import groovy.transform.CompileStatic
import org.bson.Document
import org.bson.codecs.Codec
import org.bson.codecs.configuration.CodecRegistries
import org.bson.codecs.configuration.CodecRegistry
import org.bson.types.ObjectId
import org.grails.datastore.mapping.cache.TPCacheAdapterRepository
import org.grails.datastore.mapping.core.IdentityGenerationException
import org.grails.datastore.mapping.core.impl.PendingDeleteAdapter
import org.grails.datastore.mapping.core.impl.PendingInsertAdapter
import org.grails.datastore.mapping.core.impl.PendingOperationAdapter
import org.grails.datastore.mapping.core.impl.PendingUpdateAdapter
import org.grails.datastore.mapping.engine.EntityAccess
import org.grails.datastore.mapping.engine.ThirdPartyCacheEntityPersister
import org.grails.datastore.mapping.model.ClassMapping
import org.grails.datastore.mapping.model.IdentityMapping
import org.grails.datastore.mapping.model.MappingContext
import org.grails.datastore.mapping.model.PersistentEntity
import org.grails.datastore.mapping.model.PersistentProperty
import org.grails.datastore.mapping.mongo.MongoCodecSession
import org.grails.datastore.mapping.mongo.MongoDatastore
import org.grails.datastore.mapping.mongo.engine.codecs.PersistentEntityCodec
import org.grails.datastore.mapping.mongo.query.MongoQuery
import org.grails.datastore.mapping.proxy.ProxyFactory
import org.grails.datastore.mapping.query.Query
import org.springframework.cglib.reflect.FastClass
import org.springframework.cglib.reflect.FastMethod
import org.springframework.context.ApplicationEventPublisher
import org.springframework.dao.CannotAcquireLockException


/**
 * An {@org.grails.datastore.mapping.engine.EntityPersister} that uses the MongoDB 3.0 {@link org.bson.codecs.configuration.CodecRegistry} infrastructure
 *
 * @author Graeme Rocher
 * @since 5.0.0
 */
@CompileStatic
class MongoCodecEntityPersister extends ThirdPartyCacheEntityPersister<Object> {

    public static final String INSTANCE_PREFIX = "instance:";
    public static final String MONGO_ID_FIELD = "_id";
    public static final String MONGO_CLASS_FIELD = "_class";
    protected static final String NEXT_ID = "next_id";
    protected static final String NEXT_ID_SUFFIX = ".$NEXT_ID";
    public static final String INC_OPERATOR = '$inc'



    protected final MongoCodecSession mongoSession
    protected final MongoDatastore mongoDatastore
    protected boolean hasNumericalIdentifier = false
    protected boolean hasStringIdentifier = false
    protected final FastClassData fastClassData

    MongoCodecEntityPersister(MappingContext mappingContext, PersistentEntity entity, MongoCodecSession session, ApplicationEventPublisher publisher, TPCacheAdapterRepository<Object> cacheAdapterRepository) {
        super(mappingContext, entity, session, publisher, cacheAdapterRepository)
        this.mongoSession = session
        this.mongoDatastore = session.datastore
        this.fastClassData = session.datastore.getFastClassData(entity)
        PersistentProperty identity = entity.identity
        if (identity != null) {
            hasNumericalIdentifier = Long.class.isAssignableFrom(identity.type)
            hasStringIdentifier = String.class.isAssignableFrom(identity.type)
        }
    }

    /**
     * Obtains an objects identifer
     * @param obj The object
     * @return The identifier or null if it doesn't have one
     */
    @Override
    Serializable getObjectIdentifier(Object obj) {
        if (obj == null) return null
        final ProxyFactory pf = proxyFactory
        if (pf.isProxy(obj)) {
            return pf.getIdentifier(obj)
        }
        return (Serializable)fastClassData.idReader.invoke(obj)
    }

    protected String getIdentifierName(ClassMapping cm) {
        final IdentityMapping identifier = cm.getIdentifier();
        if (identifier != null && identifier.getIdentifierName() != null) {
            return identifier.getIdentifierName()[0];
        }
        return null
    }

    @Override
    protected List<Object> retrieveAllEntities(PersistentEntity pe, Serializable[] keys) {
        retrieveAllEntities pe, Arrays.asList(keys)
    }

    @Override
    protected List<Object> retrieveAllEntities(PersistentEntity pe, Iterable<Serializable> keys) {
        createQuery()
            .in(pe.identity.name, keys.toList())
            .list()
    }

    @Override
    protected List<Serializable> persistEntities(PersistentEntity pe, @SuppressWarnings("rawtypes") Iterable objs) {
        objs.collect() {
            persistEntity(pe, it)
        }
    }

    @Override
    protected Object retrieveEntity(PersistentEntity pe, Serializable key) {
        Object o = getFromTPCache(pe, key)
        if(o != null) {
            return o
        }

        if( cancelLoad( pe, null ) ) {
            return null
        }
        else {
            MongoCollection mongoCollection = getMongoCollection(pe)
            Document idQuery = createIdQuery(key)
            o = mongoCollection
                    .withDocumentClass(persistentEntity.javaClass)
                    .withCodecRegistry(mongoDatastore.codecRegistry)
                    .find(idQuery, pe.javaClass)
                    .limit(1)
                    .first()

            if(o != null) {
                firePostLoadEvent( pe, createEntityAccess(pe, o) )
            }

            return o
        }
    }

    protected Document createIdQuery(Object key) {
        Map<String, Object> query = [:]
        query.put(AbstractMongoObectEntityPersister.MONGO_ID_FIELD, key)
        def idQuery = new Document(query)
        idQuery
    }

    @Override
    protected Serializable persistEntity(PersistentEntity entity, Object obj, boolean isInsert) {
        ProxyFactory proxyFactory = getProxyFactory()
        // if called internally, obj can potentially be a proxy, which won't work.
        obj = proxyFactory.unwrap(obj)

        Serializable id = getObjectIdentifier(obj)

        final boolean idIsNull = id == null
        final boolean isUpdate = !idIsNull && !isInsert

        if (isUpdate && !getSession().isDirty(obj)) {
            return (Serializable) id;
        }
        else {
            final EntityAccess entityAccess = createEntityAccess(entity,obj)
            boolean isAssigned = isAssignedId(entity)
            if(!isAssigned && idIsNull) {
                id = generateIdentifier(entity)
                entityAccess.setIdentifier(id)
            }
            else if(idIsNull) {
                // TODO: throw exception for null assigned id
            }

            if(!isUpdate) {
                MongoCodecEntityPersister self = this
                mongoSession.addPendingInsert(new PendingInsertAdapter(entity, id, obj, entityAccess) {
                    @Override
                    void run() {
                        if (!cancelInsert(entity, entityAccess)) {
                            updateCaches(entity, obj, id)
                            addCascadeOperation(new PendingOperationAdapter(entity, id, obj) {
                                @Override
                                void run() {
                                    self.firePostInsertEvent(entity, entityAccess)
                                }
                            })
                        }
                        else {
                            setVetoed(true)
                        }
                    }
                })
            }
            else {
                mongoSession.addPendingUpdate(new PendingUpdateAdapter( entity, id, obj, entityAccess) {
                    @Override
                    void run() {
                        if (!cancelUpdate(entity, entityAccess)) {
                            updateCaches(entity, obj, id)
                            addCascadeOperation(new PendingOperationAdapter(entity, id, obj) {
                                @Override
                                void run() {
                                    firePostUpdateEvent(entity, entityAccess)
                                }
                            })
                        }
                        else {
                            setVetoed(true)
                        }
                    }
                })
            }
        }
        return id
    }

    protected void updateCaches(PersistentEntity persistentEntity, Object e, Serializable id) {
        updateTPCache(persistentEntity, e, id)
    }

    protected Serializable generateIdentifier(final PersistentEntity persistentEntity) {
        // If there is a numeric identifier then we need to rely on optimistic concurrency controls to obtain a unique identifer
        // sequence. If the identifier is not numeric then we assume BSON ObjectIds.
        if (hasNumericalIdentifier) {
            final String collectionName = getCollectionName(persistentEntity)
            final MongoClient client = (MongoClient)mongoSession.nativeInterface

            final MongoCollection<Document>  dbCollection = client
                    .getDatabase(mongoSession.getDatabase(persistentEntity))
                    .getCollection("${collectionName}${NEXT_ID_SUFFIX}")

            int attempts = 0

            while (true) {

                final options = new FindOneAndUpdateOptions()
                options.upsert(true).returnDocument(ReturnDocument.AFTER)
                Document result = dbCollection.findOneAndUpdate(new Document(MONGO_ID_FIELD, collectionName), new Document(INC_OPERATOR, new Document(NEXT_ID, 1L)), options)
                // result should never be null and we shouldn't come back with an error ,but you never know. We should just retry if this happens...
                if (result != null) {
                    return result.getLong(NEXT_ID)
                } else {
                    attempts++;
                    if (attempts > 3) {
                        throw new IdentityGenerationException("Unable to generate identity for [$persistentEntity.name] using findAndModify after 3 attempts")
                    }
                }
            }
        }

        ObjectId objectId = ObjectId.get()
        def identityType = persistentEntity.identity.type
        if (ObjectId.class.isAssignableFrom(identityType)) {
            return objectId
        }

        return objectId.toString()
    }


    @Override
    protected void deleteEntity(PersistentEntity pe, Object obj) {

        ProxyFactory proxyFactory = getProxyFactory()
        // if called internally, obj can potentially be a proxy, which won't work.
        Serializable id
        if(proxyFactory.isProxy(obj)) {
            id = proxyFactory.getIdentifier(obj)
        }
        else {
            id = getObjectIdentifier(obj)
        }

        if(id != null) {
            MongoCodecEntityPersister self = this
            mongoSession.addPendingDelete( new PendingDeleteAdapter(pe, id, obj) {
                @Override
                void run() {
                    def entityAccess = self.createEntityAccess(pe, obj)
                    if( !self.cancelDelete( pe, entityAccess) ) {
                        mongoSession.clear(obj)
                        addCascadeOperation(new PendingOperationAdapter() {
                            @Override
                            void run() {
                                self.firePostDeleteEvent pe, entityAccess
                            }
                        })
                    }
                    else {
                        setVetoed(true)
                    }
                }
            })
        }
    }

    @Override
    protected void deleteEntities(PersistentEntity pe, @SuppressWarnings("rawtypes") Iterable objects) {
        def criteria = new DetachedCriteria(pe.javaClass)
        criteria.in( pe.identity.name, objects.collect() { getObjectIdentifier(it) }.findAll() { it != null } )
        mongoSession.deleteAll(
                criteria
        )
    }

    @Override
    Query createQuery() {
        return new MongoQuery(mongoSession, persistentEntity)
    }

    @Override
    Serializable refresh(Object o) {
        throw new UnsupportedOperationException("Refresh not supported by codec entity persistence engine")
    }

    @Override
    Object lock(Serializable id) throws CannotAcquireLockException {
        throw new UnsupportedOperationException("Pessimistic locks not supported by MongoDB")
    }

    @Override
    Object lock(Serializable id, int timeout) throws CannotAcquireLockException {
        throw new UnsupportedOperationException("Pessimistic locks not supported by MongoDB")
    }

    @Override
    boolean isLocked(Object o) {
        throw new UnsupportedOperationException("Pessimistic locks not supported by MongoDB")
    }

    @Override
    void unlock(Object o) {
        throw new UnsupportedOperationException("Pessimistic locks not supported by MongoDB")
    }

    @Override
    protected EntityAccess createEntityAccess(PersistentEntity pe, Object obj) {
        return mongoSession.datastore.createEntityAccess(pe, obj)
    }

    @Override
    protected Serializable persistEntity(PersistentEntity pe, Object obj) {
        return persistEntity(pe, obj, false)
    }

    protected MongoCollection getMongoCollection(PersistentEntity pe) {
        def database = mongoSession.getDatabase(pe)
        String collection = getCollectionName(pe)

        MongoClient client = (MongoClient)mongoSession.nativeInterface

        MongoCollection mongoCollection = client
                .getDatabase(database)
                .getCollection(collection)
                .withDocumentClass(pe.javaClass)
        return mongoCollection
    }


    protected String getCollectionName(PersistentEntity pe) {
        mongoSession.getCollectionName(pe)
    }
}