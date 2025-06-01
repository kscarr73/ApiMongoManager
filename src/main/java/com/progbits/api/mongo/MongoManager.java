package com.progbits.api.mongo;

import java.util.List;

import org.bson.BsonDocument;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ServerApi;
import com.mongodb.ServerApiVersion;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.progbits.api.config.ConfigProvider;
import com.progbits.api.exception.ApiException;
import com.progbits.api.model.ApiObject;
import com.progbits.api.model.ApiObjectUtils;
import com.progbits.api.utils.service.ApiInstance;
import com.progbits.api.utils.service.ApiService;
import org.bson.BsonInt32;

/**
 *
 * @author scarr
 */
public class MongoManager implements ApiService, AutoCloseable {

    private static final ApiInstance<MongoManager> instance = new ApiInstance<>();

    public static MongoManager getInstance() {
        return instance.getInstance(MongoManager.class);
    }

    @Override
    public void configure() {
        config = ConfigProvider.getInstance();

        ServerApi serverApi = ServerApi.builder().version(ServerApiVersion.V1).build();

        MongoClientSettings settings = MongoClientSettings.builder()
            .applyConnectionString(new ConnectionString(config.getStringProperty("MONGO_URL")))
            .serverApi(serverApi)
            .build();

        mongoClient = MongoClients.create(settings);
        mongoDb = mongoClient.getDatabase(config.getStringProperty("MONGO_DB"));
    }
    
    @Override
    public void close() throws Exception {
        mongoClient.close();
    }

    ConfigProvider config;
    MongoClient mongoClient;
    MongoDatabase mongoDb;

    private static final String FIELD_ORDER_BY = "$orderBy";
    private static final String FIELD_FIELDS = "$fields";
    private static final String FIELD_LIMIT = "$limit";
    private static final String FIELD_START = "$start";
    private static final String FIELD_ID = "_id";
    private static final String FIELD_ROOT = "root";
    
    public MongoClient getClient() {
        return mongoClient;
    }
    
    public MongoDatabase getDatabase() {
        return mongoDb;
    }
    
    public MongoCollection<BsonDocument> getCollection(String collection) {
        return mongoDb.getCollection(collection, BsonDocument.class);
    }
    
    public ApiObject insert(String collection, ApiObject subject) throws ApiException {
        ApiObject resp = null;

        MongoCollection<BsonDocument> col = mongoDb.getCollection(collection, BsonDocument.class);
        
        if (subject.isSet(FIELD_ROOT)) {
            List<BsonDocument> bsonArray = ApiMongoConverter.toBsonArray(subject.getList(FIELD_ROOT));

            InsertManyResult res = col.insertMany(bsonArray);
            
            ApiObject objInsert = new ApiObject();

            objInsert.createObject(FIELD_ID);
            
            objInsert.getObject(FIELD_ID).createStringArray("$in");
            
            for (var entry : res.getInsertedIds().values()) {
                objInsert.getStringArray("_id.$in").add(entry.asObjectId().getValue().toHexString());
            }
            
            resp = find(collection, objInsert);
        } else {
            BsonDocument bsonDoc = ApiMongoConverter.toBson(subject);

            InsertOneResult res = col.insertOne(bsonDoc);

            ApiObject objInsert = new ApiObject();
            
            objInsert.setString(FIELD_ID, res.getInsertedId().asObjectId().getValue().toHexString());
            
            resp = find(collection, objInsert);
        }

        return resp;
    }
    
    public ApiObject find(String collection, ApiObject search) throws ApiException {
        ApiObject lclSearch = ApiObjectUtils.cloneApiObject(search, null);
        BsonDocument sortObj = null;
        BsonDocument fieldObj = null;
        Integer startRow = null;
        Integer limit = null;

        MongoCollection<BsonDocument> col = mongoDb.getCollection(collection, BsonDocument.class);

        if (lclSearch.containsKey(FIELD_START)) {
            startRow = lclSearch.getInteger(FIELD_START);
            lclSearch.remove(FIELD_START);
        }

        if (lclSearch.containsKey(FIELD_LIMIT)) {
            limit = lclSearch.getInteger(FIELD_LIMIT);
            lclSearch.remove(FIELD_LIMIT);
        }

        if (lclSearch.isSet(FIELD_FIELDS)) {
            fieldObj = new BsonDocument();

            for (var fldEntry : lclSearch.getStringArray(FIELD_FIELDS)) {
                if (fldEntry.startsWith("-")) {
                    fieldObj.append(fldEntry.substring(1), new BsonInt32(0));
                } else {
                    fieldObj.append(fldEntry, new BsonInt32(1));
                }
                
            }

            lclSearch.remove(FIELD_FIELDS);
        }

        if (lclSearch.isSet(FIELD_ORDER_BY)) {
            sortObj = new BsonDocument();

            for (var fldEntry : lclSearch.getStringArray(FIELD_ORDER_BY)) {
                if (fldEntry.startsWith("-")) {
                    sortObj.append(fldEntry.substring(1), new BsonInt32(-1));
                } else if (fldEntry.startsWith("+")) {
                    sortObj.append(fldEntry.substring(1), new BsonInt32(1));
                } else {
                    sortObj.append(fldEntry, new BsonInt32(1));
                }
            }

            lclSearch.remove(FIELD_ORDER_BY);
        }

        BsonDocument findObj = ApiMongoConverter.toBson(lclSearch);

        ApiObject objResp = new ApiObject();

        if (limit != null) {
            objResp.setLong("total", col.countDocuments(findObj));
        }

        FindIterable<BsonDocument> result = col.find(findObj);

        if (fieldObj != null) {
            result.projection(fieldObj);
        }

        if (sortObj != null) {
            result.sort(sortObj);
        }

        if (startRow != null) {
            result.skip(startRow);
        }

        if (limit != null) {
            result.limit(limit);
        }

        objResp.createList("root");
        
        MongoCursor<BsonDocument> iterator = result.iterator();
        
        while (iterator.hasNext()) {
            BsonDocument doc = iterator.next();
            
            objResp.getList("root").add(ApiMongoConverter.fromBson(doc));
        }
        
        return objResp;
    }
    
    public Long delete(String collection, ApiObject search, boolean deleteMultiple) throws ApiException {
        MongoCollection<BsonDocument> col = mongoDb.getCollection(collection, BsonDocument.class);
        
        BsonDocument searchObj = ApiMongoConverter.toBson(search);
        
        DeleteResult result;
        
        if (deleteMultiple) {
            result = col.deleteMany(searchObj);
        } else {
            result = col.deleteOne(searchObj);
        }
        
        return result.getDeletedCount();
    }
    
    public ApiObject update(String collection, ApiObject search, ApiObject subject, boolean updateMultiple) throws ApiException {
        MongoCollection<BsonDocument> col = mongoDb.getCollection(collection, BsonDocument.class);
        
        BsonDocument searchObj = ApiMongoConverter.toBson(search);
        BsonDocument subjectObj = ApiMongoConverter.toBson(subject);
        
        if (updateMultiple) {
            col.updateMany(searchObj, subjectObj);
        } else {
            col.updateOne(searchObj, subjectObj);
        }
        
        return find(collection, search);
    }
}
