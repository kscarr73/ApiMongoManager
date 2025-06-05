package com.progbits.api.mongo;

import com.progbits.api.exception.ApiException;
import com.progbits.api.model.ApiObject;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.types.ObjectId;

/**
 *
 * @author scarr
 */
public class ApiMongoConverter {

    public static BsonDocument toBson(ApiObject subject) throws ApiException {
        return toBson(null, subject);
    }
    
    public static BsonDocument toBson(String parentKey, ApiObject subject) throws ApiException {
        BsonDocument bson = new BsonDocument();

        for (var entry : subject.entrySet()) {
            switch (entry.getValue()) {
                case String val -> {
                    if ("_id".equals(entry.getKey())) {
                        bson.append(entry.getKey(), new BsonObjectId(new ObjectId(val)));
                    } else {
                        bson.append(entry.getKey(), new BsonString(val));
                    }
                }
                
                case Integer val ->
                    bson.append(entry.getKey(), new BsonInt32(val));
                case Double val ->
                    bson.append(entry.getKey(), new BsonDouble(val));
                case Long val ->
                    bson.append(entry.getKey(), new BsonInt64(val));
                case Boolean val ->
                    bson.append(entry.getKey(), new BsonBoolean(val));
                case OffsetDateTime val -> bson.append(entry.getKey(), new BsonDateTime(val.toEpochSecond()));
                case BigDecimal val -> bson.append(entry.getKey(), new BsonDouble(val.doubleValue()));
                case List val -> {
                    processApiList(bson, subject, entry.getKey(), parentKey);
                }
                
                case ApiObject val -> bson.append(entry.getKey(),toBson(entry.getKey(), subject.getObject(entry.getKey())));

                default -> {
                }
            }
        }
        
        return bson;
    }

    public static List<BsonDocument> toBsonArray(List<ApiObject> subject) throws ApiException {
        List<BsonDocument> resp = new ArrayList<>();

        for (var entry : subject) {
            resp.add(toBson(entry));
        }

        return resp;
    }

    public static ApiObject fromBson(BsonDocument subject) throws ApiException {
        ApiObject ret = new ApiObject();

        for (var entry : subject.entrySet()) {
            switch (entry.getValue().getBsonType()) {
                case STRING ->
                    ret.put(entry.getKey(), entry.getValue().asString().getValue());
                case INT32 ->
                    ret.put(entry.getKey(), entry.getValue().asInt32().getValue());
                case DOUBLE ->
                    ret.put(entry.getKey(), entry.getValue().asDouble());
                case INT64  ->
                    ret.put(entry.getKey(), entry.getValue().asInt64().getValue());
                case BOOLEAN ->
                    ret.put(entry.getKey(), entry.getValue().asBoolean().getValue());
                case DATE_TIME -> {
                    Long lValue = entry.getValue().asDateTime().getValue();

                    OffsetDateTime nDt = OffsetDateTime.ofInstant(Instant.ofEpochSecond(lValue), ZoneId.of("UTC"));

                    ret.setDateTime(entry.getKey(), nDt);
                }
                    
                case ARRAY -> {
                    processBsonArray(entry.getValue().asArray(), ret, entry.getKey());
                }
                
                case DOCUMENT -> 
                   ret.setObject(entry.getKey(),fromBson(entry.getValue().asDocument()));

                case OBJECT_ID ->
                    ret.setString(entry.getKey(),entry.getValue().asObjectId().getValue().toHexString());
                
                default -> {
                }
            }
        }
        return ret;
    }

    private static void processApiList(BsonDocument bson, ApiObject subject, String key, String parentKey) throws ApiException {
        BsonArray arr = new BsonArray();
        
        switch (subject.getType(key)) {
            case ApiObject.TYPE_ARRAYLIST -> {
                for (var entry : subject.getList(key)) {
                    arr.add(toBson(entry));
                }
            }

            case ApiObject.TYPE_STRINGARRAY -> {
                for (var entry : subject.getStringArray(key)) {
                    if ("_id".equals(key) || "_id".equals(parentKey)) {
                        arr.add(new BsonObjectId(new ObjectId(entry)));
                    } else {
                        arr.add(new BsonString(entry));
                    }
                }
            }

            case ApiObject.TYPE_DOUBLEARRAY -> {
                for (var entry : subject.getDoubleArray(key)) {
                    arr.add(new BsonDouble(entry));
                }
            }

            case ApiObject.TYPE_INTEGERARRAY -> {
                for (var entry : subject.getIntegerArray(key)) {
                    arr.add(new BsonInt32(entry));
                }
            }
        }
        
        bson.append(key, arr);
    }

    private static void processBsonArray(BsonArray subject, ApiObject resp, String key) throws ApiException {
        if (!subject.isEmpty()) {
            BsonValue fval = subject.getFirst();
            
            if (fval.isDocument()) {
                List<ApiObject> arrList = new ArrayList<>();

                for (var entry : subject.getValues()) {
                    arrList.add(fromBson(entry.asDocument()));
                }

                resp.put(key, arrList);
            } else if (fval.isString()) {
                List<String> arrList = new ArrayList<>();

                for (var entry : subject.getValues()) {
                    arrList.add(entry.asString().getValue());
                }

                resp.put(key, arrList);
            } else if (fval.isInt32()) {
                List<Integer> arrList = new ArrayList<>();

                for (var entry : subject.getValues()) {
                    arrList.add(entry.asInt32().getValue());
                }

                resp.put(key, arrList);
            } else if (fval.isDouble()) {
                List<Double> arrList = new ArrayList<>();

                for (var entry : subject.getValues()) {
                    arrList.add(entry.asDouble().getValue());
                }

                resp.put(key, arrList);
            }
        }        
    }
}
