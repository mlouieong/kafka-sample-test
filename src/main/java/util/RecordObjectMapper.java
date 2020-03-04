package util;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.PropertyAccessorFactory;


public class RecordObjectMapper {

    private static final Logger log = LoggerFactory.getLogger(RecordObjectMapper.class);

    public GenericRecord mapObjectToRecord(Schema schema, GenericRecord record, Object object) {
        schema.getFields().forEach(r -> {
            record.put(r.name(), PropertyAccessorFactory.forDirectFieldAccess(object).getPropertyValue(r.name()));
        });
        return record;
    }

    public <T> T mapRecordToObject(GenericRecord record, T object) {
        record.getSchema().getFields().forEach(d -> {
            PropertyAccessorFactory.forDirectFieldAccess(object).setPropertyValue(d.name(), record.get(d.name()) == null ? record.get(d.name()) : record.get(d.name()).toString());
        });
        return object;
    }
}
