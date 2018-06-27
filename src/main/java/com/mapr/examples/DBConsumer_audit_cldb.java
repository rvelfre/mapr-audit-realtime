package com.mapr.examples;

import com.mapr.db.MapRDB;
import com.mapr.db.Table;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;
import java.util.UUID;


public class DBConsumer_audit_cldb {

  public static void main(String[] args) {

    Table table = getTable("/apps/demo.mapr.com_cldb_maprdemo");


    KafkaConsumer<String, String> consumer;
    Properties properties = new Properties();
    properties.setProperty("group","demo.mapr.com_cldb_maprdemo");
    properties.setProperty("enable.auto.commit","true");
    properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
    properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
    properties.setProperty("auto.offset.reset", "latest");

    consumer = new KafkaConsumer<>(properties);

    consumer.subscribe(Collections.singletonList("/var/mapr/auditstream/auditlogstream:demo.mapr.com_cldb_maprdemo"));

    int i=0;

    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(200);
      for (ConsumerRecord<String, String> record : records) {
        System.out.println(record.topic() +":"+  record.value());
        table.insertOrReplace(UUID.randomUUID().toString(), MapRDB.newDocument( record.value() ) );
        i = i++;
        if (i==10) {
          table.flush();
          i=0;
        }
      }
    }

  }

  public static Table getTable(String tableName) {

    if (MapRDB.tableExists(tableName)) {
      return MapRDB.getTable(tableName);
    } else {
      return MapRDB.createTable(tableName);
    }


  }


}
