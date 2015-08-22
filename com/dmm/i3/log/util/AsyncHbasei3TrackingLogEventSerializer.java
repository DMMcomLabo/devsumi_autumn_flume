package com.dmm.i3.log.util;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.*;
import java.util.Map;
import java.util.HashMap;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.PutRequest;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.sink.hbase.SimpleHbaseEventSerializer.KeyType;
import org.apache.flume.sink.hbase.AsyncHbaseEventSerializer;
import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;

 public class AsyncHbasei3TrackingLogEventSerializer implements AsyncHbaseEventSerializer {
   private byte[] table;
   private byte[] colFam;
   private Event currentEvent;
   private byte[][] columnNames;
   private final List<PutRequest> puts = new ArrayList<PutRequest>();
   private final List<AtomicIncrementRequest> incs = new ArrayList<AtomicIncrementRequest>();
   private byte[] currentRowKey;
   private final byte[] eventCountCol = "eventCount".getBytes();
   @Override
   public void initialize(byte[] table, byte[] cf) {
     this.table = table;
     this.colFam = cf;
   }
   @Override
   public void setEvent(Event event) {
     // Set the event and verify that the rowKey is not present
     this.currentEvent = event;
     String rowKeyStr = currentEvent.getHeaders().get("rowKey");
   }
   public Map<String,String> logTokenize(String env){
     String[] envs = env.split(",");
     Map<String,String> res = new HashMap<String,String>();
     for(int i = 0; i < envs.length; i++){
        int index = envs[i].indexOf(":");
        String key = envs[i].substring(0,index);
        String value = envs[i].substring(index+1,envs[i].length());
	res.put(key,value);
     }
     return res;
   }
   @Override
   public List<PutRequest> getActions() {
     // Split the event body and get the values for the columns
     String eventStr = new String(currentEvent.getBody());
     Map<String,String> cols = logTokenize(eventStr);

     puts.clear();
     String[] columnFamilyName;
     byte[] bFam;
     String sCol;
     currentRowKey = (cols.get("i3_service_code") + "_" +Long.toString((Long.MAX_VALUE- System.currentTimeMillis()))).getBytes();
     for(int i = 0; i < columnNames.length;i++){
       columnFamilyName = new String(columnNames[i]).split(":");
       bFam = columnFamilyName[0].getBytes();
       sCol = columnFamilyName[1];
       if(cols.containsKey(sCol)){
         String sVal = cols.get(sCol).toString();
         PutRequest req = new PutRequest(table,currentRowKey,bFam,sCol.getBytes(),sVal.getBytes());
         puts.add(req);
       }
     }
     
     return puts;
   }
   @Override
   public List<AtomicIncrementRequest> getIncrements() {
	   incs.clear();
	   //Increment the number of events received
	   incs.add(new AtomicIncrementRequest(table, "totalEvents".getBytes(), colFam, eventCountCol));
	   return incs;
   }
   @Override
   public void cleanUp() {
	   table = null;
	   colFam = null;
	   currentEvent = null;
	   columnNames = null;
	   currentRowKey = null;
   }
   @Override
   public void configure(Context context) {
	   //Get the column names from the configuration
	   String cols = new String(context.getString("columns"));
	   String[] names = cols.split(",");
	   columnNames = new byte[names.length][];
	   int i = 0;
	   for(String name : names) {
		   columnNames[i++] = name.getBytes();
	   }
   }
   @Override
   public void configure(ComponentConfiguration conf) {
   } 
 }
