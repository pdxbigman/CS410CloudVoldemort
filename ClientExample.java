/*
 * Copyright 2008-2009 LinkedIn, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package voldemort.examples;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.parsing.Parser;
import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;
import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.json.JsonReader;
import voldemort.utils.ByteArray;
import voldemort.versioning.Versioned;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Vector;
import java.util.Iterator;
import java.io.StringReader;

public class ClientExample {

    public static void main(String[] args) {
        primeHighways();
        primeDetectors();
        primeStations();
        primeLoopdata();
    }

    public static Vector<String[]> importStationData(){
        String FileName = "freeway_stations.csv";
        String fl = null;
        BufferedReader br = null;
        String line = null;
        String cvsSplitBy = ",";

        Vector<String[]> array = new Vector<>(10,2);
        try {
            br = new BufferedReader(new FileReader(FileName));
        }catch(Exception e){}

        try {
            //pull off first line
            String headers = fl = br.readLine();
            try{
                //get the data
                while((line = br.readLine()) != null){
                    String [] result = line.split(cvsSplitBy);
                    array.add(result);
                    System.out.println("Stationid: " + result[0] + " highwayid: " + result[1] +
                            " Milepost: " + result[2] + " LocationText: " + result[3] + " Upstream: " + result[4] + " Downstream: " + result[5] + " StationClass: " + result[6] + " NumberLanes: " + result[7] + " Location: " + result[8] + " Length: " + result[9] + "\n");
                }
            }catch(Exception e){}

        }catch(Exception e){}
        return array;
    }

    public static Vector<String[]> importDetectorData(){
        String FileName = "freeway_detectors.csv";
        String fl = null;
        BufferedReader br = null;
        String line = null;
        String cvsSplitBy = ",";

        Vector<String[]> array = new Vector<>(10,2);
        try {
            br = new BufferedReader(new FileReader(FileName));
        }catch(Exception e){}

        try {
            //pull off first line
            String headers = fl = br.readLine();
            try{
                //get the data
                while((line = br.readLine()) != null){
                    String [] result = line.split(cvsSplitBy);
                    array.add(result);
                    System.out.println("DectectorID: " + result[0] + " Highwayid: " + result[1] +
                            " Milepost: " + result[2] + " LocationText: " + result[3] + " DetectorClass: " + result[4] + " LaneNumber: "
                            + result[5] + " StationID: " + result[6] + "\n");
                }
            }catch(Exception e){}

        }catch(Exception e){}
        return array;
    }

    public static Vector<String[]> importHighwayData(){
        String FileName = "highways.csv";
        String fl = null;
        BufferedReader br = null;
        String line = null;
        String cvsSplitBy = ",";

        Vector<String[]> array = new Vector<>(10,2);
        try {
            br = new BufferedReader(new FileReader(FileName));
        }catch(Exception e){}

        try {
            //pull off first line
            String headers = fl = br.readLine();
            try{
                //get the data
                while((line = br.readLine()) != null){
                    String [] result = line.split(cvsSplitBy);
                    array.add(result);
                    System.out.println("HighwayID: " + result[0] + " ShortDirection: " + result[1] +
                            " Direction: " + result[2] + " HighwayName: " + result[3] + "\n");
                }
            }catch(Exception e){}

        }catch(Exception e){}
        return array;
    }

    public static Vector<String[]> importLoopData(){
        String FileName = "freeway_loopdata_OneHour.csv";
        String fl = null;
        BufferedReader br = null;
        String line = null;
        String cvsSplitBy = ",";

        Vector<String[]> array = new Vector<>(10,2);
        try {
            br = new BufferedReader(new FileReader(FileName));
        }catch(Exception e){}

        try {
            //pull off first line
            String headers = fl = br.readLine();
            try{
                //get the data
                while((line = br.readLine()) != null){
                    String [] result = line.split(cvsSplitBy);
                    array.add(result);
                    System.out.println("DetectorID: " + result[0] + " StartTime: " + result[1] +
                            " Volume: " + result[2] + " Speed: " + result[3] + " Occupancy: " + result[4]+
                            " Status: " + result[5] + " Dqflags: " + result[6] + "\n");
                }
            }catch(Exception e){}

        }catch(Exception e){}
        return array;
    }

    public static void primeHighways() {

        // In production environment, the StoreClient instantiation should be done using factory pattern
        // through a Framework such as Spring
        String bootstrapUrl = "tcp://localhost:6666";
        StoreClientFactory factory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(bootstrapUrl));
        int num = 0;
        StoreClient<GenericRecord, GenericRecord> client = factory.getStoreClient("highways");
        Vector<String[]> data = new Vector<>(10,2);
        data = importHighwayData();
        Iterator<String[]> iter = data.iterator();
        String keySchemaJson = "{ \"name\": \"key\", \"type\": \"record\", \"fields\": [{ \"name\": \"num\", \"type\": \"int\" }] }";
        Schema keySchema = Schema.parse(keySchemaJson);
        GenericRecord key = new GenericData.Record(keySchema);
        String valueSchemaJson = "{\n" +
                "      \"name\": \"value\",\n" +
                "       \"type\": \"record\",\n" +
                "      \"fields\": [{ \n" +
                "        \"name\": \"highwayid\",\n" +
                "        \"type\": \"int\"\n" +
                "      }, {\n" +
                "        \"name\": \"shortdirection\",\n" +
                "        \"type\": \"string\"\n" +
                "      }, {\n" +
                "        \"name\": \"direction\",\n" +
                "        \"type\": \"string\"\n" +
                "      }, {\n" +
                "        \"name\": \"highwayname\",\n" +
                "        \"type\": \"string\"\n" +
                "      }]\n" +
                "    }";
        Schema valueSchema = Schema.parse(valueSchemaJson);
        GenericRecord value = new GenericData.Record(valueSchema);

        while(iter.hasNext()){
          String[] s = iter.next();
          // creating initial k-v pair
          key.put("num", num++);
          value.put("highwayid", Integer.parseInt(s[0]));
          value.put("shortdirection", s[1]);
          value.put("direction", s[2]);
          value.put("highwayname", s[3]);

          // put initial value
          client.put(key, value);
        }
    }

    public static void primeDetectors() {

        // In production environment, the StoreClient instantiation should be done using factory pattern
        // through a Framework such as Spring
        String bootstrapUrl = "tcp://localhost:6666";
        StoreClientFactory factory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(bootstrapUrl));
        int num = 0;
        StoreClient<GenericRecord, GenericRecord> client = factory.getStoreClient("detectors");
        Vector<String[]> data = new Vector<>(10,2);
        data = importDetectorData();
        Iterator<String[]> iter = data.iterator();
        // creating initial k-v pair
        String keySchemaJson = "{ \"name\": \"key\", \"type\": \"record\", \"fields\": [{ \"name\": \"num\", \"type\": \"int\" }] }";
        Schema keySchema = Schema.parse(keySchemaJson);
        GenericRecord key = new GenericData.Record(keySchema);
        String valueSchemaJson = "{\n" +
                "      \"name\": \"value\",\n" +
                "      \"type\": \"record\",\n" +
                "      \"fields\": [{ \n" +
                "        \"name\": \"detectorid\",\n" +
                "        \"type\": \"int\"\n" +
                "      },{ \n" +
                "        \"name\": \"highwayid\",\n" +
                "        \"type\": \"int\"\n" +
                "      }, {\n" +
                "        \"name\": \"milepost\",\n" +
                "        \"type\": \"string\"\n" +
                "      }, {\n" +
                "        \"name\": \"locationtext\",\n" +
                "        \"type\": \"string\"\n" +
                "      }, {\n" +
                "        \"name\": \"detectorclass\",\n" +
                "        \"type\": \"int\"\n" +
                "      }, {\n" +
                "        \"name\": \"lanenumber\",\n" +
                "        \"type\": \"int\"\n" +
                "      }, {\n" +
                "        \"name\": \"stationid\",\n" +
                "        \"type\": \"int\"\n" +
                "      }]\n" +
                "    }";
        Schema valueSchema = Schema.parse(valueSchemaJson);
        GenericRecord value = new GenericData.Record(valueSchema);

        while(iter.hasNext()){
          String[] s = iter.next();

          key.put("num", num++);

          value.put("detectorid", Integer.parseInt(s[0]));
          value.put("highwayid", Integer.parseInt(s[1]));
          value.put("milepost", s[2]);
          value.put("locationtext", s[3]);
          value.put("detectorclass", Integer.parseInt(s[4]));
          value.put("lanenumber", Integer.parseInt(s[5]));
          value.put("stationid", Integer.parseInt(s[6]));

          // put initial value
          client.put(key, value);
        }
    }

    public static void primeStations() {

        // In production environment, the StoreClient instantiation should be done using factory pattern
        // through a Framework such as Spring
        String bootstrapUrl = "tcp://localhost:6666";
        StoreClientFactory factory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(bootstrapUrl));
        int num = 0;
        StoreClient<GenericRecord, GenericRecord> client = factory.getStoreClient("stations");
        Vector<String[]> data = new Vector<>(10,2);
        data = importStationData();
        Iterator<String[]> iter = data.iterator();

        // creating initial k-v pair
        System.out.println("Creating initial Key and Value");
        String keySchemaJson = "{ \"name\": \"key\", \"type\": \"record\", \"fields\": [{ \"name\": \"num\", \"type\": \"int\" }] }";
        Schema keySchema = Schema.parse(keySchemaJson);
        GenericRecord key = new GenericData.Record(keySchema);
        String valueSchemaJson = "{\n" +
                "      \"name\": \"value\",\n" +
                "      \"type\": \"record\",\n" +
                "      \"fields\": [{ \n" +
                "        \"name\": \"stationid\",\n" +
                "        \"type\": \"int\"\n" +
                "      },{ \n" +
                "        \"name\": \"highwayid\",\n" +
                "        \"type\": \"int\"\n" +
                "      }, {\n" +
                "        \"name\": \"milepost\",\n" +
                "        \"type\": \"string\"\n" +
                "      }, {\n" +
                "        \"name\": \"locationtext\",\n" +
                "        \"type\": \"string\"\n" +
                "      }, {\n" +
                "        \"name\": \"upstream\",\n" +
                "        \"type\": \"int\"\n" +
                "      }, {\n" +
                "        \"name\": \"downstream\",\n" +
                "        \"type\": \"int\"\n" +
                "      }, {\n" +
                "        \"name\": \"stationclass\",\n" +
                "        \"type\": \"int\"\n" +
                "      }, {\n" +
                "        \"name\": \"numberlanes\",\n" +
                "        \"type\": \"int\"\n" +
                "      }, {\n" +
                "        \"name\": \"latlon\",\n" +
                "        \"type\": \"string\"\n" +
                "      }, {\n" +
                "        \"name\": \"length\",\n" +
                "        \"type\": \"string\"\n" +
                "      }]\n" +
                "    }";
        Schema valueSchema = Schema.parse(valueSchemaJson);
        GenericRecord value = new GenericData.Record(valueSchema);

        while(iter.hasNext()){
          String[] s = iter.next();

          key.put("num", num++);

          value.put("stationid", Integer.parseInt(s[0]));
          value.put("highwayid", Integer.parseInt(s[1]));
          value.put("milepost", s[2]);
          value.put("locationtext", s[3]);
          value.put("upstream", Integer.parseInt(s[4]));
          value.put("downstream", Integer.parseInt(s[5]));
          value.put("stationclass", Integer.parseInt(s[6]));
          value.put("numberlanes", Integer.parseInt(s[7]));
          value.put("latlon", s[8]);
          value.put("length", s[9]);

          // put initial value
          client.put(key, value);
        }
    }

    public static void primeLoopdata() {

        // In production environment, the StoreClient instantiation should be done using factory pattern
        // through a Framework such as Spring
        String bootstrapUrl = "tcp://localhost:6666";
        StoreClientFactory factory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(bootstrapUrl));

        StoreClient<GenericRecord, GenericRecord> client = factory.getStoreClient("loopdata");
        Vector<String[]> data = new Vector<>(10,2);
        data = importLoopData();
        Iterator<String[]> iter = data.iterator();
        int num = 0;
        // creating initial k-v pair
        System.out.println("Creating initial Key and Value");
        String keySchemaJson = "{ \"name\": \"key\", \"type\": \"record\", \"fields\": [{ \"name\": \"num\", \"type\": \"int\" }] }";
        Schema keySchema = Schema.parse(keySchemaJson);
        GenericRecord key = new GenericData.Record(keySchema);
        String valueSchemaJson = "{\n" +
                "      \"name\": \"value\",\n" +
                "      \"type\": \"record\",\n" +
                "      \"fields\": [{ \n" +
                "        \"name\": \"detectorid\",\n" +
                "        \"type\": \"int\"\n" +
                "      },{ \n" +
                "        \"name\": \"starttime\",\n" +
                "        \"type\": \"string\"\n" +
                "      }, {\n" +
                "        \"name\": \"volume\",\n" +
                "        \"type\": \"int\"\n" +
                "      }, {\n" +
                "        \"name\": \"speed\",\n" +
                "        \"type\": \"int\"\n" +
                "      }, {\n" +
                "        \"name\": \"occupancy\",\n" +
                "        \"type\": \"int\"\n" +
                "      }, {\n" +
                "        \"name\": \"status\",\n" +
                "        \"type\": \"int\"\n" +
                "      }, {\n" +
                "        \"name\": \"dqflags\",\n" +
                "        \"type\": \"int\"\n" +
                "      }]\n" +
                "    }";
        Schema valueSchema = Schema.parse(valueSchemaJson);
        GenericRecord value = new GenericData.Record(valueSchema);

        while(iter.hasNext()){
          String[] s = iter.next();

          key.put("num", num++);

          value.put("detectorid", Integer.parseInt(s[0]));
          value.put("starttime", s[1]);
          if(!s[2].isEmpty()){
            value.put("volume", Integer.parseInt(s[2]));
          }
          if(!s[3].isEmpty()){
            value.put("speed", Integer.parseInt(s[3]));
          }
          if(!s[4].isEmpty()){
            value.put("occupancy", Integer.parseInt(s[4]));
          }
          value.put("status", Integer.parseInt(s[5]));
          value.put("dqflags", Integer.parseInt(s[6]));

          // put initial value
          client.put(key, value);
        }
    }

}
