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

public class Queries {

    public static void main(String[] args) {
        Query4();
    }

    public static void Query1()
        {
            //this is pseudocode for finding the number of recorded speeds above 100mph.
            //we need only deal with loopdata for this as that's where the speeds are located
            String bootstrapUrl = "tcp://localhost:6666";
            StoreClientFactory factory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(bootstrapUrl));

            StoreClient<GenericRecord, GenericRecord> client = factory.getStoreClient("loopdata");
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

            //create iterator i
            int i = 0;
            int total = 0;
            key.put("num",i);
            Versioned<GenericRecord> versioned;
            //loop through the store and get all the speeds greater than 100 saved to a vector
            while((versioned = client.get(key))!= null)
            {
                value = versioned.getValue();
                //get the speed out of the data, im assuming here syntacially
                if((Integer) value.get("speed") > 100) {
                  total++;
                }
                i++;
                key.put("num",i);
            }
            System.out.println("Query 1: Number of speeds over 100mph: " + total);
            //total is the number of speeds over 100mph
        }

        public static void Query2(){
          String bootstrapUrl = "tcp://localhost:6666";
          StoreClientFactory factory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(bootstrapUrl));

          String keySchemaJson = "{ \"name\": \"key\", \"type\": \"record\", \"fields\": [{ \"name\": \"num\", \"type\": \"int\" }] }";
          Schema keySchema = Schema.parse(keySchemaJson);
          GenericRecord key = new GenericData.Record(keySchema);

          StoreClient<GenericRecord, GenericRecord> detectors = factory.getStoreClient("detectors");
          String detectorsSchemaJson = "{\n" +
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
          Schema detectorsSchema = Schema.parse(detectorsSchemaJson);
          GenericRecord detector = new GenericData.Record(detectorsSchema);

          StoreClient<GenericRecord, GenericRecord> loopdata = factory.getStoreClient("loopdata");
          String loopdataSchemaJson = "{\n" +
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
          Schema loopdataSchema = Schema.parse(loopdataSchemaJson);
          GenericRecord loop = new GenericData.Record(loopdataSchema);

        //this is pseudocode for finding the total volume for station Foster NB on Sept 21 2011

        int total = 0;
        int i = 0;
        Vector<Integer> MyVec = new Vector<Integer>(100,5);
        key.put("num",i);
        Versioned<GenericRecord> versioned;
        while((versioned = detectors.get(key)) != null){
          detector = versioned.getValue();
          if(detector.get("locationtext").toString().equals("Foster NB")){
            MyVec.add((Integer) detector.get("detectorid"));
          }
          i++;
          key.put("num",i);
        }

        i = 0;
        key.put("num",i);
        //loop through the store and get all the matches
        while((versioned = loopdata.get(key))!=null){
            loop = versioned.getValue();
            if(MyVec.contains((Integer) loop.get("detectorid"))){
                if(loop.get("starttime").toString().startsWith("9/15/2011")){
                    total += (Integer) loop.get("volume");
                }
            }
            i++;
            key.put("num",i);
        }
        System.out.println("Query 2: Total volume at Foster NB on 9/15/2011: " + total);
    }

    public static void Query4(){
    //Find the average travel time for 7-9AM and 4-6PM on Foster NB. Report travel
    //time in seconds.
    String bootstrapUrl = "tcp://localhost:6666";
    StoreClientFactory factory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(bootstrapUrl));

    String keySchemaJson = "{ \"name\": \"key\", \"type\": \"record\", \"fields\": [{ \"name\": \"num\", \"type\": \"int\" }] }";
    Schema keySchema = Schema.parse(keySchemaJson);
    GenericRecord key = new GenericData.Record(keySchema);

    StoreClient<GenericRecord, GenericRecord> detectors = factory.getStoreClient("detectors");
    String detectorsSchemaJson = "{\n" +
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
    Schema detectorsSchema = Schema.parse(detectorsSchemaJson);
    GenericRecord detector = new GenericData.Record(detectorsSchema);

    StoreClient<GenericRecord, GenericRecord> loopdata = factory.getStoreClient("loopdata");
    String loopdataSchemaJson = "{\n" +
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
    Schema loopdataSchema = Schema.parse(loopdataSchemaJson);
    GenericRecord loop = new GenericData.Record(loopdataSchema);

    int morning = 0; //total avg of speeds from 7-9AM.
    int evening = 0; //total avg of speeds from 4-6PM.
    String data = null; //used to traverse through detector.csv
    String check = null; //used to traverse through loopdata.csv
    String[] date = null;   //date used for 7-9AM check.
    String date_check = "09/22/2011";   //date check used for both times.

    Vector<Integer> MyVec = new Vector<Integer>(100,5);//vector to contain all detectors going
    //to "Foster NB"
    int i = 0;
    key.put("num",i);
    Versioned<GenericRecord> versioned;
    while((versioned = detectors.get(key)) != null){
      detector = versioned.getValue();
      if(detector.get("locationtext").toString().equals("Foster NB")){
        MyVec.add((Integer) detector.get("detectorid"));
      }
      i++;
      key.put("num",i);
    }

    i = 0;
    key.put("num",i);
    while((versioned = loopdata.get(key)) != null) {   //loop through loopdata to get the speed of detectorid's with
        //the location from the vector created above.
        loop = versioned.getValue();
        date = loop.get("starttime").toString().split(" ");  //split the starttime variable on spaces to get data and time.
        if(date[0].equals(date_check)) {  //if we on the correct date.
            String[] hour = date[1].split(":");
            int hr = Integer.parseInt(hour[0]);
            int min = Integer.parseInt(hour[1]);
            int sec = Integer.parseInt(hour[2]);
            if ((hr >= 7 && hr < 9) || (hr == 9 && min == 0 && sec == 0)){ //check between 7Am and 8:59:59 AM
                if(MyVec.contains((Integer)loop.get("detectorid"))){
                  morning += (Integer)loop.get("speed");  //get the speed of the detectorid;
                }
            }
            if ((hr >= 16 && hr < 18) || (hr == 18 && min == 0 && sec == 0)){ //check between 4PM and
                    // 9:59:59 PM
                if(MyVec.contains((Integer)loop.get("detectorid"))){
                  evening += (Integer)loop.get("speed");  //get the speed of the detectorid;
                }
            }
        }
        i++;
        key.put("num",i);
    }
    int morningavg = morning/7200; //divide total speed over the two hours/seconds of 2hr to get avg in seconds.
    int eveningavg = evening/7200; //divide total speed over the two hours/seconds of 2hr to get avg in seconds.

    System.out.println("Query 4");
    System.out.println("Morning Average Time on 09/22/2011 during 7Am to 9Am is ");
    System.out.println(morningavg);
    System.out.println('\n');

    System.out.println("Evening Average Time on 09/22/2011 during 4PM to 6PM is ");
    System.out.println(eveningavg);
    System.out.println('\n');
  }
}
