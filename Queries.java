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
        Query2();
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
          if(((String) detector.get("locationtext")).equals("Foster NB")){
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
                if(((String) loop.get("starttime")).startsWith("9/15/2011")){
                    total += (Integer) loop.get("volume");
                }
            }
            i++;
            key.put("num",i);
        }
        System.out.println("Query 2: Total volume at Foster NB on 9/15/2011: " + total);
    }
}
