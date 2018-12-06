package com.company;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Vector;

public class Main {
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
        String FileName = "freeway_loopdata.csv";
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
    public static void main(String[] args) {
        /* Stations
            StationID,
            HighwayID,
            Milepost,
            LocationText,
            Upstream,
            Downstream,
            Stationclass,
            Numberlanes,
            Location,
            Length
         */
        //Vector<String[]> Stations = importStationData();
        /*
            DetectorID,
            HighwayID,
            Milepost,
            LocationText,
            DetectorClass,
            LaneNumber,
            StationID
         */
        //Vector<String[]> Detectors = importDetectorData();
        /*
            HighwayID,
            ShortDirection,
            Direction,
            HighwayName
         */
        //Vector<String[]> HighwayData = importHighwayData();
        /*
            DetectorID,
            StartTime,
            Volume,
            Speed,
            Occupancy,
            Status,
            DQFlags
         */
        //Vector<String[]> LoopData = importLoopData();

    }

    public static void Query1()
    {
        //this is pseudocode for finding the number of recorded speeds above 100mph.
        //we need only deal with loopdata for this as that's where the speeds are located

        //create iterator i
        int i = 0;
        String data = null;
        int total = 0;
        Vector<int> MyVec = new Vector<int>(100, 5);
        //loop through the store and get all the speeds greater than 100 saved to a vector
        while((data = store.get(i))!= null)
        {
            //get the speed out of the data, im assuming here syntacially
            if(data.speed > 100) {
                MyVec.add(data.speed);
                total++;
            }
            i++;
        }
        //total is the number of speeds over 100mph
    }

    public static void Query2(){
        //this is pseudocode for finding the total volume for station Foster NB on Sept 21 2011

        String data = null;
        int total = 0;
        int i = 0;
        Vector<String> FNBStations = null;
        //find the stations for fosterNB
        while((data = DetectorStore.get(i))!=null){
            if(data.locationtext == "Foster NB")
                FNBStations.add(data.detectorid);
            i++;
        }
        i = 0; //reset counter
        Vector<String> MyVec = new Vector<String>(100,5);
        //loop through the store and get all the matches
        while((data = LoopDataStore.get(i))!=null){
            //check the station
            if(FNBStations.contains(data.detectorid)){
                //check the date
                String date = data.date.split(" ");
                if(date == "Sept 21 2011"){
                    MyVec.add(data);
                    total++;
                }
            }
            i++;
        }
    }

    public static void query3() {
        //Find travel time for station Foster Nb for 5 minute intervals for Sept 22 2011, report in seconds
        int i = 0;
        int j = 0;
        int k = 0;
        String data = null; //used to get the length of "Foster NB"
        String check = null;
        String[] date = null;   //date used to hold split of string to get date and time separately.
        int length = 0; //find the length of Foster NB section.
        String date_check = "09/21/2011";

        Vector<int> Myvec = new Vector<int>(100, 5);
        while ((data = Stationstore.get(i)) != null) //loop through stations.csv to get Foster NB's length.
        {
            if (data.locationtext = "Foster NB") {
                length = data.length;   //set length as the length of "Foster NB"
            }
        }
        while ((data.detectorsstore.get(j)) != null) { //loop through detector.csv to get the detectors whose locationtext
            //is set to Foster NB and add them to the vector for later calculations.

            if (data.locationtext = "Foster NB") {
                Myvec.add(data.detectorid); //get all of the detectorid's whose location text is Foster NB and
                //add them to the Myvec Vector.
            }
        }
        //By now we should have a vector with all of the detector id's whose location is in "Foster NB".
        Vector<int> TravelTime = new Vector<int>(100, 5);    //Make a new vector for
        //time calculations.
        while ((check = loopdata.get(k)) != null) {

            date = check.starttime.split(" ");  //split the starttime variable on spaces to get data and time.
            boolean equal = date.equals(date_check);    //check for the date of "09/21/2011"
            if (equal == true) { //if its the right date continue.
                for (int i = 0; i < Myvec; ++i) {

                    if (check(k).detectorid == Myvec(i).detectorid) { //if it is a detector in "Foster NB".
                        if (check(k).speed != null) {

                        }
                    }
                }
            }
        }

    }

    public static void Query5(){
        //find the average travel time for 7-9am and 4-6pm on Sept 22 2011 for the I-205 NB Freeway. Report travel time in seconds
        int i = 0;
        String data = null;
        //query detectors and find all the ones where highwayid = 3, add their ids to an array
        Vector<String> NBArr = new Vector<String>(100,5);
        while((data = DetectorStore.get(i))!=null){
            if(data.highwayid == 3) {
                //add to arr
                NBArr.add(data.detectorid);
            }
            ++i;
        }
        //query loopdata, if the id matches check the time is between 7-9am and 4-6 (separately), increment counters, and add speed to total if there is a speed
        int j = 0;
        String otherData = null;
        int morningCount = 0;
        int afternoonCount = 0;
        int morningSpeedTotal = 0;
        int afternoonSpeedTotal = 0;
        Vector<String> LoopArr = new Vector<String>(100,5);
        while((otherData = LoopStore.get(i))!=null){
            //check the time and date
            String [] time = otherData.starttime.split(" ");
            if(time[0] == "Sept 22 2011") {
                String[] times = time.split(":");
                //the time is between 7-9am
                if((times[0] >= 7 && (times[0] <= 9 && times[2] >= 0 && times[2] <= 59))) {
                        if(otherData.volume > 0 && otherData.speed > 0){
                            morningCount++;
                            morningSpeedTotal+=otherData.speed;
                        }
                }
                if((times[0] >= 16 && (times[0] <= 18 && times[2] >= 0 && times[2] < 59))) {
                        if(otherData.volume > 0 && otherData.speed > 0){
                            afternoonCount++;
                            afternoonSpeedTotal += otherData.speed;
                        }
                }
            }
        }
        int afternoonAvg = 
    }



}

