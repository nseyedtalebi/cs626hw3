import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class Main {
    public static Logger log  = LoggerFactory.getLogger("hw3_1");

    public static void main(String[] args) {
        Configuration conf = HBaseConfiguration.create();
        conf.addResource(new Path("/home/nima/bin/hbase-2.1.1/conf", "hbase-site.xml"));
        Connection conn;
        long maleCount = 0;
        long femaleCount = 0;
        long otherCount = 0;
        try {
            conn = ConnectionFactory.createConnection(conf);
            Table patients = conn.getTable(TableName.valueOf("patients"));
            loadPatients(conf);
            Scan patientScan = new Scan();
            patientScan.addColumn(Bytes.toBytes("demographics"),Bytes.toBytes("gender"));
            ResultScanner patientScanner = patients.getScanner(patientScan);
            for(Result patient: patientScanner){
                String gender = Bytes.toString(patient.getValue(Bytes.toBytes("demographics"),Bytes.toBytes("gender")));
                if(gender.equals("0")){
                    maleCount++;
                }
                else if(gender.equals("1")){
                    femaleCount++;
                }
                else{
                    otherCount++;
                }
            }
            log.info("Male patients:"+maleCount);
            log.info("Female patients:"+femaleCount);
            log.info("Other patients:"+otherCount);
        }
        catch(IOException ex){
            log.error(ExceptionUtils.getStackTrace(ex));
        }
    }

    static void loadPatients(Configuration conf) throws IOException {
        /*R1.1,R1.3*/
        Map<String,String> p1Gender = new HashMap<>();
        Map<String,String> p2Gender = new HashMap<>();
        Map<String,String> p1Asthma = new HashMap<>();
        Map<String,String> p2Asthma = new HashMap<>();
        /*Patient1.csv mappings*/
        //1-> male
        p1Gender.put("1","0");
        //2->female
        p1Gender.put("2","1");
        //0->no
        p1Asthma.put("0","0");
        //1->yes
        p1Asthma.put("1","1");
        //9->unknown
        p1Asthma.put("8","-9");

        /*Patient2.csv mappings*/
        //0-> female
        p2Gender.put("0","1");
        //1->male
        p2Gender.put("1","0");
        //1->Yes
        p2Asthma.put("1","1");
        //2->No
        p2Asthma.put("2","0");

        Connection conn;
        conn = ConnectionFactory.createConnection(conf);
        Table patients = conn.getTable(TableName.valueOf("patients"));
        putRows("patients1.csv",patients,p1Gender,p1Asthma);
        putRows("patients2.csv",patients,p2Gender,p2Asthma);
        patients.close();
        conn.close();
    }
    /*R1.1*/
    static void putRows(String resourceName, Table table,Map<String,String> genderMap,Map<String,String> asthmaMap) throws IOException{
        BufferedReader reader = new BufferedReader(new FileReader(Main.class.getResource(resourceName).getFile()));
        reader.readLine();//skip header line
        while (reader.ready()) {
            String rawline = reader.readLine();
            String[] line = rawline.split(",");
            Put row = prepareRow(line, genderMap, asthmaMap);
            table.put(row);
        }
        reader.close();
    }
    /*R1.1*/
    static Put prepareRow(String[] line, Map<String,String> genderMap, Map<String,String>asthmaMap){
        Put row = new Put(Bytes.toBytes(line[0]));
        /*R1.3*/
        /*Set NULL to unknown for all fields*/
        for(int i=0;i<line.length;i++){
            if(line[i].equals("NULL")){
                line[i] = "-9";
            }
        }
        row.addColumn(Bytes.toBytes("demographics"), Bytes.toBytes("gender"), Bytes.toBytes(genderMap.getOrDefault(line[1],"-9")));
        row.addColumn(Bytes.toBytes("demographics"),Bytes.toBytes("race"),Bytes.toBytes(line[2]));
        row.addColumn(Bytes.toBytes("anthropometry"),Bytes.toBytes("height"),Bytes.toBytes(line[3]));
        row.addColumn(Bytes.toBytes("anthropometry"),Bytes.toBytes("weight"),Bytes.toBytes(line[4]));
        row.addColumn(Bytes.toBytes("medical_history"),Bytes.toBytes("asthma"),Bytes.toBytes(asthmaMap.getOrDefault(line[5],"-9")));
        row.addColumn(Bytes.toBytes("medical_history"),Bytes.toBytes("hypertension"),Bytes.toBytes(line[6]));
        row.addColumn(Bytes.toBytes("other"),Bytes.toBytes("pid"),Bytes.toBytes(line[0]));
        row.addColumn(Bytes.toBytes("other"),Bytes.toBytes("year"),Bytes.toBytes(line[7]));
        return row;
    }
    /*R1.1*/
    public static void createPatientsTable(Configuration conf) throws IOException{
            Connection conn = ConnectionFactory.createConnection(conf);
            Admin admin = conn.getAdmin();
            List<ColumnFamilyDescriptor> columnFamilies =
                    Stream.of("demographics","anthropometry","medical_history","other")
                            .map(Bytes::toBytes)
                            .map(ColumnFamilyDescriptorBuilder::newBuilder)
                            .map(ColumnFamilyDescriptorBuilder::build)
                            .collect(Collectors.toList());
            TableDescriptor patientsDesc =
                    TableDescriptorBuilder.newBuilder(TableName.valueOf("patients"))
                            .setColumnFamilies(columnFamilies)
                            .build();
            admin.createTable(patientsDesc);
    }

}
