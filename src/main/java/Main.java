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
        //Add entry  for null
        p1Asthma.put("NULL","-9");
        /*Patient2.csv mappings*/
        //0-> female
        p2Gender.put("0","1");
        //1->male
        p2Gender.put("1","0");
        //1->Yes
        p2Asthma.put("1","1");
        //2->No
        p2Asthma.put("2","0");
        //Add entry  for null
        p2Asthma.put("NULL","-9");
        Connection conn = null;
        try {
            conn = ConnectionFactory.createConnection(conf);
            Table patients = conn.getTable(TableName.valueOf("patients"));
            BufferedReader p1 = new BufferedReader(new FileReader(Main.class.getResource("patients1.csv").getFile()));
            //pid,gender,race,height,weight,asthma,hypertension,year
            BufferedReader p2 = new BufferedReader(new FileReader(Main.class.getResource("patients2.csv").getFile()));
            //"pid","gender","race","height","weight","asthma","hypertension","year"
            p1.readLine();//skip header line
            while (p1.ready()) {
                String rawline = p1.readLine();
                log.debug(rawline);
                String[] line = rawline.split(",");
                Put row = prepareRow(line, p1Gender, p1Asthma);
                patients.put(row);
                //log.debug(Bytes.toString(row.getRow()));
            }
            conn.close();
        }
        catch(IOException ex){
            log.error(ExceptionUtils.getStackTrace(ex));
        }

    }

    static Put prepareRow(String[] line, Map<String,String> genderMap, Map<String,String>asthmaMap){
        Put row = new Put(Bytes.toBytes(line[0]));

        row.addColumn(Bytes.toBytes("demographics"), Bytes.toBytes("gender"), Bytes.toBytes(genderMap.getOrDefault(line[1],"-8")));
        row.addColumn(Bytes.toBytes("demographics"),Bytes.toBytes("race"),Bytes.toBytes(line[2]));
        row.addColumn(Bytes.toBytes("anthropometry"),Bytes.toBytes("height"),Bytes.toBytes(line[3]));
        row.addColumn(Bytes.toBytes("anthropometry"),Bytes.toBytes("weight"),Bytes.toBytes(line[4]));
        row.addColumn(Bytes.toBytes("medical_history"),Bytes.toBytes("asthma"),Bytes.toBytes(asthmaMap.getOrDefault(line[5],"-8")));
        row.addColumn(Bytes.toBytes("medical_history"),Bytes.toBytes("hypertension"),Bytes.toBytes(line[6]));
        row.addColumn(Bytes.toBytes("other"),Bytes.toBytes("pid"),Bytes.toBytes(line[0]));
        row.addColumn(Bytes.toBytes("other"),Bytes.toBytes("year"),Bytes.toBytes(line[7]));
        return row;
    }
    public static void createPatientsTable(Configuration conf) throws IOException{
        try{
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
        } catch (IOException ex){
            log.error(ExceptionUtils.getStackTrace(ex));
            throw(ex);
        }
    }

}
