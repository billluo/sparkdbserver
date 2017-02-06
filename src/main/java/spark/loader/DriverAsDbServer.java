package spark.loader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;

public class DriverAsDbServer {
	public static void main(String[] args) throws  IOException{
		//local port to listen
		int portNumber = 9090;
	    SparkConf sparkConf = new SparkConf()
	    							  .setAppName("JavaRecoverableNetworkWordCount")
	    							  .setMaster("local[3]");
	    SparkSession sse = SparkSession.builder().config(sparkConf).getOrCreate();
	    
		
	    String inputLine;
	    try(ServerSocket serverSocket = new ServerSocket(portNumber);
			Socket clientSocket = serverSocket.accept();
	    		ObjectOutputStream queryOutput = 
	    				new ObjectOutputStream(clientSocket.getOutputStream());
			BufferedReader in = new BufferedReader(
						new InputStreamReader(clientSocket.getInputStream()));
			){
			while ((inputLine = in.readLine()) != null) {
				System.out.println("***read from client : " + inputLine);
				//when client call database service as entering "dbService", driver returns 
				//result after running Spark transformations and actions
				if (inputLine.contains("dbService")) {
					List<Person> personsFiltered = personService(sse).collect();
					queryOutput.writeObject(personsFiltered);
					
				} else {
					System.out.println("don't provide any data feed");
					queryOutput.writeObject(null);
				}
				
			    if (inputLine .contains("stop")){
			    		System.out.println("stop server : " + inputLine);
		    			sse.stop();
			    		break;
			    }
			}
	    } catch (Exception e) {
	    		System.out.println("Exception caught when trying to listen on port "
	                + portNumber + " or listening for a connection");
	            System.out.println(e.getMessage());
		}
	}
	
	public static JavaRDD<Person> personService(SparkSession sse) throws IOException{
	    
        String fileName = "hdfs://localhost:9000/dbinput/person.csv";
        JavaRDD<Person> personRdd = sse.read()
        		  .textFile(fileName)
        		  .javaRDD()
        		  .map(new Function<String, Person>() {
        		    @Override
        		    public Person call(String line) throws Exception {
          		      String[] parts = line.split(",");
          		      Person person = new Person();
          		      person.setId(Long.parseLong(parts[0]));
          		      person.setFirstName(parts[1]);
          		      person.setLastName(parts[2]);
          		      person.setAddress(parts[3]);
          		      person.setPhone(parts[4]);
          		      return person;
        		    }
        		  });
        
        //run Spark transformations and actions
        JavaRDD<Person> personFiltered = personRdd.filter( x -> x.getFirstName().equalsIgnoreCase(("David")));
        
	    return personFiltered;
	}
}
