package spark.loader;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.List;

public class ExecutorAsDbClient {

	public static void main(String[] args) throws UnknownHostException, IOException, ClassNotFoundException{
		
		String hostName = "localhost";
		int portNumber=9090;
		
		try(
				Socket echoSocket = new Socket(hostName, portNumber);
				PrintWriter out = new PrintWriter(echoSocket.getOutputStream(),true);
				ObjectInputStream queryInput = new ObjectInputStream(new BufferedInputStream(echoSocket.getInputStream()));
				BufferedReader stdIn = new BufferedReader(new InputStreamReader(System.in));
				){
			String userInput;
			System.out.println("please input request: /n");;
			while ((userInput = stdIn.readLine()) != null) {
				out.println(userInput);
				//retrieve data from Spark driver after transformation and action
				@SuppressWarnings("unchecked")
				List<Person> persons= (List<Person>) queryInput.readObject();
				for (Person person : persons) {
					System.out.println("Result from person first name: " + person.getFirstName());
				}
			}
		} catch (UnknownHostException e) {
				System.err.println("Don't know about host " + hostName);
				System.exit(1);
		} catch (IOException e) {
				System.err.println("Couldn't get I/O for the connection to " + hostName);
				System.exit(1);
		}
	}
}
