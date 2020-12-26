import java.io.*;

public class CreateAJarFile {

public static void main(String[] args) {


        String content = "sqoop import --connect 'jdbc:sqlserver://serverm13.database.windows.net:1433;database=databasem13' --username batchm13 --password somepasswordD1! --driver com.microsoft.sqlserver.jdbc.SQLServerDriver --table COVID_CASES --hcatalog-table covid_Cases --split-by rowkey -m 1";

        // If the file doesn't exists, create and write to it
        // If the file exists, truncate (remove all content) and write to it
                         try (FileWriter writer = new FileWriter("app.log");
                                      BufferedWriter bw = new BufferedWriter(writer)) {
        
                                                  bw.write(content);
        
                                                          } catch (IOException e) {
                                                                      System.err.format("IOException: %s%n", e);
                                                                             }	  


 // String scriptPath = args[0];
    execute("cat app.log");
    execute("/bin/chmod u+x app.log");
    execute("/bin/sh app.log");
  }
 
  private static void execute(String command) {
    try {
      Process p = Runtime.getRuntime().exec(command);
      BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
      String line = "";
      while ((line = input.readLine()) != null) {
        System.out.println(line);
      }
      input.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
