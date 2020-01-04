import java.io.Serializable;


import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

public class CQLClientTradeFrequencies implements Serializable{
   /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
private Cluster cluster;
   private Session session;

   public void connect(String node) {
      cluster = Cluster.builder()
            .addContactPoint(node).build();
      session = cluster.connect();
      Metadata metadata = cluster.getMetadata();
      System.out.printf("Connected to cluster: %s\n", 
            metadata.getClusterName());
      for ( Host host : metadata.getAllHosts() ) {
         System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n",
               host.getDatacenter(), host.getAddress(), host.getRack());
      }
   }
   
   public void loadData(String security, String action) {
	   
	   try
	   {
	   if(security==null || action == null)
		   return;
	   
	   if(Exists(security, action))
		   return;
		   
	   session.execute(
			      "INSERT INTO BIGDATAKEYSPACE.tradefrequencies (symbol_id, trade_action, trade_frequency) " +
			      "VALUES ('" + security + "', '" + action + "',0);");
	   }
	   catch(Exception ex)
	   {
		   System.out.println(ex.getMessage());
	   }
   }
   
   public void updateFrequency(String security, String action, Long timesTraded) {
	 
	   try
	   {
		   if(security==null || action == null || timesTraded == null)
			   return;
		   
		   if(!Exists(security, action))
			   return;
		   
		   session.execute(
				      "UPDATE BIGDATAKEYSPACE.tradefrequencies SET trade_frequency = " + timesTraded + 
				      " WHERE symbol_id = '" + security + "' AND trade_action = '" + action + "';");
   }
   catch(Exception ex)
   {
	   System.out.println(ex.getMessage());
   }
	   
   }
   
   public void querySchema(String security){
	   
	   ResultSet results = session.execute("SELECT * FROM BIGDATAKEYSPACE.tradefrequencies WHERE symbol_id = '" + security + "';");
	   System.out.println(String.format("%-30s\t%-20s\t%-20s\n%s", "Security", "Action", "No of times traded",
		    	  "-------------------------------+-----------------------+--------------------"));
	   for (Row row : results) {
		   System.out.println(row.toString());
		    //System.out.println(String.format("%-30s\t%-20s\t%-20s", row.getString("symbol_id"),  row.getString("trade_action"), 
		    //row.getInt("trade_frequency")));
		    //"89"));
		}
		System.out.println();
   }
   
public void querySchema(String security, String action){
	   
	ResultSet results = session.execute("SELECT * FROM BIGDATAKEYSPACE.tradefrequencies" +
		   	" WHERE symbol_id = '" + security + "' AND trade_action = '" + action + "';");
	for (Row row : results) {
		   System.out.println(row.toString());
		    //System.out.println(String.format("%-30s\t%-20s\t%-20s", row.getString("symbol_id"),  row.getString("trade_action"), 
		    //row.getInt("trade_frequency")));
		    //"89"));
		}
		System.out.println();
   }
   
   public boolean Exists(String security, String action){
	   ResultSet results = session.execute("SELECT * FROM BIGDATAKEYSPACE.tradefrequencies" +
			   	" WHERE symbol_id = '" + security + "' AND trade_action = '" + action + "';");
	   return results!=null && results.iterator() != null && results.iterator().hasNext();
	   
   }
   
   public void querySchema(){
	   System.out.println("querying Schema...");
	   ResultSet results = session.execute("SELECT * FROM BIGDATAKEYSPACE.tradefrequencies");
	   System.out.println(String.format("%-30s\t%-20s\t%-20s\n%s", "Security", "Action", "No of times traded",
		    	  "-------------------------------+-----------------------+--------------------"));
	   for (Row row : results) {
		    System.out.println(String.format("%-30s\t%-20s\t%-20s",
		    row.getString("symbol_id"),  row.getString("trade_action"), row.getInt("trade_frequency")));
		}
		System.out.println();
   }
   
   public void queryALL(){
	   ResultSet results = session.execute("select * from bigdatakeyspace.tradefrequencies where trade_action in ('BUY', 'SELL', 'COVER', 'SHORT') ALLOW FILTERING;");
	   System.out.println(String.format("%-30s\t%-20s\t%-20s\n%s", "Security", "Action", "No of times traded",
		    	  "-------------------------------+-----------------------+--------------------"));
	   for (Row row : results) {
		    System.out.println(String.format("%-30s\t%-20s\t%-20s",
		    row.getString("symbol_id"),  row.getString("trade_action"), row.getLong("trade_frequency")));
		}
		System.out.println();
   }
   
   
   
   public void close() {
      cluster.close(); // .shutdown();
   }
   
   public static void main(String[] args) throws Exception {
	   CQLClientTradeFrequencies dbClient = null;
	   dbClient = new CQLClientTradeFrequencies();
 	  dbClient.connect("127.0.0.1");
 	  while(true)
 	  {
 	 dbClient.queryALL();
 	 Thread.sleep(1000);
 	  }
 	 
	  }
}
