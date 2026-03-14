package bhav;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.commons.io.FileUtils;

import com.opencsv.CSVReader;


public class SampleDBTest {
	Connection connection = null;
	int fileCounter=0;
	HashSet<String> holidays=new HashSet();
	 void connect(){
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			connection =
		       DriverManager.getConnection("jdbc:mysql://localhost/bhav?" +
		                                   "user=root&password=root");
		    
		    System.out.println("Connected !");
		} catch (Exception ex) {
		    // handle any errors
		    System.out.println("SQLException: " + ex.getMessage());
		    ex.printStackTrace();
		    //System.out.println("SQLState: " + ex.getSQLState());
		    //System.out.println("VendorError: " + ex.getErrorCode());
		}
		
	}
	public static void main(String[] args) throws SQLException,IOException,Exception{
	SampleDBTest obj=new SampleDBTest();	
	
	Calendar cal = Calendar.getInstance();
	  SimpleDateFormat formatter=new SimpleDateFormat("yyyy-MM-dd");
	  
	  java.util.Date currentDay=cal.getTime();	
	  
	String startdate=formatter.format(currentDay);	
	//startdate="2024-02-07";			
// 	String enddate="2024-12-22";
//	System.err.println(startdate+" -- "+enddate);
	obj.connect();
	obj.loadHolidays();	
	DownloadNSE nse=new DownloadNSE(startdate);	
//		
		nse.downloadDeliveryFile();
obj.processData();	
	obj.updatePositivedays();
	obj.updateMovingAverages(startdate, startdate);
	obj.loadTrendTemplate(startdate, startdate);
//	obj.loopBuyCandidates(startdate, startdate);
////	
//	obj.loopbuyalerts(startdate, startdate);
//	obj.updatepositions(startdate);
	
	}
	
	private void processData() throws SQLException,IOException{
		
		Iterator it = FileUtils.iterateFiles(new File("H:/marketdata/test"), null, false);
		File processedDir=new File("H:/marketdata/processed");
        while(it.hasNext()){
        	File file=(File)it.next();
        	String fileName=file.getName();
            System.out.println(fileName);
            StringTokenizer fileTokens=new StringTokenizer(fileName, "-");
            String day=fileTokens.nextToken();
            String month=fileTokens.nextToken();
            String year=fileTokens.nextToken();
            String fileDate=day+"-"+month+"-"+year;
            System.out.println("File Date "+fileDate);
            if (dataAlreadyLoaded(fileDate)){
            	System.out.println("File Date already processed ..skipping .."+fileDate);
            	//return;
            }
            else
            readCsv(file,fileDate);
            //copy file to another directory 
            FileUtils.moveFileToDirectory(file, processedDir,true);
            System.out.println("Moved after processed .."+file);
        }
	}
	
	private boolean isPrevDayHoliday(java.util.Date date){
		boolean isHoliday=false;
		
		DateFormat destDf = new SimpleDateFormat("dd-MMM-yy");
		            // format the date into another format

		           String dateStr = destDf.format(date);

		             

		
		isHoliday=holidays.contains(dateStr);
		return isHoliday;
		
	}
	
	 void loadHolidays() throws SQLException{
		
		String query="select Date from holidaylist";
		
		Statement selQuery;
		try {
			selQuery = connection.createStatement();
			ResultSet rs=selQuery.executeQuery(query);
			while(rs.next()){
				holidays.add(rs.getString("Date"));
			}
			System.out.println("loaded holidays "+holidays.size());
		}catch(SQLException e){
			e.printStackTrace();	
			throw e;
		}
		
		
	}
	
	private void updatePositivedays() throws SQLException{
		
	      String updatePositivedaysquery = "update bhav2024 set closeindictor=case when close-PREVCLOSE >=0 then 'P' else 'N' end";
	      		System.err.println(updatePositivedaysquery);
	    		  PreparedStatement pstmt = connection.prepareStatement(updatePositivedaysquery);
                  pstmt.execute();// add batch
                  pstmt.close();
                  connection.commit();
	    		  
	    		  
	    		  
	}
	
	public java.util.Date getPreviousWorkingDay(java.util.Date date) {
	    Calendar cal = Calendar.getInstance();
	    cal.setTime(date);

	    int dayOfWeek;
	    do {
	        cal.add(Calendar.DAY_OF_MONTH, -1);
	        dayOfWeek = cal.get(Calendar.DAY_OF_WEEK);
	    } while (dayOfWeek == Calendar.SATURDAY || dayOfWeek == Calendar.SUNDAY || isPrevDayHoliday(cal.getTime()));

	    return cal.getTime();
	}
	private void addMarketDateToCalendar(String currentDate) throws SQLException{
		
		String selQuery="insert into mktdatecalendar values(?)";
		PreparedStatement prep=null;
		try{
		prep=connection.prepareStatement(selQuery);
		
		prep.setString(1, currentDate);
		prep.execute();
		prep.close();
		}catch(SQLException e){
			throw e;
			
		}
		finally{
			prep.close();
		}
		
	}
	
	private void updateMovingAverages(String startdate,String enddate){
		CallableStatement stmt=null;
		try{
			connection.setAutoCommit(true);
		System.out.println("call setmovingaverages('"+startdate+"','"+enddate+"')");
		String setmaquery="call setmovingaverages(?,?)";
		stmt=connection.prepareCall(setmaquery);
		
		stmt.setString(1, startdate);
		stmt.setString(2, enddate);

		stmt.execute();
		}catch(SQLException e){
			e.printStackTrace();
		}
	}
	
	private void loadTrendTemplate(String startdate,String enddate){
		CallableStatement stmt=null;
		try{
			connection.setAutoCommit(true);
		System.out.println("call looptrendtemplate('"+startdate+"','"+enddate+"')");
		String setmaquery="call looptrendtemplate(?,?)";
		stmt=connection.prepareCall(setmaquery);
		
		stmt.setString(1, startdate);
		stmt.setString(2, enddate);
		stmt.execute();
		
		}catch(SQLException e){
			e.printStackTrace();
		}
	}
	
	private void loopBuyCandidates(String startdate,String enddate){
		CallableStatement stmt=null;
		try{
			connection.setAutoCommit(true);
		System.out.println("call loopbuycandidates('"+startdate+"','"+enddate+"')");
		String setmaquery="call loopbuycandidates(?,?)";
		stmt=connection.prepareCall(setmaquery);
		
		stmt.setString(1, startdate);
		stmt.setString(2, enddate);
		stmt.execute();
		
		}catch(SQLException e){
			e.printStackTrace();
		}
	}
	
	private void loopbuyalerts(String startdate,String enddate){
		CallableStatement stmt=null;
		try{
			connection.setAutoCommit(true);
		System.out.println("call loopbuyalerts('"+startdate+"','"+enddate+"')");
		String setmaquery="call loopbuyalerts(?,?)";
		stmt=connection.prepareCall(setmaquery);
		
		stmt.setString(1, startdate);
		stmt.setString(2, enddate);
		stmt.execute();
		
		}catch(SQLException e){
			e.printStackTrace();
		}
	}
	
	private void updatepositions(String startdate){
		CallableStatement stmt=null;
		try{
			connection.setAutoCommit(true);
		System.out.println("call updatepositions('"+startdate+"')");
		String setmaquery="call updatepositions(?)";
		stmt=connection.prepareCall(setmaquery);
		
		stmt.setString(1, startdate);
		
		stmt.execute();
		
		}catch(SQLException e){
			e.printStackTrace();
		}
	}
	
	private boolean dataAlreadyLoaded(String currentDate) throws SQLException{
		Statement selQuery;
		ResultSet rs=null;
		boolean exists=true;
		try {
			selQuery = connection.createStatement();;
 
  	  String getPrices="select symbol,close from bhav2024 WHERE MKTDATE='"+currentDate+"'";
			//selQuery.setString(1, prevDay);
			 rs=selQuery.executeQuery(getPrices);
			if(!rs.next()) exists=false;
			if(!exists){
				System.out.println("Adding date to calendar "+currentDate);
				addMarketDateToCalendar(currentDate);
			}
//			while (rs.next()){
//				previousPrices.put(rs.getString("symbol"),rs.getString("close"));
//			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw e;
		}
		
		return exists;
		
	}
	
	
	
	private HashMap<String,String> getCloseForPreviousDay(String currentDay) throws SQLException{
		//java.util.Date previousDay=getPreviousWorkingDay(currentDay);
		
		
		
		
		HashMap<String,String> previousPrices=new HashMap();
		Statement selQuery;
		try {
			selQuery = connection.createStatement();;
			 SimpleDateFormat format1 = new SimpleDateFormat("yyyy-MM-dd");
      	  
  	    String prevDay=TradeUtil.getPreviousDay(currentDay,connection);  	 
  	 
  	  String getPrices="select symbol,close from bhav2024 WHERE MKTDATE='"+prevDay+"'";
			//selQuery.setString(1, prevDay);
			ResultSet rs=selQuery.executeQuery(getPrices);
			while (rs.next()){
				previousPrices.put(rs.getString("symbol"),rs.getString("close"));
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw e;
		}
		return previousPrices;
		
	}
	
	 private void readCsv(File file,String fileDate)
	 
     {
		 
		 //String fileName="C:\\Users\\Admin\\bHAV\\2018-03-01-NSE-EQ.txt";
		 
		 CSVReader reader = null;
		 PreparedStatement pstmt =null;
		 String symbol=null;
		 try {
			 reader = new CSVReader(new FileReader(file), ',');
			 
  
			
			
			
      

              String insertQuery = "Insert into bhav2024 (SYMBOL,MKTDATE,OPEN,HIGH,LOW,CLOSE,VOLUME,PREVCLOSE) values (?,?,?,?,?,?,?,?)";
connection.setAutoCommit(false);
              pstmt = connection.prepareStatement(insertQuery);

              String[] rowData = null;
              
            
              String open;
              String high;
              String low;
              String close;
              String volume;
              String mktdate=null;
           	  String prevClosePrice=null;

    int counter=0;          
    HashMap<String, String> previousCloses=null;
    boolean noPreviousDay=true;

              while((rowData = reader.readNext()) != null)

              {
            	  //System.err.println(rowData.length);
            	  //System.out.println(rowData.toString());
                	  //System.out.println(rowData);
            	  if(rowData.length<=1){
            		  break;
            	  }
                	  symbol=rowData[0];
//                	  String ticker=rowData[10];
                	  String series=rowData[1];
                	  if(!series.equals("EQ")){
                		  System.err.println("Ignoring Sreies is "+series);
                		  continue;
                	  }
                	  
                	  SimpleDateFormat formatter=new SimpleDateFormat("yyyy-MM-dd");
//
                	    java.util.Date currentDay=new java.util.Date(formatter.parse(fileDate).getTime());              	 
                	 java.sql.Date sqlDate=new java.sql.Date(formatter.parse(fileDate).getTime()) ;
//                	 //get previous day prices
//                	 
                	 System.err.println("Date is "+sqlDate);

//                	 if(counter==0){
//                		                 		 previousCloses=getCloseForPreviousDay(sqlDate.toString());
//                		                 		 noPreviousDay=previousCloses.isEmpty();
//                	 }
                	 
                	  open=rowData[2];
                	  high=rowData[3];
                	  low=rowData[4];
                	  close=rowData[5];
                	  volume=rowData[8];
                   	   prevClosePrice=rowData[7];

                	  if(low.equals("-")){
                		  low="0";
                		  
                	  }
                  	  if(open.equals("-")){
                  		open="0";
                		  
                	  }
                  	  if(high.equals("-")){
                  		high="0";
                		  
                	  }
                  	  if(close.equals("-")){
                  		close="0";
                		  
                	  }
                	  if(volume.equals("-")){
                		  volume="0";
                  		  
                  	  }
            
          
                          pstmt.setString(1, symbol);
                          pstmt.setDate(2, sqlDate);
                          pstmt.setString(3, open);
                          pstmt.setString(4, high);
                          pstmt.setString(5, low);
                          pstmt.setString(6, close);
                          pstmt.setString(7, volume);
                          
                          
                        	  
                          pstmt.setString(8, prevClosePrice);
//pstmt.executeUpdate();
//System.out.println("Data added ");
                         pstmt.addBatch();// add batch
                         
counter++;




              }
              System.out.println("Starting to commit "+System.currentTimeMillis());
              pstmt.executeBatch();
              connection.commit();
              System.out.println("eneded to commit "+System.currentTimeMillis());
              fileCounter++;
              System.out.println("Data Successfully Uploaded "+fileCounter);
              

      }

      catch (Exception e)

      {
          e.printStackTrace();

//    	  System.err.println("Processing for symbol "+symbol+reader.getLinesRead());
//              e.printStackTrace();

      }
		 finally{
			 try{
			 reader.close();
			 pstmt.close();
			 }catch(Exception e){
				 e.printStackTrace();
			 }
		 }



     }


	
}
