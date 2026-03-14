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


public class DeliveryDataProcessor {
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
	DeliveryDataProcessor obj=new DeliveryDataProcessor();
	
	Calendar cal = Calendar.getInstance();
	  SimpleDateFormat formatter=new SimpleDateFormat("yyyy-MM-dd");
	  
	  java.util.Date currentDay=cal.getTime();
	  
	String startdate=formatter.format(currentDay);
	System.err.println(startdate);
	obj.connect();
	obj.loadHolidays();	
	obj.processData();
	
	//obj.updatepositions(startdate);
	
	}
	
	 void processData() throws SQLException,IOException,Exception{
		 java.util.Date currentDay;

		Iterator it = FileUtils.iterateFiles(new File("H:/deliverydata/test"), null, false);
		File processedDir=new File("H:/deliverydata/processed");
        while(it.hasNext()){
        	File file=(File)it.next();
        	String fileName=file.getName();
            System.out.println(fileName);
            StringTokenizer fileTokens=new StringTokenizer(fileName, "_");
            fileTokens.nextToken();
            String extension=fileTokens.nextToken();
            StringTokenizer fileTokens2=new StringTokenizer(extension, ".");
            System.out.println("extension File Date "+extension);

            String fileDate=fileTokens2.nextToken();
            System.out.println("File Date "+fileDate);

      	  SimpleDateFormat formatter=new SimpleDateFormat("yyyy-MM-dd");
	  	  SimpleDateFormat formatter2=new SimpleDateFormat("ddMMyyyy");

	  	  currentDay=new java.util.Date(formatter2.parse(fileDate).getTime()); 

      	  
String mktdate=formatter.format(currentDay);
            
            
            System.out.println("File Date "+mktdate);

            if (dataAlreadyLoaded(fileDate)){
            	System.out.println("File Date already processed ..skipping .."+fileDate);
            	//return;
            }
            else
            readCsv(file,mktdate);
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
	


	
	private boolean dataAlreadyLoaded(String currentDate) throws SQLException{
		Statement selQuery;
		ResultSet rs=null;
		boolean exists=true;
		try {
			selQuery = connection.createStatement();;
 
  	  String getPrices="select symbol from delivery2020 WHERE MKTDATE='"+currentDate+"'";
			//selQuery.setString(1, prevDay);
			 rs=selQuery.executeQuery(getPrices);
			if(!rs.next()) exists=false;
			if(!exists){
				System.out.println("Adding date to calendar "+currentDate);
			//	addMarketDateToCalendar(currentDate);
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
	
	
	

	
	 private void readCsv(File file,String mktdate)
	 
     {
		 
		 //String fileName="C:\\Users\\Admin\\bHAV\\2018-03-01-NSE-EQ.txt";
		 
		 CSVReader reader = null;
		 PreparedStatement pstmt =null;
		 String symbol=null;
		 try {
			 reader = new CSVReader(new FileReader(file), ',');
			 
			
			
			
      

              String insertQuery = "Insert into delivery2020 (SYMBOL,MKTDATE,volume,delivery,percentagedel) values (?,?,?,?,?)";
connection.setAutoCommit(false);
              pstmt = connection.prepareStatement(insertQuery);

              String[] rowData = null;
              
            String type;
             String deliveryQuantity;
              String deliverypercentage;
              String volume;
    int counter=0;          
    reader.readNext();
    reader.readNext();
    reader.readNext();
    reader.readNext();
              while((rowData = reader.readNext()) != null)

              {
            	  //System.err.println(rowData.length);
            	 // System.out.println(rowData.toString());
                	//  System.out.println(rowData);
            	  if(rowData.length<=1){
            		  break;
            	  }
                	  symbol=rowData[2];
                	   type=rowData[3];
//
                	  SimpleDateFormat formatter=new SimpleDateFormat("yyyy-MM-dd");
                	    java.util.Date currentDay=new java.util.Date(formatter.parse(mktdate).getTime());              	 
                	 java.sql.Date sqlDate=new java.sql.Date(currentDay.getTime()) ;
//                	 //get previous day prices
                	 
                	 
                	 
                	
                	   volume=rowData[4];

                	   deliveryQuantity=rowData[5];
                	  
                	   deliverypercentage=rowData[6];
                
                	  if(volume.equals("-")){
                		  volume="0";
                  		  
                  	  }
                	     pstmt.setString(1, symbol);
                         pstmt.setDate(2, sqlDate);
                         pstmt.setString(3, volume);
                         pstmt.setString(4, deliveryQuantity);
                         pstmt.setString(5, deliverypercentage);
                      
          
//pstmt.executeUpdate();
//System.out.println("Data added ");
                	  if(type.equalsIgnoreCase("EQ"))
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
    	  System.err.println("Processing for symbol "+symbol+reader.getLinesRead());
              e.printStackTrace();

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
