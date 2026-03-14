package bhav;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

public class BhavIncliner {
	Connection connection = null;
	HashMap<String,Ticker> prevDaytickers=null;
	HashMap<String,Ticker> curDaytickers=null;
	HashMap<String,Ticker> updatedTickers=null;
	ArrayList<String> mktdates=null;
	
	 static final String prevDayBhav="select mktdate,symbol,50dma,close,200dayma,150dayma,20dma from bhav2017ma where mktdate=(select mktdate from mktdatecalendar where mktdate<? order by mktdate desc limit 1)";
	 static final String currentDayBhav="select symbol,mktdate,close,50dma,200dma,30dma from bhav2017 where mktdate=?";
	 //static final String insertMovingAverages="INSERT INTO bhav2017ma(mktdate,symbol,50dma,50daydelta,50daysequence,close,50dayturn,200dayma,200daysequence,200daydelta,30dma,30daydelta,30daysequence,200dayturn,30dayturn)VALUES(?,?,?,round(?,2),?,?,?,round(?,2),?,?,round(?,2),?,round(?,2),?,?,?)";
	 static final String insertMovingAverages="INSERT INTO bhav2017ma(mktdate,symbol,50dma,close,200dayma,150dayma,20dma)VALUES(?,?,?,?,?,?,?)";
	
	private void connect(){
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
	
	private void getPreviousRows(String mktdate) throws SQLException{
		if(prevDaytickers!=null){
			return;
		}
		Ticker prevRow=null;
		PreparedStatement selQuery=null;
		prevDaytickers=new HashMap<String,Ticker>();
		try {
			selQuery = connection.prepareStatement(prevDayBhav);
			selQuery.setString(1, mktdate);
			ResultSet rs=selQuery.executeQuery();
		
			while(rs.next()){
				prevRow=new Ticker();
				String symbol=rs.getString(1);
				prevRow.setSymbol(symbol);
				prevRow.setDma50(rs.getDouble(4));
				prevRow.setSeq50(rs.getInt(6));
				prevRow.setDeltaturn50(rs.getString(7));
				prevRow.setRoc50(rs.getDouble(8));
				prevDaytickers.put(symbol,prevRow);
				
			}
			System.out.println("Loaded PreviousRow"
					+ " "+prevDaytickers.size());
		}catch(SQLException e){
			e.printStackTrace();
			throw e;
		}
		finally{
			selQuery.close();
		}
		
	}
	private void getCurrentRow(String mktdate) throws SQLException{

		Ticker currentRow=null;
		PreparedStatement selQuery=null;
		curDaytickers=new HashMap<String,Ticker>();
		try {
			selQuery = connection.prepareStatement(currentDayBhav);
			//System.err.println(currentDayBhav);
			selQuery.setString(1, mktdate);
			ResultSet rs=selQuery.executeQuery();
		
			while(rs.next()){
				currentRow=new Ticker();
				String symbol=rs.getString(1);
				currentRow.setSymbol(symbol);
				currentRow.setClose(Double.parseDouble(rs.getString(3).trim()));
				currentRow.setDma50(rs.getDouble(4));
				currentRow.setDma200(rs.getDouble(5));
				currentRow.setDma30(rs.getDouble(6));
				
				curDaytickers.put(symbol,currentRow);				
			}
			//System.out.println("Loaded current day data "					+ " "+curDaytickers.size());
		}catch(SQLException e){
			e.printStackTrace();
			throw e;
		}
		finally{
			selQuery.close();
		}
		
	}
	
	private void setupAverageDeltas(){
		System.err.println("Setting up averages");
		Iterator<String> it=curDaytickers.keySet().iterator();
		updatedTickers=new HashMap<String,Ticker>();
		while(it.hasNext()){
			String symbol=it.next();
			Ticker curticker=curDaytickers.get(symbol);
			Ticker prevticker=prevDaytickers.get(symbol);
			if(prevticker!=null){
			double prev50dayma=prevticker.getDma50();
			double cur50dayma=curticker.getDma50();
			double delta=(100*(cur50dayma-prev50dayma))/prev50dayma;
			double prevdelta=prevticker.getDelta50();
			double roc50=0.0;
			if(prevdelta!=0){
			roc50=(100*(delta-prevdelta))/prevdelta;
			}
			
		
			curticker.setDelta50(delta);
			curticker.setRoc50(roc50);
			
			
			if((prevdelta<0 && delta>0)){
				curticker.setDeltaturn50("Y");
			}else{
				curticker.setDeltaturn50("N");
			}
			if(delta>=0){
				curticker.setSeq50(prevticker.getSeq50()+1);
				
			}else{
				curticker.setSeq50(0);
			}
			if(prevticker.getSeq50()>1){
				if(delta<0){
					curticker.setDeltaturn50("E");
				}
			}
			//200 day dma calculations
			double prev200dayma=prevticker.getDma200();
			double cur200dayma=curticker.getDma200();
			double delta200=(100*(cur200dayma-prev200dayma))/prev200dayma;
			double prevdelta200=prevticker.getDelta200();

			curticker.setDelta200(delta200);
			if((prevdelta200<0 && delta200>0)){
				curticker.setDeltaturn200("Y");
			}else{
				curticker.setDeltaturn200("N");
			}
			if(delta200>=0){
				curticker.setSeq200(prevticker.getSeq200()+1);
				
			}else{
				curticker.setSeq200(0);
			}
			
			//30 dma calculations
			//200 day dma calculations
			double prev30dayma=prevticker.getDma30();
			double cur30dayma=curticker.getDma30();
			double delta30=(100*(cur30dayma-prev30dayma))/prev30dayma;
			double prevdelta30=prevticker.getDelta30();

			curticker.setDelta30(delta30);
			if((prevdelta30<0 && delta30>0)){
				curticker.setDeltaturn30("Y");
			}else{
				curticker.setDeltaturn30("N");
			}
			if(delta30>=0){
				curticker.setSeq30(prevticker.getSeq30()+1);
				
			}else{
				curticker.setSeq30(0);
			}
			
			}
			updatedTickers.put(symbol,curticker);
		}
		prevDaytickers=updatedTickers;
	}
	private void updateSequenceDeltas(String mktdate) throws SQLException{
		Collection<Ticker> tickers=updatedTickers.values();
		PreparedStatement prep=null;
		try{
		connection.setAutoCommit(false);
		prep=connection.prepareStatement(insertMovingAverages);
//"INSERT INTO bhav2015ma(mktdate,symbol,50dayma,50daydelta,50daysequence,close,50dayturn,50deltaroc,
		//200dayma,200daysequence,200daydelta,30dma,30daydelta,30daysequence

		
		for(Ticker t:tickers){
			prep.setString(1, mktdate);
			prep.setString(2, t.getSymbol());
			prep.setDouble(3, t.getDma50());
			prep.setDouble(4, t.getDelta50());
			prep.setInt(5, t.getSeq50());
			prep.setDouble(6, t.getClose());
			prep.setString(7, t.getDeltaturn50());
			prep.setDouble(8, t.getRoc50());
			prep.setDouble(9, t.getDma200());
			prep.setInt(10, t.getSeq200());
			prep.setDouble(11, t.getDelta200());
			prep.setDouble(12, t.getDma30());
			prep.setDouble(13, t.getDelta30());
			prep.setInt(14, t.getSeq30());
			prep.setString(15, t.getDeltaturn200());
			prep.setString(16, t.getDeltaturn30());
			
			prep.addBatch();
		}
		prep.executeBatch();
		connection.commit();
		}      catch (Exception e)

	      {
	    	  //System.err.println("Processing for symbol "+symbol+reader.getLinesRead());
	              e.printStackTrace();

	      }
			 finally{
				 try{
				 
				 prep.close();
				 //connection.close();
				 
				 }catch(Exception e){
					 e.printStackTrace();
				 }
			 }
	}
	private void loadMarketDates(String mktdate)throws SQLException{
		String sql="select distinct mktdate from mktdatecalendar where mktdate>=? order by mktdate asc";
		PreparedStatement prep=connection.prepareStatement(sql);
		prep.setString(1, mktdate);
		ResultSet rs=prep.executeQuery();
		mktdates=new ArrayList<String>();
		while(rs.next()){
		mktdates.add(rs.getString(1))
		;
		}
		
	}
	public static void main(String[] args) throws SQLException,Exception {
		// TODO Auto-generated method stub
		BhavIncliner obj=new BhavIncliner();
		String mktdate="2017-01-02";
		obj.connect();
		obj.loadMarketDates(mktdate);
		for(String mdate : obj.mktdates){
		System.err.println("Processing for "+mdate);
		obj.getPreviousRows(mdate);
		obj.getCurrentRow(mdate);
		obj.setupAverageDeltas();
		obj.updateSequenceDeltas(mdate);
		}
		
		
		
	}

}
