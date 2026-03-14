package bhav;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class TradeSimulator {
	Connection connection = null;
	private java.util.Date currentDay;
	ArrayList<Ticker> tickers=new ArrayList<Ticker>();
	HashSet<Ticker> trades=new HashSet<Ticker>();
	private HashMap<String,Ticker> bhav=null;
	  public final static String getPrices="select symbol,mktdate,open,close,high,low,50dma,150dma,200dma,20dma from bhav2014";
	  public final static String getPricesForTicker="select symbol,mktdate,open,close,high,low,50dma,150dma,200dma,20dma from bhav2014 where symbol=? and mktdate>=? order by mktdate asc";

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
		
		void loadBuyAlerts(String mktdate,int count){
			PreparedStatement selQuery;
			
			try{
			selQuery = connection.prepareStatement("select symbol,mktdate from ttbuyalert where mktdate=? limit "+count);
			ResultSet rs=selQuery.executeQuery();
			Ticker ticker=null;
			while(rs.next()){
				ticker=new Ticker();
				ticker.setSymbol(rs.getString("symbol"));
				ticker.setMktdate(rs.getString("mktdate"));
				trades.add(ticker);
				
			}
			
			}catch(SQLException e){
				e.printStackTrace();
			}
			System.out.println("Tickers "+tickers.size());
			
		}
		
		void loadTrades(){
			PreparedStatement selQuery;
			
			try{
			selQuery = connection.prepareStatement("select symbol,mktdate from ttbuyalertunique");
			ResultSet rs=selQuery.executeQuery();
			Ticker ticker=null;
			while(rs.next()){
				ticker=new Ticker();
				ticker.setSymbol(rs.getString("symbol"));
				ticker.setMktdate(rs.getString("mktdate"));
				tickers.add(ticker);
				
			}
			
			}catch(SQLException e){
				e.printStackTrace();
			}
			System.out.println("Tickers "+tickers.size());
		}

	void cycleTrade(Ticker ticker) throws SQLException{
		
		PreparedStatement selQuery;
		//System.out.println("Gettng details for "+ticker.getSymbol()+" Bough on "+ticker.getMktdate());
		try {
			selQuery = connection.prepareStatement(getPricesForTicker);
			selQuery.setString(1, ticker.getSymbol());
			selQuery.setString(2,ticker.getMktdate());
			ResultSet rs=selQuery.executeQuery();
			
			boolean first=true;
			double buyPrice=0.0;
			double stoploss=0.0;
			double exitPrice=0;
			String mktdate=null;
			int helddays=0;
			while(rs.next()){
			//	ticker=new Ticker();
				helddays++;
				ticker.setHeldDays(helddays);
				mktdate=rs.getString("mktdate");
				double close=rs.getDouble("close");
				if(first){
					buyPrice=close;
					ticker.setBuyPrice(buyPrice);
					ticker.setBuyDate(mktdate);
					stoploss=buyPrice-buyPrice*8/100;
					//System.out.println("BuyPrice : "+buyPrice+" Stoploss "+stoploss);
					
					
				}
				
				first=false;
				exitPrice=close;
				ticker.setExitPrice(exitPrice);
			
				double percentdiff=100*((close-buyPrice)/buyPrice);
				
				if(percentdiff>12){
					stoploss=close-close*8/100;
					//System.out.println("Close is : "+close+"New Stop Loss is "+stoploss);
				}
				if(close<stoploss){
					//System.out.println("Exiting stoploss reached "+mktdate);
					ticker.setExitDate(mktdate);
					//trades.add(ticker);
					break;
					
				}
				//exit positions that have squatted long and made no major profits
				if(helddays>30 && percentdiff<3){
					ticker.setExitDate(mktdate);
					System.err.println("Exiting "+ticker.getSymbol()+" Squatted!");
					break;
				}

			}
			ticker.setExitDate(mktdate);

			
		}catch(SQLException e){
			e.printStackTrace();
			throw e;
		}
	}
	void simulateTrade(String ticker,String mktdate){
		
	}
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int winners=0;
		int losers=0;
		double totalwin=0;
		double totalloss=0;
		TradeSimulator obj=new TradeSimulator();
		obj.connect();
	  	  SimpleDateFormat formatter=new SimpleDateFormat("yyyy-MM-dd");

	  	//  obj.currentDay=new java.util.Date(formatter.parse(buydate).getTime()); 
	  	  obj.loadTrades();
	  	  for(Ticker ticker:obj.tickers){
	  		obj.cycleTrade(ticker);
	  		

	  	  }
	  	  double capital=100000;
	  	  double totalearned=0;
	  	  int holdingperiod=0;
	  	 for(Ticker trade:obj.tickers){
	  		holdingperiod=holdingperiod+trade.getHeldDays();
		  		double exitprice=trade.getExitPrice();
		  		double buyprice=trade.getBuyPrice();
		  		double investedamount=capital/10;
		  		double Bought=investedamount/buyprice;
		  		double Sold=exitprice*Bought;
		  		double gainloss=Sold-(Bought*buyprice);
		  		gainloss=Math.round(gainloss*100.0)/100.0;
		  		double Result=investedamount+gainloss;
		  		capital=capital+gainloss;
		  		capital=Math.round(capital*100.0)/100.0;
//		  		System.err.print(trade.getSymbol()+"- ");
//		  		System.err.printf("%f\n",capital);
		  		
	  		 if(exitprice>=buyprice){
				winners++;
				double gain=100*((exitprice-buyprice)/buyprice);
				double profit=exitprice-buyprice;
				
				
				
				totalwin=totalwin+gain;
						System.out.println(trade.getSymbol()+" Bought on "+trade.getBuyPrice()+" on "+trade.getBuyDate()+"  Exit Price "+trade.getExitPrice()+" on "+trade.getExitDate()+" Gain :"+gainloss+" Held :"+trade.getHeldDays()+" Days");
		  	  }else{
		  		  losers++;
		  		double loss=100*((exitprice-buyprice)/buyprice);
		  		double lost=buyprice-exitprice;
				//capital=capital-lost;
		  		System.err.println(trade.getSymbol()+" Bought on "+trade.getBuyPrice()+" on "+trade.getBuyDate()+"  Exit Price "+trade.getExitPrice()+" on "+trade.getExitDate()+" Lost :"+gainloss+" Held :"+trade.getHeldDays()+" Days");
		  		totalloss=totalloss+loss;
		  	  }
	  		totalearned=totalearned+Result;
	}
	  	  System.err.println("Winners "+winners+" Losers "+losers);
	  	 System.err.println("Avg Holding period :"+holdingperiod/obj.tickers.size()+" Days");
	  	  
	  	 System.err.println("Win % "+totalwin+" Lost% "+totalloss);
	  	 System.err.println("Average Win % "+totalwin/winners+" Lost% "+totalloss/losers);
	  	 
	  	 System.err.println("Earned :"+capital);

	  
		
	}

}
