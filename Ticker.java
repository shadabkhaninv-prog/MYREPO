package bhav;

public class Ticker {
	private String Symbol;
	private int open;
	private int high;
	private int low;
	private double buyQty;
	
	private double close;
	private double prevclose;
	private double dma50;
	private double dma10;
	private double dma200;
	private double dma30;
	private double highest;
	private double lowest;
	private double volatility;
	private double pocketChange;
	private boolean scalped=false;
	private double scalpPrice;
	private double scalpProfit;
	private double scalpedDay;
	private double scalpStoploss;
	private boolean passThreshold=false;
	private boolean passThreshold2=false;
	private boolean passThreshold3=false;
	private boolean passThreshold4=false;
	private String status;
	private double laddersl;
	private double initialstop;

	public double getInitialstop() {
		return initialstop;
	}

	public void setInitialstop(double initialstop) {
		this.initialstop = initialstop;
	}

	public double getLaddersl() {
		return laddersl;
	}

	public void setLaddersl(double laddersl) {
		this.laddersl = laddersl;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public boolean hasPassThreshold3() {
		return passThreshold3;
	}

	public void setPassThreshold3(boolean passThreshold3) {
		this.passThreshold3 = passThreshold3;
	}

	public boolean hasPassThreshold4() {
		return passThreshold4;
	}

	public void setPassThreshold4(boolean passThreshold4) {
		this.passThreshold4 = passThreshold4;
	}
	private double scalpedQty=0;
	private boolean scalpedAgain=false;
	private double againScalpPrice=0;
	private double remainingQty=0;
	private boolean idealBuypointreached=false;
	private double idealBuyPoint=0;
	private double pivotBuyPrice=0;
	private double breakoutVolume=0;
	private boolean gappedup=false;
	private double avgVolume=0;
	private double lastVolume=0;
	private double lastHigh=0;
	private double surgeFactor;
	private double stopLossFactor;
	private double breakoutHigh=0;
	private boolean crossedHigh=false;
	private double trueRange=0;
	private int volumerank;
	public boolean isProcessed() {
		return processed;
	}

	public void setProcessed(boolean processed) {
		this.processed = processed;
	}
	private double high10day;
	private double high20day;
	private double closediff;
	private double tightcloses;
	private double trailer;
	private double low10day;
	private String opendate;
	private String closedate;
	private boolean processed;
	private double reactionClose=0;
	private double returns;
	private boolean past20=false;
	private boolean past15=false;

	
	public boolean isPast20() {
		return past20;
	}

	public void setPast20(boolean past20) {
		this.past20 = past20;
	}

	public boolean isPast15() {
		return past15;
	}

	public void setPast15(boolean past15) {
		this.past15 = past15;
	}

	public double getReturns() {
		return returns;
	}

	public void setReturns(double returns) {
		this.returns = returns;
	}

	public double getReactionClose() {
		return reactionClose;
	}

	public void setReactionClose(double reactionClose) {
		this.reactionClose = reactionClose;
	}

	public double getLow10day() {
		return low10day;
	}

	public void setLow10day(double low10day) {
		this.low10day = low10day;
	}

	public double getVolumeondd() {
		return volumeondd;
	}

	public void setVolumeondd(double volumeondd) {
		this.volumeondd = volumeondd;
	}

	public double getDelivery() {
		return delivery;
	}

	public void setDelivery(double delivery) {
		this.delivery = delivery;
	}
	private double avgVolatility;
	private boolean strengthsell=false;
	private double volumeondd;
	private double delivery;
	public boolean isStrengthsell() {
		return strengthsell;
	}

	public void setStrengthsell(boolean strengthsell) {
		this.strengthsell = strengthsell;
	}

	public double getAvgVolatility() {
		return avgVolatility;
	}

	public void setAvgVolatility(double avgVolatility) {
		this.avgVolatility = avgVolatility;
	}

	public double getTrailer() {
		return trailer;
	}

	public void setTrailer(double trailer) {
		this.trailer = trailer;
	}

	public double getTightcloses() {
		return tightcloses;
	}

	public void setTightcloses(double tightcloses) {
		this.tightcloses = tightcloses;
	}

	public double getClosediff() {
		return closediff;
	}

	public void setClosediff(double closediff) {
		this.closediff = closediff;
	}

	public double getHigh10day() {
		return high10day;
	}

	public void setHigh10day(double high10day) {
		this.high10day = high10day;
	}

	public double getHigh20day() {
		return high20day;
	}

	public void setHigh20day(double high20day) {
		this.high20day = high20day;
	}

	public int getVolumerank() {
		return volumerank;
	}

	public void setVolumerank(int volumerank) {
		this.volumerank = volumerank;
	}

	public double getTrueRange() {
		return trueRange;
	}

	public void setTrueRange(double trueRange) {
		this.trueRange = trueRange;
	}

	public boolean isCrossedHigh() {
		return crossedHigh;
	}

	public void setCrossedHigh(boolean crossedHigh) {
		this.crossedHigh = crossedHigh;
	}

	public double getBreakoutHigh() {
		return breakoutHigh;
	}

	public void setBreakoutHigh(double breakoutHigh) {
		this.breakoutHigh = breakoutHigh;
	}

	public double getStopLossFactor() {
		return stopLossFactor;
	}

	public void setStopLossFactor(double stopLossFactor) {
		this.stopLossFactor = stopLossFactor;
	}

	public double getSurgeFactor() {
		return surgeFactor;
	}

	public void setSurgeFactor(double surgeFactor) {
		this.surgeFactor = surgeFactor;
	}

	public double getLastVolume() {
		return lastVolume;
	}

	public void setLastVolume(double lastVolume) {
		this.lastVolume = lastVolume;
	}

	public double getLastHigh() {
		return lastHigh;
	}

	public void setLastHigh(double lastHigh) {
		this.lastHigh = lastHigh;
	}

	public double getAvgVolume() {
		return avgVolume;
	}

	public void setAvgVolume(double avgVolume) {
		this.avgVolume = avgVolume;
	}

	public boolean isGappedup() {
		return gappedup;
	}

	public void setGappedup(boolean gappedup) {
		this.gappedup = gappedup;
	}

	public double getBreakoutVolume() {
		return breakoutVolume;
	}

	public void setBreakoutVolume(double breakoutVolume) {
		this.breakoutVolume = breakoutVolume;
	}

	public double getPivotBuyPrice() {
		return pivotBuyPrice;
	}

	public void setPivotBuyPrice(double pivotBuyPrice) {
		this.pivotBuyPrice = pivotBuyPrice;
	}

	public double getIdealBuyPoint() {
		return idealBuyPoint;
	}

	public void setIdealBuyPoint(double idealBuyPoint) {
		this.idealBuyPoint = idealBuyPoint;
	}

	public boolean isIdealBuypointreached() {
		return idealBuypointreached;
	}

	public void setIdealBuypointreached(boolean idealBuypointreached) {
		this.idealBuypointreached = idealBuypointreached;
	}

	public double getRemainingQty() {
		return remainingQty;
	}

	public void setRemainingQty(double remainingQty) {
		this.remainingQty = remainingQty;
	}

	public double getAgainScalpPrice() {
		return againScalpPrice;
	}

	public void setAgainScalpPrice(double againScalpPrice) {
		this.againScalpPrice = againScalpPrice;
	}

	public boolean isScalpedAgain() {
		return scalpedAgain;
	}

	public void setScalpedAgain(boolean scalpedAgain) {
		this.scalpedAgain = scalpedAgain;
	}

	public double getScalpedQty() {
		return scalpedQty;
	}

	public void setScalpedQty(double scalpedQty) {
		this.scalpedQty = scalpedQty;
	}

	public boolean hasPassThreshold() {
		return passThreshold;
	}

	public boolean hasPassThreshold2() {
		return passThreshold2;
	}

	public void setPassThreshold2(boolean passThreshold2) {
		this.passThreshold2 = passThreshold2;
	}

	public void setPassThreshold(boolean passThreshold) {
		this.passThreshold = passThreshold;
	}

	public boolean isLowHitOnsl() {
		return lowHitOnsl;
	}

	public void setLowHitOnsl(boolean lowHitOnsl) {
		this.lowHitOnsl = lowHitOnsl;
	}
	private double scalpedAmount;
	
	private boolean lowHitOnsl=false;
	
	
	public double getScalpedAmount() {
		return scalpedAmount;
	}

	public void setScalpedAmount(double scalpedAmount) {
		this.scalpedAmount = scalpedAmount;
	}

	public double getLowest() {
		return lowest;
	}

	public void setLowest(double lowest) {
		this.lowest = lowest;
	}
	
	
	public double getScalpedDay() {
		return scalpedDay;
	}

	public void setScalpedDay(double scalpedDay) {
		this.scalpedDay = scalpedDay;
	}

	public double getScalpStoploss() {
		return scalpStoploss;
	}

	public void setScalpStoploss(double scalpStoploss) {
		this.scalpStoploss = scalpStoploss;
	}

	public double getScalpProfit() {
		return scalpProfit;
	}

	public void setScalpProfit(double scalpProfit) {
		this.scalpProfit = scalpProfit;
	}

	public double getScalpPrice() {
		return scalpPrice;
	}

	public void setScalpPrice(double scalpPrice) {
		this.scalpPrice = scalpPrice;
	}

	public boolean isScalped() {
		return scalped;
	}

	public void setScalped(boolean scalped) {
		this.scalped = scalped;
	}

	public double getPocketChange() {
		return pocketChange;
	}

	public void setPocketChange(double pocketChange) {
		this.pocketChange = pocketChange;
	}

	public double getVolatility() {
		return volatility;
	}

	public void setVolatility(double volatility) {
		this.volatility = volatility;
	}

	public double getHighest() {
		return highest;
	}

	public void setHighest(double highest) {
		this.highest = highest;
	}
	private double dma20;
	public double getDma20() {
		return dma20;
	}

	public void setDma20(double dma20) {
		this.dma20 = dma20;
	}
	private double delta50;
	private int seq50;
	private String deltaturn50;
	private double roc50;
	private double delta200;
	private int seq200;
	private String deltaturn200;
	private String mktdate;
	private double exitPrice;
	private String exitDate;
	private double buyPrice;
	private String buyDate;
	private int heldDays;
	private boolean Squatted=false;
	private int breached20dma=0;
	private boolean neilWinner=false;
	private double allocatedCapital;
	private double currentValue;
	private double fivedayprice=0;
	private double roc;
	private double roc63;
	private boolean exitTommorow=false;
	private double bodayLow=0;
	private String positiveIndictor;
	
	public String getPositiveIndictor() {
		return positiveIndictor;
	}

	public void setPositiveIndictor(String positiveIndictor) {
		this.positiveIndictor = positiveIndictor;
	}

	public double getBodayLow() {
		return bodayLow;
	}

	public void setBodayLow(double bodayLow) {
		this.bodayLow = bodayLow;
	}

	public boolean isExitTommorow() {
		return exitTommorow;
	}

	public void setExitTommorow(boolean exitTommorow) {
		this.exitTommorow = exitTommorow;
	}

	public double getRoc() {
		return roc;
	}

	public void setRoc(double roc) {
		this.roc = roc;
	}

	public double getRoc63() {
		return roc63;
	}

	public void setRoc63(double roc63) {
		this.roc63 = roc63;
	}

	public double getFivedayprice() {
		return fivedayprice;
	}

	public void setFivedayprice(double fivedayprice) {
		this.fivedayprice = fivedayprice;
	}

	public double getAllocatedCapital() {
		return allocatedCapital;
	}

	public void setAllocatedCapital(double allocatedCapital) {
		this.allocatedCapital = allocatedCapital;
	}

	public double getCurrentValue() {
		return this.getBuyQty()*this.getBuyPrice();
	}

	public void setCurrentValue(double currentValue) {
		this.currentValue = currentValue;
	}

	public double getBuyQty() {
		return buyQty;
	}

	public void setBuyQty(double buyQty) {
		this.buyQty = buyQty;
	}
	
	public boolean isNeilWinner() {
		return neilWinner;
	}

	public void setNeilWinner(boolean neilWinner) {
		this.neilWinner = neilWinner;
	}

	public double getPercentGain() {
		return percentGain;
	}

	public void setPercentGain(double percentGain) {
		this.percentGain = percentGain;
	}
	private int breached50dma=0;
	private double percentGain=0;
	
	public int getBreached20dma() {
		return breached20dma;
	}

	public void setBreached20dma(int breached20dma) {
		this.breached20dma = breached20dma;
	}

	public int getBreached50dma() {
		return breached50dma;
	}

	public void setBreached50dma(int breached50dma) {
		this.breached50dma = breached50dma;
	}

	public boolean isSquatted() {
		return Squatted;
	}

	public void setSquatted(boolean squatted) {
		Squatted = squatted;
	}
	public boolean isStoplossHit() {
		return stoplossHit;
	}

	public void setStoplossHit(boolean stoplossHit) {
		this.stoplossHit = stoplossHit;
	}
	private boolean stoplossHit=false;
	
	private double stopLoss;
	
	private double lowday10;
	
	public double getLowday10() {
		return lowday10;
	}

	public void setLowday10(double lowday10) {
		this.lowday10 = lowday10;
	}

	public boolean equals(Object o){
		Ticker obj=(Ticker)o;
//		if(this.getSymbol().equals(obj.getSymbol()) && this.getBuyPrice()==obj.getBuyPrice() && this.getBuyDate().equals(obj.getBuyDate())){
//			//System.err.println(this.getSymbol()+" exists");
//			return true;
//		}
		
		if(this.getSymbol().equals(obj.getSymbol())){
		//System.err.println(this.getSymbol()+" exists");
		return true;
	}
		return false;
	}
	
	public int hashCode(){
		return this.getSymbol().hashCode();
	}
	
	public String toString(){
		return this.Symbol+" - "+this.buyDate+" - "+this.exitDate+"- BuyPrice "+this.buyPrice+"- ExitPrice "+this.getExitPrice()+"- BuyQty - "+this.getBuyQty()+"- PassThreshold "+this.hasPassThreshold();
	}
	
	public double getStopLoss() {
		return stopLoss;
	}
	public void setStopLoss(double stopLoss) {
		this.stopLoss = stopLoss;
	}

	public int getHeldDays() {
		return heldDays;
	}
	public void setHeldDays(int heldDays) {
		this.heldDays = heldDays;
	}
	public double getBuyPrice() {
		return buyPrice;
	}
	public void setBuyPrice(double buyPrice) {
		this.buyPrice = buyPrice;
	}
	public String getBuyDate() {
		return buyDate;
	}
	public void setBuyDate(String buyDate) {
		this.buyDate = buyDate;
	}
	public String getExitDate() {
		return exitDate;
	}
	public void setExitDate(String exitDate) {
		this.exitDate = exitDate;
	}
	public double getExitPrice() {
		return exitPrice;
	}
	public void setExitPrice(double exitPrice) {
		this.exitPrice = exitPrice;
	}
	public String getMktdate() {
		return mktdate;
	}
	public void setMktdate(String mktdate) {
		this.mktdate = mktdate;
	}
	public double getDma30() {
		return dma30;
	}
	public void setDma30(double dma30) {
		this.dma30 = dma30;
	}
	public double getDelta30() {
		return delta30;
	}
	public void setDelta30(double delta30) {
		this.delta30 = delta30;
	}
	public int getSeq30() {
		return seq30;
	}
	public void setSeq30(int seq30) {
		this.seq30 = seq30;
	}
	public String getDeltaturn30() {
		return deltaturn30;
	}
	public void setDeltaturn30(String deltaturn30) {
		this.deltaturn30 = deltaturn30;
	}
	private double delta10;
	private int seq10;
	private String deltaturn10;
	
	private double delta30;
	private int seq30;
	private String deltaturn30;
	
	public double getDelta10() {
		return delta10;
	}
	public void setDelta10(double delta10) {
		this.delta10 = delta10;
	}
	public int getSeq10() {
		return seq10;
	}
	public void setSeq10(int seq10) {
		this.seq10 = seq10;
	}
	public String getDeltaturn10() {
		return deltaturn10;
	}
	public void setDeltaturn10(String deltaturn10) {
		this.deltaturn10 = deltaturn10;
	}
	public double getDelta200() {
		return delta200;
	}
	public void setDelta200(double delta200) {
		this.delta200 = delta200;
	}
	public int getSeq200() {
		return seq200;
	}
	public void setSeq200(int seq200) {
		this.seq200 = seq200;
	}
	public String getDeltaturn200() {
		return deltaturn200;
	}
	public void setDeltaturn200(String deltaturn200) {
		this.deltaturn200 = deltaturn200;
	}
	
	public double getRoc50() {
		return roc50;
	}
	public void setRoc50(double roc50) {
		this.roc50 = roc50;
	}
	public void setRoc50(Double roc50) {
		this.roc50 = roc50;
	}
	public String getDeltaturn50() {
		return deltaturn50;
	}
	public void setDeltaturn50(String deltaturn50) {
		this.deltaturn50 = deltaturn50;
	}
	public double getDma50() {
		return dma50;
	}
	public void setDma50(double dma50) {
		this.dma50 = dma50;
	}
	public double getDma10() {
		return dma10;
	}
	public void setDma10(double dma10) {
		this.dma10 = dma10;
	}
	public double getDma200() {
		return dma200;
	}
	public void setDma200(double dma200) {
		this.dma200 = dma200;
	}
	public double getDelta50() {
		return delta50;
	}
	public void setDelta50(double delta50) {
		this.delta50 = delta50;
	}
	public int getSeq50() {
		return seq50;
	}
	public void setSeq50(int seq50) {
		this.seq50 = seq50;
	}
	public String getSymbol() {
		return Symbol;
	}
	public void setSymbol(String symbol) {
		Symbol = symbol;
	}
	public int getOpen() {
		return open;
	}
	public void setOpen(int open) {
		this.open = open;
	}
	public int getHigh() {
		return high;
	}
	public void setHigh(int high) {
		this.high = high;
	}
	public int getLow() {
		return low;
	}
	public void setLow(int low) {
		this.low = low;
	}
	public double getClose() {
		return close;
	}
	public void setClose(double close) {
		this.close = close;
	}
	public double getPrevclose() {
		return prevclose;
	}
	public void setPrevclose(double prevclose) {
		this.prevclose = prevclose;
	}

	
}
