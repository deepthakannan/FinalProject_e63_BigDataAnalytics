
public class TradeInfo {
	
	public TradeInfo(String symbol, String quantity, String action, String exchange)
	{
		
	}
	
	private String symbol; //"private" means access to this is restricted

	public String getSymbol()
	{
	     //include validation, logic, logging or whatever you like here
	    return this.symbol;
	}
}
