package com.sigmaflow.trading;

import com.ib.client.Contract;

public class RecommendedTrade {
    private final String tradeId;
    private final String ticker;
    private final Contract optionContract;
    private final Contract stockContract;
    private final String optionAction; // "BUY" or "SELL"
    private final int optionRatio;
    private final String stockAction; // "BUY" or "SELL"
    private final int stockRatio;
    private final double netPrice;

    public RecommendedTrade(String tradeId, String ticker, Contract optionContract, Contract stockContract, 
                            String optionAction, int optionRatio, String stockAction, int stockRatio, double netPrice) {
        this.tradeId = tradeId;
        this.ticker = ticker;
        this.optionContract = optionContract;
        this.stockContract = stockContract;
        this.optionAction = optionAction;
        this.optionRatio = optionRatio;
        this.stockAction = stockAction;
        this.stockRatio = stockRatio;
        this.netPrice = netPrice;
    }

    public String getTradeId() {
        return tradeId;
    }

    public String getTicker() {
        return ticker;
    }

    public Contract getOptionContract() {
        return optionContract;
    }

    public Contract getStockContract() {
        return stockContract;
    }

    public String getOptionAction() {
        return optionAction;
    }

    public int getOptionRatio() {
        return optionRatio;
    }

    public String getStockAction() {
        return stockAction;
    }

    public int getStockRatio() {
        return stockRatio;
    }

    public double getNetPrice() {
        return netPrice;
    }
}
