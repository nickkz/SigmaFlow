package com.sigmaflow.trading;

import com.ib.client.Contract;
import com.ib.client.Decimal;
import com.sigmaflow.analytics.Volatility;
import com.sigmaflow.api.EWrapperImpl;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class PortfolioManager {

    private static final Logger logger = LogManager.getLogger(PortfolioManager.class);

    private final EWrapperImpl api;
    private static final AtomicInteger nextReqId = new AtomicInteger(2000); // Use a different range for these requests

    private final Map<String, Double> accountSummary = new ConcurrentHashMap<>();
    private final Map<String, Position> portfolio = new ConcurrentHashMap<>();
    private final Map<String, List<Double>> positionReturns = new ConcurrentHashMap<>();
    private final Volatility volatilityCalculator = new Volatility();

    public PortfolioManager(EWrapperImpl api) {
        this.api = api;
    }

    public void handleAccountSummary(String tag, String value) {
        try {
            accountSummary.put(tag, Double.parseDouble(value));
        } catch (NumberFormatException e) {
            // Ignore non-numeric values
        }
    }

    public void handlePosition(String account, Contract contract, Decimal position, double avgCost) {
        if (position.isZero()) {
            portfolio.remove(contract.symbol());
        } else {
            portfolio.put(contract.symbol(), new Position(contract, position.value().doubleValue(), avgCost));
        }
    }

    public void onPositionEnd() {
        logger.info("Finished receiving portfolio positions. Requesting historical data for VaR calculation...");
        requestHistoricalDataForPortfolio();
    }

    private void requestHistoricalDataForPortfolio() {
        for (Position pos : portfolio.values()) {
            int reqId = nextReqId.getAndIncrement();
            api.addPortfolioRequest(reqId, pos.getContract().symbol());
            
            String endDateTime = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")) + " 16:00:00";
            api.getClient().reqHistoricalData(reqId, pos.getContract(), endDateTime, "1 Y", "1 day", "TRADES", 1, 1, false, null);
        }
    }

    public void addHistoricalReturns(String symbol, List<Double> returns) {
        positionReturns.put(symbol, returns);
        
        // Check if all historical data has been received
        if (positionReturns.size() == portfolio.size()) {
            logger.info("All historical data received. Calculating portfolio risk...");
            displayRiskReport();
        }
    }

    private void displayRiskReport() {
        System.out.println("\n=====================================================================================================================================");
        System.out.println("PORTFOLIO RISK REPORT");
        System.out.println("=====================================================================================================================================");
        System.out.printf("%-15s | %-15s | %-15s | %-15s | %-15s | %-15s | %-15s | %-15s | %-15s | %-15s%n", 
                "Underlying", "Mark", "Value", "Long Value", "Short Value", "Value % NLV", "Unrealized P&L", "VaR 90%", "VaR 95%", "VaR 99%");
        System.out.println("-------------------------------------------------------------------------------------------------------------------------------------");

        double totalPortfolioValue = accountSummary.getOrDefault("NetLiquidation", 0.0);
        List<Double> allPositionValues = new ArrayList<>();
        List<List<Double>> allReturnsMatrix = new ArrayList<>();

        for (Position pos : portfolio.values()) {
            // This is a simplification. Mark price should come from market data.
            // For now, we'll estimate it based on avg cost or need to fetch it.
            // Let's assume avgCost is close enough for this example.
            double markPrice = pos.getAverageCost(); // Placeholder
            double value = markPrice * pos.getQuantity();
            double longValue = pos.getQuantity() > 0 ? value : 0;
            double shortValue = pos.getQuantity() < 0 ? value : 0;
            double valuePercentNlv = totalPortfolioValue != 0 ? (value / totalPortfolioValue) * 100 : 0;
            
            // Unrealized P&L would also need real-time price. Placeholder.
            double unrealizedPnl = 0.0; 

            List<Double> returns = positionReturns.get(pos.getContract().symbol());
            double annualizedVol = 0;
            if (returns != null) {
                // Simplified volatility calculation from returns
                double dailyVol = Math.sqrt(returns.stream().mapToDouble(r -> r * r).average().orElse(0.0));
                annualizedVol = dailyVol * Math.sqrt(252);
            }
            
            double var90 = volatilityCalculator.calculateParametricVaR(value, annualizedVol, 0.90);
            double var95 = volatilityCalculator.calculateParametricVaR(value, annualizedVol, 0.95);
            double var99 = volatilityCalculator.calculateParametricVaR(value, annualizedVol, 0.99);

            System.out.printf("%-15s | %-15.2f | %-15.2f | %-15.2f | %-15.2f | %-15.2f%% | %-15.2f | %-15.2f | %-15.2f | %-15.2f%n",
                    pos.getContract().symbol(), markPrice, value, longValue, shortValue, valuePercentNlv, unrealizedPnl, var90, var95, var99);
            
            allPositionValues.add(value);
            if (returns != null) {
                allReturnsMatrix.add(returns);
            }
        }
        
        System.out.println("-------------------------------------------------------------------------------------------------------------------------------------");
        
        // Portfolio VaR
        double portfolioVaR95 = volatilityCalculator.calculatePortfolioVaR(allPositionValues, allReturnsMatrix, 0.95);
        System.out.printf("Portfolio VaR (95%%): %.2f%n", portfolioVaR95);
        
        // What-if scenarios (placeholders)
        System.out.println("Portfolio VaR with Recommended Long Trade (95%): N/A");
        System.out.println("Portfolio VaR with Recommended Short Trade (95%): N/A");
        
        System.out.println("=====================================================================================================================================\n");
    }

    // Inner class to hold position data
    private static class Position {
        private final Contract contract;
        private final double quantity;
        private final double averageCost;

        public Position(Contract contract, double quantity, double averageCost) {
            this.contract = contract;
            this.quantity = quantity;
            this.averageCost = averageCost;
        }

        public Contract getContract() {
            return contract;
        }

        public double getQuantity() {
            return quantity;
        }

        public double getAverageCost() {
            return averageCost;
        }
    }
}
