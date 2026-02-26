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
    private final Map<String, Boolean> positionDataError = new ConcurrentHashMap<>(); // Track positions with data errors
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
        checkAndDisplayReport();
    }

    public void handleDataError(String symbol) {
        logger.warn("Marking position {} as having data error. VaR will not be calculated.", symbol);
        positionDataError.put(symbol, true);
        checkAndDisplayReport();
    }

    private void checkAndDisplayReport() {
        // Check if we have either returns or an error for every position
        boolean allComplete = true;
        for (String symbol : portfolio.keySet()) {
            if (!positionReturns.containsKey(symbol) && !positionDataError.containsKey(symbol)) {
                allComplete = false;
                break;
            }
        }

        if (allComplete) {
            logger.info("All historical data (or errors) received. Calculating portfolio risk...");
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
            String symbol = pos.getContract().symbol();
            double markPrice = pos.getAverageCost(); // Placeholder
            double value = markPrice * pos.getQuantity();
            double longValue = pos.getQuantity() > 0 ? value : 0;
            double shortValue = pos.getQuantity() < 0 ? value : 0;
            double valuePercentNlv = totalPortfolioValue != 0 ? (value / totalPortfolioValue) * 100 : 0;
            double unrealizedPnl = 0.0; 

            List<Double> returns = positionReturns.get(symbol);
            
            String var90Str = "N/A";
            String var95Str = "N/A";
            String var99Str = "N/A";

            if (returns != null && !returns.isEmpty()) {
                double dailyVol = Math.sqrt(returns.stream().mapToDouble(r -> r * r).average().orElse(0.0));
                double annualizedVol = dailyVol * Math.sqrt(252);
                
                double var90 = volatilityCalculator.calculateParametricVaR(value, annualizedVol, 0.90);
                double var95 = volatilityCalculator.calculateParametricVaR(value, annualizedVol, 0.95);
                double var99 = volatilityCalculator.calculateParametricVaR(value, annualizedVol, 0.99);
                
                var90Str = String.format("%.2f", var90);
                var95Str = String.format("%.2f", var95);
                var99Str = String.format("%.2f", var99);

                allPositionValues.add(value);
                allReturnsMatrix.add(returns);
            }

            System.out.printf("%-15s | %-15.2f | %-15.2f | %-15.2f | %-15.2f | %-15.2f%% | %-15.2f | %-15s | %-15s | %-15s%n",
                    symbol, markPrice, value, longValue, shortValue, valuePercentNlv, unrealizedPnl, var90Str, var95Str, var99Str);
        }
        
        System.out.println("-------------------------------------------------------------------------------------------------------------------------------------");
        
        // Portfolio VaR
        if (!allPositionValues.isEmpty()) {
            double portfolioVaR95 = volatilityCalculator.calculatePortfolioVaR(allPositionValues, allReturnsMatrix, 0.95);
            System.out.printf("Portfolio VaR (95%%): %.2f%n", portfolioVaR95);
        } else {
            System.out.println("Portfolio VaR (95%): N/A (Insufficient Data)");
        }
        
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
