package com.sigmaflow.data;

import com.sigmaflow.api.EWrapperImpl;
import com.ib.client.Bar;
import com.ib.client.Contract;
import com.ib.client.ContractDetails;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class MarketData {

    private static final Logger logger = LogManager.getLogger(MarketData.class);

    public enum DataSource {
        SIMULATED,
        LIVE
    }

    public enum RequestType {
        CONTRACT_DETAILS,
        UNDERLYING_MARKET_DATA,
        OPTION_CHAIN_PARAMS,
        HISTORICAL_DATA,
        OPTION_MARKET_DATA,
        HISTORICAL_VOLATILITY,
        OPTION_IMPLIED_VOLATILITY
    }

    private final List<String> tickers;
    private final DataSource dataSource;
    private final EWrapperImpl api;
    private static final AtomicInteger nextReqId = new AtomicInteger(1);

    private final Map<String, Double> underlyingPrices = new ConcurrentHashMap<>();
    private final Map<Integer, String> reqIdToTickerMap = new ConcurrentHashMap<>();
    private final Map<String, Integer> tickerToConIdMap = new ConcurrentHashMap<>();
    private final Map<Integer, RequestType> reqIdToRequestType = new ConcurrentHashMap<>();
    
    // Data storage for report
    private final Map<String, ContractDetails> contractDetailsMap = new ConcurrentHashMap<>();
    private final Map<String, List<Bar>> historicalBars = new ConcurrentHashMap<>();
    private final Map<String, Map<LocalDate, Double>> historicalVolatility = new ConcurrentHashMap<>();
    private final Map<String, Map<LocalDate, Double>> optionImpliedVolatility = new ConcurrentHashMap<>();
    private final Map<String, String> optionChainSummary = new ConcurrentHashMap<>();
    
    // Track completed tickers
    private final Set<String> completedTickers = ConcurrentHashMap.newKeySet();


    public MarketData(DataSource dataSource, String[] tickers, EWrapperImpl api) {
        this.dataSource = dataSource;
        this.tickers = new ArrayList<>(Arrays.asList(tickers));
        this.api = api;
        this.api.setMarketData(this);
    }

    public void fetchMarketData() {
        if (dataSource == DataSource.SIMULATED) {
            fetchSimulatedMarketData();
        } else {
            fetchLiveMarketData();
        }
    }

    private void fetchLiveMarketData() {
        for (String ticker : tickers) {
            fetchContractDetails(ticker);
        }
    }

    public void fetchContractDetails(String ticker) {
        Contract contract = createStockContract(ticker);
        int reqId = nextReqId.getAndIncrement();
        reqIdToTickerMap.put(reqId, ticker);
        reqIdToRequestType.put(reqId, RequestType.CONTRACT_DETAILS);
        api.getClient().reqContractDetails(reqId, contract);
    }

    public void handleContractDetails(String ticker, ContractDetails contractDetails) {
        contractDetailsMap.put(ticker, contractDetails);
        setConId(ticker, contractDetails.contract().conid());
        requestUnderlyingMarketData(ticker, contractDetails.contract().conid());
    }

    public void requestUnderlyingMarketData(String ticker, int conId) {
        int reqId = nextReqId.getAndIncrement();
        reqIdToTickerMap.put(reqId, ticker);
        reqIdToRequestType.put(reqId, RequestType.UNDERLYING_MARKET_DATA);
        Contract contract = createStockContract(ticker);
        contract.conid(conId);
        api.getClient().reqMktData(reqId, contract, "", true, false, null);
    }

    public void requestHistoricalData(String ticker, int conId) {
        int reqId = nextReqId.getAndIncrement();
        reqIdToTickerMap.put(reqId, ticker);
        reqIdToRequestType.put(reqId, RequestType.HISTORICAL_DATA);
        Contract contract = createStockContract(ticker);
        contract.conid(conId);

        String endDateTime = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")) + " 16:00:00";
        api.getClient().reqHistoricalData(reqId, contract, endDateTime, "1 M", "1 day", "TRADES", 1, 1, false, null);
    }

    public void requestHistoricalVolatility(String ticker, int conId) {
        int reqId = nextReqId.getAndIncrement();
        reqIdToTickerMap.put(reqId, ticker);
        reqIdToRequestType.put(reqId, RequestType.HISTORICAL_VOLATILITY);
        Contract contract = createStockContract(ticker);
        contract.conid(conId);

        String endDateTime = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")) + " 16:00:00";
        api.getClient().reqHistoricalData(reqId, contract, endDateTime, "30 D", "1 day", "HISTORICAL_VOLATILITY", 1, 1, false, null);
    }

    public void requestStockImpliedVolatility(String ticker, int conId) {
        int reqId = nextReqId.getAndIncrement();
        reqIdToTickerMap.put(reqId, ticker);
        reqIdToRequestType.put(reqId, RequestType.OPTION_IMPLIED_VOLATILITY);
        Contract contract = createStockContract(ticker);
        contract.conid(conId);

        String endDateTime = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")) + " 16:00:00";
        api.getClient().reqHistoricalData(reqId, contract, endDateTime, "30 D", "1 day", "OPTION_IMPLIED_VOLATILITY", 1, 1, false, null);
    }

    public void setUnderlyingPrice(int reqId, double price) {
        String ticker = reqIdToTickerMap.get(reqId);
        if (ticker != null) {
            underlyingPrices.put(ticker, price);
            logger.info("Updated underlying price for " + ticker + " to " + price);
            
            Integer conId = tickerToConIdMap.get(ticker);
            if (conId != null) {
                int optionReqId = nextReqId.getAndIncrement();
                reqIdToTickerMap.put(optionReqId, ticker);
                reqIdToRequestType.put(optionReqId, RequestType.OPTION_CHAIN_PARAMS);
                api.getClient().reqSecDefOptParams(optionReqId, ticker, "", "STK", conId);

                requestHistoricalData(ticker, conId);
                requestHistoricalVolatility(ticker, conId);
                requestStockImpliedVolatility(ticker, conId);
            }
            api.getClient().cancelMktData(reqId);
            reqIdToTickerMap.remove(reqId);
            reqIdToRequestType.remove(reqId);
        }
    }

    public void setConId(String ticker, int conId) {
        tickerToConIdMap.put(ticker, conId);
    }

    public void addHistoricalBar(int reqId, Bar bar) {
        String ticker = reqIdToTickerMap.get(reqId);
        if (ticker != null) {
            historicalBars.computeIfAbsent(ticker, k -> new ArrayList<>()).add(bar);
        }
    }

    public void historicalDataEnd(int reqId, String startDateStr, String endDateStr) {
        String ticker = reqIdToTickerMap.get(reqId);
        if (ticker != null) {
            logger.info("Finished receiving " + reqIdToRequestType.get(reqId) + " for " + ticker);
            reqIdToTickerMap.remove(reqId);
            reqIdToRequestType.remove(reqId);
            checkAndDisplay(ticker);
        }
    }

    public void processOptionChainParameters(int reqId, Set<String> expirations, Set<Double> strikes) {
        String ticker = reqIdToTickerMap.get(reqId);

        if (ticker == null) {
            logger.debug("reqId {}. Ticker or price not found.", reqId);
            return;
        }

        Double underlyingPrice = underlyingPrices.get(ticker);
        if (underlyingPrice == null) {
            logger.debug("reqId {}. Ticker or price not found.", reqId);
            return;
        }

        LocalDate today = LocalDate.now();
        LocalDate oneMonthFromNow = today.plusMonths(1);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");

        Set<String> filteredExpirations = expirations.stream()
                .filter(exp -> {
                    try {
                        LocalDate expDate = LocalDate.parse(exp, formatter);
                        return !expDate.isBefore(today) && !expDate.isAfter(oneMonthFromNow);
                    } catch (Exception e) { return false; }
                })
                .collect(Collectors.toCollection(TreeSet::new));

        double lowerBound = underlyingPrice * 0.8;
        double upperBound = underlyingPrice * 1.2;
        Set<Double> filteredStrikes = strikes.stream()
                .filter(strike -> strike >= lowerBound && strike <= upperBound)
                .collect(Collectors.toCollection(TreeSet::new));

        String summary = String.format("Expirations (<= 1 Month): %s\nStrikes (+/- 20%%): %s", filteredExpirations, filteredStrikes);
        optionChainSummary.put(ticker, summary);

        reqIdToTickerMap.remove(reqId);
        reqIdToRequestType.remove(reqId);
        checkAndDisplay(ticker);
    }

    public void setHistoricalVolatility(int reqId, String dateStr, double volatility) {
        String ticker = reqIdToTickerMap.get(reqId);
        if (ticker != null) {
            try {
                LocalDate date = LocalDate.parse(dateStr, DateTimeFormatter.ofPattern("yyyyMMdd"));
                historicalVolatility.computeIfAbsent(ticker, k -> new ConcurrentSkipListMap<>()).put(date, volatility);
            } catch (Exception e) {
                logger.error("Error parsing date for historical volatility: " + dateStr, e);
            }
        }
    }

    public void setOptionImpliedVolatility(int reqId, String dateStr, double volatility) {
        String ticker = reqIdToTickerMap.get(reqId);
        if (ticker != null) {
            try {
                LocalDate date = LocalDate.parse(dateStr, DateTimeFormatter.ofPattern("yyyyMMdd"));
                optionImpliedVolatility.computeIfAbsent(ticker, k -> new ConcurrentSkipListMap<>()).put(date, volatility);
            } catch (Exception e) {
                logger.error("Error parsing date for implied volatility: " + dateStr, e);
            }
        }
    }
    
    public RequestType getRequestType(int reqId) {
        return reqIdToRequestType.get(reqId);
    }

    private synchronized void checkAndDisplay(String ticker) {
        if (contractDetailsMap.containsKey(ticker) &&
            historicalBars.containsKey(ticker) &&
            historicalVolatility.containsKey(ticker) &&
            optionImpliedVolatility.containsKey(ticker) &&
            optionChainSummary.containsKey(ticker)) {

            completedTickers.add(ticker);
            printTickerReport(ticker);
            
            if (completedTickers.size() == tickers.size()) {
                printFinalStatisticsTable();
            }
        }
    }

    private void printTickerReport(String ticker) {
        ContractDetails cd = contractDetailsMap.get(ticker);
        List<Bar> bars = historicalBars.get(ticker);
        Map<LocalDate, Double> histVolMap = historicalVolatility.get(ticker);
        Map<LocalDate, Double> impVolMap = optionImpliedVolatility.get(ticker);
        String chainSummary = optionChainSummary.get(ticker);

        System.out.println("\n==================================================");
        System.out.println("REPORT FOR TICKER: " + ticker);
        System.out.println("==================================================");
        System.out.println("1. Contract Details:");
        System.out.println("   Company Name: " + cd.longName());
        System.out.println("   Primary Exchange: " + cd.contract().primaryExch());
        System.out.println("--------------------------------------------------");
        System.out.println("2. Historical Daily Prices (Last 30 Days):");
        if (bars != null && !bars.isEmpty()) {
            System.out.println("   Total Bars: " + bars.size());
            System.out.println("   First Bar: " + bars.get(0).time() + " Close: " + bars.get(0).close());
            System.out.println("   Last Bar:  " + bars.get(bars.size()-1).time() + " Close: " + bars.get(bars.size()-1).close());
        }
        System.out.println("--------------------------------------------------");
        
        if (histVolMap != null && !histVolMap.isEmpty()) {
            ConcurrentSkipListMap<LocalDate, Double> sortedMap = (ConcurrentSkipListMap<LocalDate, Double>) histVolMap;
            System.out.println("3. Historical Volatility (30-day):");
            System.out.println("   First Date: " + sortedMap.firstKey() + " Value: " + sortedMap.firstEntry().getValue());
            System.out.println("   Last Date:  " + sortedMap.lastKey() + " Value: " + sortedMap.lastEntry().getValue());
            System.out.println("   Data Points: " + histVolMap.size());
        } else {
             System.out.println("3. Historical Volatility (30-day): N/A");
        }

        System.out.println("--------------------------------------------------");
        
        if (impVolMap != null && !impVolMap.isEmpty()) {
             ConcurrentSkipListMap<LocalDate, Double> sortedMap = (ConcurrentSkipListMap<LocalDate, Double>) impVolMap;
             System.out.println("4. Implied Volatility (30-day):");
             System.out.println("   First Date: " + sortedMap.firstKey() + " Value: " + sortedMap.firstEntry().getValue());
             System.out.println("   Last Date:  " + sortedMap.lastKey() + " Value: " + sortedMap.lastEntry().getValue());
             System.out.println("   Data Points: " + impVolMap.size());
        } else {
            System.out.println("4. Implied Volatility (30-day): N/A");
        }
        
        System.out.println("--------------------------------------------------");
        System.out.println("5. Underlying Option Chain (Filtered):");
        System.out.println(chainSummary);
        System.out.println("==================================================\n");
    }

    private void printFinalStatisticsTable() {
        System.out.println("\n====================================================================================================");
        System.out.println("FINAL STATISTICS TABLE");
        System.out.println("====================================================================================================");
        System.out.printf("%-10s | %-15s | %-15s | %-15s | %-15s%n", "Ticker", "Diff A (Last-First IV)", "Diff B (Last IV-HV)", "Diff C (Last IV-Ind Avg)", "Sum");
        System.out.println("----------------------------------------------------------------------------------------------------");

        // Calculate Industry Average Implied Volatility
        double totalLastImpVol = 0;
        int count = 0;
        for (String ticker : tickers) {
            Map<LocalDate, Double> impVolMap = optionImpliedVolatility.get(ticker);
            if (impVolMap != null && !impVolMap.isEmpty()) {
                ConcurrentSkipListMap<LocalDate, Double> sortedMap = (ConcurrentSkipListMap<LocalDate, Double>) impVolMap;
                totalLastImpVol += sortedMap.lastEntry().getValue();
                count++;
            }
        }
        double industryAvgImpVol = count > 0 ? totalLastImpVol / count : 0;
        System.out.println("Industry Average Implied Volatility: " + industryAvgImpVol);
        System.out.println("----------------------------------------------------------------------------------------------------");

        for (String ticker : tickers) {
            Map<LocalDate, Double> impVolMap = optionImpliedVolatility.get(ticker);
            Map<LocalDate, Double> histVolMap = historicalVolatility.get(ticker);

            double diffA = 0;
            double diffB = 0;
            double diffC = 0;

            if (impVolMap != null && !impVolMap.isEmpty()) {
                ConcurrentSkipListMap<LocalDate, Double> sortedImpMap = (ConcurrentSkipListMap<LocalDate, Double>) impVolMap;
                double lastImpVol = sortedImpMap.lastEntry().getValue();
                double firstImpVol = sortedImpMap.firstEntry().getValue();
                
                // a) Difference between Last Implied Volatility and First Implied Volatility
                diffA = lastImpVol - firstImpVol;

                // b) Difference between Last Implied Volatility and Last Historical Volatility
                if (histVolMap != null && !histVolMap.isEmpty()) {
                    ConcurrentSkipListMap<LocalDate, Double> sortedHistMap = (ConcurrentSkipListMap<LocalDate, Double>) histVolMap;
                    double lastHistVol = sortedHistMap.lastEntry().getValue();
                    diffB = lastImpVol - lastHistVol;
                }

                // c) Difference between Last Implied Volatility and Industry Average Implied Volatility
                diffC = lastImpVol - industryAvgImpVol;
            }

            double sum = diffA + diffB + diffC;

            System.out.printf("%-10s | %-15.4f | %-15.4f | %-15.4f | %-15.4f%n", ticker, diffA, diffB, diffC, sum);
        }
        System.out.println("====================================================================================================\n");
    }

    private Contract createStockContract(String symbol) {
        Contract contract = new Contract();
        contract.symbol(symbol);
        contract.secType("STK");
        contract.exchange("SMART");
        contract.currency("USD");
        return contract;
    }

    private void fetchSimulatedMarketData() {
        System.out.println("Simulated data not updated for new report format.");
    }

    private double getSimulatedUnderlyingPrice(String ticker) {
        switch (ticker) {
            case "MSFT": return 400.0;
            case "NVDA": return 900.0;
            case "TSLA": return 180.0;
            default: return 100.0;
        }
    }
    
    private List<OptionContract> getSimulatedOptionChain(String ticker) { return new ArrayList<>(); }
    private double getSimulatedOptionPrice(OptionContract option) { return 0.0; }
    private static class OptionContract {
        private final String symbol; private final double strikePrice;
        public OptionContract(String symbol, double strikePrice) { this.symbol = symbol; this.strikePrice = strikePrice; }
        public String getSymbol() { return symbol; }
        public double getStrikePrice() { return strikePrice; }
    }
}
