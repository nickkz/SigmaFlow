package com.sigmaflow.data;

import com.sigmaflow.api.EWrapperImpl;
import com.ib.client.Bar;
import com.ib.client.Contract;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
    private final Map<String, List<Bar>> historicalBars = new ConcurrentHashMap<>();
    private final Map<String, Contract> atmOptionContracts = new ConcurrentHashMap<>();
    private final Map<String, Double> atmOptionPrices = new ConcurrentHashMap<>();
    private final Map<String, Double> historicalVolatility = new ConcurrentHashMap<>();
    private final Map<String, Double> optionImpliedVolatility = new ConcurrentHashMap<>();


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
        api.getClient().reqHistoricalData(reqId, contract, endDateTime, "1 Y", "1 day", "TRADES", 1, 1, false, null);
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

    public void requestOptionImpliedVolatility(String ticker, Contract optionContract) {
        int reqId = nextReqId.getAndIncrement();
        reqIdToTickerMap.put(reqId, ticker);
        reqIdToRequestType.put(reqId, RequestType.OPTION_IMPLIED_VOLATILITY);

        String endDateTime = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")) + " 16:00:00";
        api.getClient().reqHistoricalData(reqId, optionContract, endDateTime, "30 D", "1 day", "OPTION_IMPLIED_VOLATILITY", 1, 1, false, null);
    }

    public void setUnderlyingPrice(int reqId, double price) {
        String ticker = reqIdToTickerMap.get(reqId);
        if (ticker != null) {
            underlyingPrices.put(ticker, price);
            logger.info("Updated underlying price for " + ticker + " to " + price);
            
            Integer conId = tickerToConIdMap.get(ticker);
            if (conId != null) {
                // Request option chain parameters
                int optionReqId = nextReqId.getAndIncrement();
                reqIdToTickerMap.put(optionReqId, ticker);
                reqIdToRequestType.put(optionReqId, RequestType.OPTION_CHAIN_PARAMS);
                api.getClient().reqSecDefOptParams(optionReqId, ticker, "", "STK", conId);

                // Request historical data
                requestHistoricalData(ticker, conId);
                requestHistoricalVolatility(ticker, conId);
            }
            api.getClient().cancelMktData(reqId); // Cancel market data for underlying
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
            logger.info("Finished receiving historical data for " + ticker);
            reqIdToTickerMap.remove(reqId);
            reqIdToRequestType.remove(reqId);
        }
    }

    public void processOptionChainParameters(int reqId, Set<String> expirations, Set<Double> strikes) {
        String ticker = reqIdToTickerMap.get(reqId);
        Double underlyingPrice = underlyingPrices.get(ticker);

        if (ticker == null || underlyingPrice == null) {
            logger.error("Could not process option chain for reqId " + reqId + ". Ticker or price not found.");
            return;
        }

        // Filter expirations to within 3 months and find the next monthly expiration
        LocalDate today = LocalDate.now();
        LocalDate threeMonthsFromNow = today.plusMonths(3);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");

        String nextMonthlyExpiration = expirations.stream()
                .map(exp -> LocalDate.parse(exp, formatter))
                .filter(expDate -> !expDate.isBefore(today) && !expDate.isAfter(threeMonthsFromNow))
                .filter(expDate -> expDate.getDayOfMonth() >= 15 && expDate.getDayOfMonth() <= 21) // Third Friday of the month
                .min(Comparator.comparingLong(expDate -> ChronoUnit.DAYS.between(today, expDate)))
                .map(expDate -> expDate.format(formatter))
                .orElse(null);

        // Find the at-the-money (ATM) strike
        Double atmStrike = strikes.stream()
                .filter(strike -> strike >= underlyingPrice * 0.8 && strike <= underlyingPrice * 1.2)
                .min(Comparator.comparingDouble(strike -> Math.abs(strike - underlyingPrice)))
                .orElse(null);

        logger.info("Filtered Option Chain for " + ticker);
        logger.info("Next Monthly Expiration: " + nextMonthlyExpiration);
        logger.info("ATM Strike: " + atmStrike);

        if (nextMonthlyExpiration != null && atmStrike != null) {
            // Request market data for the ATM Call option
            Contract atmCallOption = createOptionContract(ticker, nextMonthlyExpiration, atmStrike, "C");
            int optionMktDataReqId = nextReqId.getAndIncrement();
            reqIdToTickerMap.put(optionMktDataReqId, ticker);
            reqIdToRequestType.put(optionMktDataReqId, RequestType.OPTION_MARKET_DATA);
            atmOptionContracts.put(ticker, atmCallOption); // Store the ATM option contract
            api.getClient().reqMktData(optionMktDataReqId, atmCallOption, "", true, false, null);

            // Request implied volatility for the ATM Call option
            requestOptionImpliedVolatility(ticker, atmCallOption);
        }
        reqIdToTickerMap.remove(reqId);
        reqIdToRequestType.remove(reqId);
    }

    public void setAtmOptionPrice(int reqId, double price) {
        String ticker = reqIdToTickerMap.get(reqId);
        if (ticker != null) {
            atmOptionPrices.put(ticker, price);
            logger.info("Updated ATM option price for " + ticker + " to " + price);
            api.getClient().cancelMktData(reqId); // Cancel market data for option
            reqIdToTickerMap.remove(reqId);
            reqIdToRequestType.remove(reqId);
        }
    }

    public void setHistoricalVolatility(int reqId, double volatility) {
        String ticker = reqIdToTickerMap.get(reqId);
        if (ticker != null) {
            historicalVolatility.put(ticker, volatility);
            logger.info("Updated historical volatility for " + ticker + " to " + volatility);
        }
    }

    public void setOptionImpliedVolatility(int reqId, double volatility) {
        String ticker = reqIdToTickerMap.get(reqId);
        if (ticker != null) {
            optionImpliedVolatility.put(ticker, volatility);
            logger.info("Updated option implied volatility for " + ticker + " to " + volatility);
        }
    }
    
    public RequestType getRequestType(int reqId) {
        return reqIdToRequestType.get(reqId);
    }

    private Contract createStockContract(String symbol) {
        Contract contract = new Contract();
        contract.symbol(symbol);
        contract.secType("STK");
        contract.exchange("SMART");
        contract.currency("USD");
        return contract;
    }

    private Contract createOptionContract(String symbol, String lastTradeDateOrContractMonth, double strike, String right) {
        Contract contract = new Contract();
        contract.symbol(symbol);
        contract.secType("OPT");
        contract.exchange("SMART");
        contract.currency("USD");
        contract.lastTradeDateOrContractMonth(lastTradeDateOrContractMonth);
        contract.strike(strike);
        contract.right(right);
        contract.multiplier("100"); // Standard multiplier for equity options
        return contract;
    }

    private void fetchSimulatedMarketData() {
        for (String ticker : tickers) {
            double underlyingPrice = getSimulatedUnderlyingPrice(ticker);
            logger.info("Market price for " + ticker + ": " + underlyingPrice);

            List<OptionContract> optionChain = getSimulatedOptionChain(ticker);

            double lowerBound = underlyingPrice * 0.8;
            double upperBound = underlyingPrice * 1.2;
            List<OptionContract> filteredOptions = optionChain.stream()
                    .filter(option -> option.getStrikePrice() >= lowerBound && option.getStrikePrice() <= upperBound)
                    .collect(Collectors.toList());

            logger.info("Option prices for " + ticker + ":");
            for (OptionContract option : filteredOptions) {
                double optionPrice = getSimulatedOptionPrice(option);
                logger.info("  " + option.getSymbol() + " (Strike: " + option.getStrikePrice() + "): " + optionPrice);
            }
        }
    }

    private double getSimulatedUnderlyingPrice(String ticker) {
        // Simulate fetching the underlying price
        switch (ticker) {
            case "MSFT":
                return 400.0;
            case "NVDA":
                return 900.0;
            case "TSLA":
                return 180.0;
            default:
                return 100.0;
        }
    }

    private List<OptionContract> getSimulatedOptionChain(String ticker) {
        // Simulate fetching the option chain
        List<OptionContract> optionChain = new ArrayList<>();
        double baseStrike = getSimulatedUnderlyingPrice(ticker);
        for (int i = -5; i <= 5; i++) {
            double strike = baseStrike + i * 5;
            optionChain.add(new OptionContract(ticker + "_C_" + strike, strike));
            optionChain.add(new OptionContract(ticker + "_P_" + strike, strike));
        }
        return optionChain;
    }

    private double getSimulatedOptionPrice(OptionContract option) {
        // Simulate fetching the option price
        return Math.random() * 10; // Random price for demonstration
    }

    // A simple class to represent an option contract
    private static class OptionContract {
        private final String symbol;
        private final double strikePrice;

        public OptionContract(String symbol, double strikePrice) {
            this.symbol = symbol;
            this.strikePrice = strikePrice;
        }

        public String getSymbol() {
            return symbol;
        }

        public double getStrikePrice() {
            return strikePrice;
        }
    }
}
