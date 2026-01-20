package com.sigmaflow;

import com.sigmaflow.api.EWrapperImpl;
import com.sigmaflow.data.MarketData;
import com.sigmaflow.analytics.Volatility;
import com.sigmaflow.strategy.VolatilityArbitrage;
import com.sigmaflow.trading.OrderManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class Main {

    private static final Logger logger = LogManager.getLogger(Main.class);

    public static void main(String[] args) {
        // --- Configuration ---
        // To run with live data and tickers from file: java com.sigmaflow.Main live data/finviz.csv
        // To run with live data and specific tickers: java com.sigmaflow.Main live MSFT NVDA
        // To run with simulated data: java com.sigmaflow.Main simulated TSLA
        // Default is simulated with MSFT, NVDA, TSLA

        logger.info("Start Program...");

        MarketData.DataSource dataSource = MarketData.DataSource.SIMULATED;
        String[] tickers = {"MSFT", "NVDA", "TSLA"};

        if (args.length > 0) {
            if (args[0].equalsIgnoreCase("live")) {
                dataSource = MarketData.DataSource.LIVE;
            }
            
            if (args.length > 1) {
                String arg1 = args[1];
                if (arg1.endsWith(".csv")) {
                    // Assume it's a file path
                    tickers = readTickersFromCsv(arg1);
                } else {
                    // Assume it's a list of tickers
                    tickers = Arrays.copyOfRange(args, 1, args.length);
                }
            }
        }

        if (tickers.length == 0) {
            logger.error("No tickers found. Exiting.");
            return;
        }
        
        logger.info("Tickers: " + Arrays.toString(tickers));

        // 1. Initialize the components
        EWrapperImpl api = new EWrapperImpl();
        MarketData marketData = new MarketData(dataSource, tickers, api);
        Volatility volatility = new Volatility();
        OrderManager orderManager = new OrderManager();
        VolatilityArbitrage strategy = new VolatilityArbitrage();

        // 2. Connect to the Interactive Brokers API if needed
        if (dataSource == MarketData.DataSource.LIVE) {
            api.connect("127.0.0.1", 7496, 0); // Use 7496 for TWS, 7497 for Paper, 4002 for IB Gateway
            // Wait for the connection to be established
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                logger.error("Error during sleep", e);
                Thread.currentThread().interrupt();
                return;
            }
        }

        // 3. Request and display market data
        marketData.fetchMarketData();

        // In a real-time application, you'd keep the application running to receive data.
        if (dataSource == MarketData.DataSource.LIVE) {
            logger.info("Waiting for real-time data. Press [ENTER] to exit.");
            // Keep the main thread alive to receive callbacks
            Scanner scanner = new Scanner(System.in);
            scanner.nextLine();
            
            logger.info("Disconnecting...");
            api.disconnect();
        }

        // 4. Perform volatility calculations (to be implemented)
        // 5. Apply the trading strategy (to be implemented)
        // 6. Manage orders (to be implemented)

        logger.info("Volatility Arbitrage Trading Application shutting down.");
    }

    private static String[] readTickersFromCsv(String filePath) {
        List<String> tickers = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            // Skip header
            br.readLine();
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                if (values.length > 0) {
                    // Assuming Ticker is the first column
                    String ticker = values[0].trim().replace("\"", "");
                    if (!ticker.isEmpty()) {
                        tickers.add(ticker);
                    }
                }
            }
        } catch (IOException e) {
            logger.error("Error reading CSV file: " + filePath, e);
        }
        return tickers.toArray(new String[0]);
    }
}
