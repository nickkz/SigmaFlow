package com.sigmaflow.analytics;

import com.ib.client.Bar;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.stream.Collectors;

public class Volatility {

    private static final Logger logger = LogManager.getLogger(Volatility.class);

    /**
     * Calculates the historical volatility from a list of historical bars.
     * Historical volatility is typically calculated as the standard deviation of log returns.
     *
     * @param historicalBars A list of historical price bars.
     * @return The annualized historical volatility.
     */
    public double calculateHistoricalVolatility(List<Bar> historicalBars) {
        if (historicalBars == null || historicalBars.size() < 2) {
            return 0.0; // Not enough data to calculate volatility
        }

        // Extract close prices
        List<Double> closePrices = historicalBars.stream()
                .map(Bar::close)
                .collect(Collectors.toList());

        // Calculate log returns
        List<Double> logReturns = new java.util.ArrayList<>();
        for (int i = 1; i < closePrices.size(); i++) {
            double logReturn = Math.log(closePrices.get(i) / closePrices.get(i - 1));
            logReturns.add(logReturn);
        }

        // Calculate mean of log returns
        double sumLogReturns = logReturns.stream().mapToDouble(Double::doubleValue).sum();
        double meanLogReturns = sumLogReturns / logReturns.size();

        // Calculate sum of squared differences from the mean
        double sumOfSquaredDifferences = logReturns.stream()
                .mapToDouble(logReturn -> Math.pow(logReturn - meanLogReturns, 2))
                .sum();

        // Calculate variance and standard deviation (daily volatility)
        double variance = sumOfSquaredDifferences / (logReturns.size() - 1);
        double dailyVolatility = Math.sqrt(variance);

        // Annualize the volatility (assuming 252 trading days in a year)
        double annualizedVolatility = dailyVolatility * Math.sqrt(252);

        return annualizedVolatility;
    }

    /**
     * Placeholder for calculating implied volatility using the Black-Scholes model.
     * This is a complex calculation often requiring an iterative solver.
     *
     * @param S The current price of the underlying asset.
     * @param K The strike price of the option.
     * @param T The time to expiration (in years).
     * @param r The risk-free interest rate (annualized).
     * @param optionPrice The market price of the option.
     * @param optionType The type of option ("call" or "put").
     * @return The implied volatility.
     */
    public double calculateImpliedVolatility(double S, double K, double T, double r, double optionPrice, String optionType) {
        // This is a placeholder. A full implementation would involve an iterative solver
        // (e.g., Newton-Raphson) to find the volatility that makes the Black-Scholes
        // price equal to the market price.
        logger.info("Calculating Implied Volatility (placeholder)...");
        logger.info(String.format("S: %f, K: %f, T: %f, r: %f, Option Price: %f, Type: %s", S, K, T, r, optionPrice, optionType));
        return 0.0; // Return 0.0 for now
    }

    /**
     * Calculates the theoretical price of a European call or put option using the Black-Scholes formula.
     *
     * @param S     Current price of the underlying asset
     * @param K     Strike price
     * @param T     Time to expiration in years
     * @param r     Risk-free interest rate (annualized)
     * @param sigma Volatility (annualized)
     * @param type  Option type ("C" for Call, "P" for Put)
     * @return The theoretical option price
     */
    public double calculateOptionPrice(double S, double K, double T, double r, double sigma, String type) {
        double d1 = (Math.log(S / K) + (r + 0.5 * Math.pow(sigma, 2)) * T) / (sigma * Math.sqrt(T));
        double d2 = d1 - sigma * Math.sqrt(T);

        if ("C".equalsIgnoreCase(type)) {
            return S * cumulativeDistribution(d1) - K * Math.exp(-r * T) * cumulativeDistribution(d2);
        } else {
            return K * Math.exp(-r * T) * cumulativeDistribution(-d2) - S * cumulativeDistribution(-d1);
        }
    }

    private double cumulativeDistribution(double x) {
        // Approximation of the cumulative distribution function for the standard normal distribution
        double b1 = 0.319381530;
        double b2 = -0.356563782;
        double b3 = 1.781477937;
        double b4 = -1.821255978;
        double b5 = 1.330274429;
        double p = 0.2316419;
        double c = 1.0 / Math.sqrt(2 * Math.PI);

        if (x >= 0.0) {
            double t = 1.0 / (1.0 + p * x);
            return 1.0 - c * Math.exp(-x * x / 2.0) * t * (t * (t * (t * (t * b5 + b4) + b3) + b2) + b1);
        } else {
            double t = 1.0 / (1.0 - p * x);
            return c * Math.exp(-x * x / 2.0) * t * (t * (t * (t * (t * b5 + b4) + b3) + b2) + b1);
        }
    }
}
