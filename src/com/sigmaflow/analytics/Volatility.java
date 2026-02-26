package com.sigmaflow.analytics;

import com.ib.client.Bar;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Volatility {

    private static final Logger logger = LogManager.getLogger(Volatility.class);

    public static class GarchResult {
        public final double predictedVolatility;
        public final double lowerConfidenceInterval;
        public final double upperConfidenceInterval;

        public GarchResult(double predictedVolatility, double lowerConfidenceInterval, double upperConfidenceInterval) {
            this.predictedVolatility = predictedVolatility;
            this.lowerConfidenceInterval = lowerConfidenceInterval;
            this.upperConfidenceInterval = upperConfidenceInterval;
        }
    }

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
        List<Double> logReturns = calculateLogReturns(closePrices);

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
     * Estimates GARCH(1,1) parameters and predicts the next period's volatility.
     * Uses a simplified grid search for parameter estimation.
     *
     * @param historicalBars A list of historical price bars.
     * @return A GarchResult object containing the predicted annualized volatility and confidence intervals.
     */
    public GarchResult calculateGarchVolatility(List<Bar> historicalBars) {
        if (historicalBars == null || historicalBars.size() < 2) {
            return new GarchResult(0.0, 0.0, 0.0);
        }

        List<Double> closePrices = historicalBars.stream().map(Bar::close).collect(Collectors.toList());
        List<Double> returns = calculateLogReturns(closePrices);
        
        // Remove mean from returns (assuming 0 mean for simplicity in GARCH often works, or de-mean)
        double meanReturn = returns.stream().mapToDouble(d -> d).average().orElse(0.0);
        List<Double> zeroMeanReturns = returns.stream().map(r -> r - meanReturn).collect(Collectors.toList());

        // Initial variance estimate (sample variance)
        double sampleVariance = zeroMeanReturns.stream().mapToDouble(r -> r * r).average().orElse(0.0);

        // Grid search for best alpha and beta (omega is constrained by long-run variance)
        // variance_long_run = omega / (1 - alpha - beta) => omega = variance_long_run * (1 - alpha - beta)
        // We assume long-run variance is the sample variance.
        
        double bestAlpha = 0.05;
        double bestBeta = 0.90;
        double bestOmega = sampleVariance * (1 - bestAlpha - bestBeta);
        double maxLikelihood = -Double.MAX_VALUE;

        // Coarse grid search
        for (double alpha = 0.01; alpha < 0.3; alpha += 0.02) {
            for (double beta = 0.5; beta < 0.99; beta += 0.02) {
                if (alpha + beta >= 0.999) continue; // Stationarity constraint

                double omega = sampleVariance * (1 - alpha - beta);
                double likelihood = calculateLogLikelihood(zeroMeanReturns, omega, alpha, beta, sampleVariance);

                if (likelihood > maxLikelihood) {
                    maxLikelihood = likelihood;
                    bestAlpha = alpha;
                    bestBeta = beta;
                    bestOmega = omega;
                }
            }
        }

        // Calculate last variance using best parameters
        double currentVariance = sampleVariance;
        for (Double r : zeroMeanReturns) {
            currentVariance = bestOmega + bestAlpha * (r * r) + bestBeta * currentVariance;
        }

        // Forecast next variance: sigma^2_t+1 = omega + alpha * r_t^2 + beta * sigma^2_t
        // Here currentVariance is already sigma^2_t+1 based on the loop above ending at T
        
        double dailyVolatility = Math.sqrt(currentVariance);
        double annualizedVolatility = dailyVolatility * Math.sqrt(252);

        // Confidence Interval (95%)
        List<Double> garchVols = new ArrayList<>();
        double h = sampleVariance;
        for (Double r : zeroMeanReturns) {
            h = bestOmega + bestAlpha * (r * r) + bestBeta * h;
            garchVols.add(Math.sqrt(h) * Math.sqrt(252));
        }
        
        garchVols.sort(Double::compareTo);
        double lowerCI = garchVols.get((int)(garchVols.size() * 0.05));
        double upperCI = garchVols.get((int)(garchVols.size() * 0.95));

        return new GarchResult(annualizedVolatility, lowerCI, upperCI);
    }

    private double calculateLogLikelihood(List<Double> returns, double omega, double alpha, double beta, double initialVariance) {
        double logLikelihood = 0.0;
        double currentVariance = initialVariance;

        for (Double r : returns) {
            double sigma2 = currentVariance;
            logLikelihood += -0.5 * (Math.log(sigma2) + (r * r) / sigma2);
            currentVariance = omega + alpha * (r * r) + beta * currentVariance;
        }
        return logLikelihood;
    }

    public List<Double> calculateLogReturns(List<Double> prices) {
        List<Double> returns = new ArrayList<>();
        for (int i = 1; i < prices.size(); i++) {
            returns.add(Math.log(prices.get(i) / prices.get(i - 1)));
        }
        return returns;
    }

    /**
     * Calculates the Value at Risk (VaR) for a single position using the Parametric method.
     *
     * @param positionValue The current value of the position.
     * @param volatility    The annualized volatility of the asset.
     * @param confidenceLevel The confidence level (e.g., 0.95, 0.99).
     * @return The 1-day VaR.
     */
    public double calculateParametricVaR(double positionValue, double volatility, double confidenceLevel) {
        double dailyVolatility = volatility / Math.sqrt(252);
        double zScore = getZScore(confidenceLevel);
        return Math.abs(positionValue * dailyVolatility * zScore);
    }

    /**
     * Calculates the Portfolio VaR considering covariance between assets.
     *
     * @param positionValues List of values for each position.
     * @param returnsMatrix  List of return series for each position (must be aligned by date).
     * @param confidenceLevel The confidence level.
     * @return The 1-day Portfolio VaR.
     */
    public double calculatePortfolioVaR(List<Double> positionValues, List<List<Double>> returnsMatrix, double confidenceLevel) {
        if (positionValues.isEmpty() || returnsMatrix.isEmpty() || positionValues.size() != returnsMatrix.size()) {
            return 0.0;
        }

        int n = positionValues.size();
        double[][] covarianceMatrix = calculateCovarianceMatrix(returnsMatrix);
        double totalPortfolioValue = positionValues.stream().mapToDouble(Double::doubleValue).sum();
        
        // Weights vector
        double[] weights = new double[n];
        for (int i = 0; i < n; i++) {
            weights[i] = positionValues.get(i) / totalPortfolioValue;
        }

        // Portfolio Variance = w' * Sigma * w
        double portfolioVariance = 0.0;
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < n; j++) {
                portfolioVariance += weights[i] * weights[j] * covarianceMatrix[i][j];
            }
        }

        double portfolioDailyVolatility = Math.sqrt(portfolioVariance);
        double zScore = getZScore(confidenceLevel);

        return Math.abs(totalPortfolioValue * portfolioDailyVolatility * zScore);
    }

    private double[][] calculateCovarianceMatrix(List<List<Double>> returnsMatrix) {
        int n = returnsMatrix.size();
        int m = returnsMatrix.get(0).size(); // Number of observations
        double[][] covarianceMatrix = new double[n][n];

        // Calculate means
        double[] means = new double[n];
        for (int i = 0; i < n; i++) {
            means[i] = returnsMatrix.get(i).stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
        }

        for (int i = 0; i < n; i++) {
            for (int j = 0; j < n; j++) {
                double sum = 0.0;
                for (int k = 0; k < m; k++) {
                    sum += (returnsMatrix.get(i).get(k) - means[i]) * (returnsMatrix.get(j).get(k) - means[j]);
                }
                covarianceMatrix[i][j] = sum / (m - 1);
            }
        }
        return covarianceMatrix;
    }

    private double getZScore(double confidenceLevel) {
        // Approximation for standard normal inverse cumulative distribution
        // 90% -> 1.282, 95% -> 1.645, 99% -> 2.326
        if (confidenceLevel >= 0.99) return 2.326;
        if (confidenceLevel >= 0.95) return 1.645;
        if (confidenceLevel >= 0.90) return 1.282;
        return 0.0;
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
