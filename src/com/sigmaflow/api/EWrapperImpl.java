package com.sigmaflow.api;

import com.ib.client.protobuf.*;
import com.sigmaflow.data.MarketData;
import com.ib.client.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class EWrapperImpl implements EWrapper {

    private static final Logger logger = LogManager.getLogger(EWrapperImpl.class);

    private final EClientSocket client;
    private final EReaderSignal readerSignal;
    private MarketData marketData; // Reference to the MarketData instance

    public EWrapperImpl() {
        this.readerSignal = new EJavaSignal();
        this.client = new EClientSocket(this, readerSignal);
    }

    public void setMarketData(MarketData marketData) {
        this.marketData = marketData;
    }

    public void connect(String host, int port, int clientId) {
        logger.info("Connecting to TWS...");
        client.eConnect(host, port, clientId);
        final EReader reader = new EReader(client, readerSignal);
        reader.start();
        new Thread(() -> {
            while (client.isConnected()) {
                readerSignal.waitForSignal();
                try {
                    reader.processMsgs();
                } catch (Exception e) {
                    logger.error("Exception in EReader", e);
                }
            }
        }).start();
    }

    public void disconnect() {
        logger.info("Disconnecting from TWS...");
        client.eDisconnect();
    }

    public EClientSocket getClient() {
        return client;
    }

    // --- Connection and Error Handling ---

    @Override
    public void connectAck() {
        logger.info("API connection acknowledged.");
    }

    @Override
    public void connectionClosed() {
        logger.info("API connection closed.");
    }

    @Override
    public void error(Exception e) {
        logger.error("Error: ", e);
    }

    @Override
    public void error(String str) {
        logger.error("Error: " + str);
    }

    @Override
    public void error(int i, long l, int i1, String s, String s1) {

    }

    public void error(int id, int errorCode, String errorMsg, String advancedOrderRejectJson) {
        String error = "API Error. Id: " + id + ", Code: " + errorCode + ", Msg: " + errorMsg;
        if (advancedOrderRejectJson != null && !advancedOrderRejectJson.isEmpty()) {
            error += ", AdvancedJson: " + advancedOrderRejectJson;
        }
        logger.error(error);
    }

    // --- Market Data and Contract Details ---

    @Override
    public void nextValidId(int orderId) {
        logger.info("Connection successful. Next valid order ID: " + orderId);
    }

    @Override
    public void contractDetails(int reqId, ContractDetails contractDetails) {
        logger.info("Contract Details Received for ReqId: " + reqId);
        String ticker = contractDetails.contract().symbol();
        int conId = contractDetails.contract().conid();
        logger.info(ticker + " ConId: " + conId);
        
        if (marketData != null) {
            marketData.handleContractDetails(ticker, contractDetails);
        }
    }

    @Override
    public void contractDetailsEnd(int reqId) {
        logger.info("Finished receiving contract details for ReqId: " + reqId);
    }

    @Override
    public void securityDefinitionOptionalParameter(int reqId, String exchange, int underlyingConId, String tradingClass, String multiplier, Set<String> expirations, Set<Double> strikes) {
        logger.debug("Received Option Chain Parameters for ReqId: {}", reqId);
        if (marketData != null) {
            marketData.processOptionChainParameters(reqId, expirations, strikes);
        }
    }

    @Override
    public void securityDefinitionOptionalParameterEnd(int reqId) {
        logger.info("Finished receiving option chain parameters for ReqId: {}", reqId);
    }

    @Override
    public void realtimeBar(int reqId, long time, double open, double high, double low, double close, Decimal volume, Decimal wap, int count) {
         logger.info(String.format("Real-time bar. ReqId: %d, Time: %d, O: %f, H: %f, L: %f, C: %f, Vol: %s",
                reqId, time, open, high, low, close, volume));
    }

    @Override
    public void tickPrice(int tickerId, int field, double price, TickAttrib attrib) {
        logger.debug(String.format("Tick Price. Ticker Id: %d, Field: %s, Price: %f", tickerId, TickType.getField(field), price));
        if (marketData != null) {
            MarketData.RequestType requestType = marketData.getRequestType(tickerId);
            if (requestType == MarketData.RequestType.UNDERLYING_MARKET_DATA) {
                if (field == TickType.LAST.ordinal() || field == TickType.CLOSE.ordinal()) {
                    marketData.setUnderlyingPrice(tickerId, price);
                }
            }
        }
    }

    @Override
    public void tickSize(int tickerId, int field, Decimal size) {
        logger.debug(String.format("Tick Size. Ticker Id: %d, Field: %s, Size: %s", tickerId, TickType.getField(field), size));
    }

    @Override
    public void tickString(int tickerId, int tickType, String value) {
        logger.info(String.format("Tick String. Ticker Id: %d, Type: %s, Value: %s", tickerId, TickType.getField(tickType), value));
    }

    @Override
    public void historicalData(int reqId, Bar bar) {
        if (marketData != null) {
            MarketData.RequestType requestType = marketData.getRequestType(reqId);
            if (requestType == MarketData.RequestType.HISTORICAL_DATA) {
                marketData.addHistoricalBar(reqId, bar);
            } else if (requestType == MarketData.RequestType.HISTORICAL_VOLATILITY) {
                marketData.setHistoricalVolatility(reqId, bar.time(), bar.close());
            } else if (requestType == MarketData.RequestType.OPTION_IMPLIED_VOLATILITY) {
                marketData.setOptionImpliedVolatility(reqId, bar.time(), bar.close());
            }
        }
    }

    @Override
    public void historicalDataEnd(int reqId, String startDateStr, String endDateStr) {
        if (marketData != null) {
            marketData.historicalDataEnd(reqId, startDateStr, endDateStr);
        }
    }

    // --- Empty Implementations for the rest of EWrapper ---

    @Override
    public void orderStatus(int orderId, String status, Decimal filled, Decimal remaining, double avgFillPrice, long permId, int parentId, double lastFillPrice, int clientId, String whyHeld, double mktCapa) {}

    @Override
    public void tickOptionComputation(int tickerId, int field, int tickAttrib, double impliedVol, double delta, double optPrice, double pvDividend, double gamma, double vega, double theta, double undPrice) {}

    @Override
    public void tickGeneric(int tickerId, int tickType, double value) {}

    @Override
    public void tickEFP(int tickerId, int tickType, double basisPoints, String formattedBasisPoints, double impliedFuture, int holdDays, String futureLastTradeDate, double dividendImpact, double dividendsToLastTradeDate) {}

    @Override
    public void openOrder(int orderId, Contract contract, Order order, OrderState orderState) {}

    @Override
    public void openOrderEnd() {}

    @Override
    public void updateAccountValue(String key, String value, String currency, String accountName) {}

    @Override
    public void updatePortfolio(Contract contract, Decimal position, double marketPrice, double marketValue, double averageCost, double unrealizedPNL, double realizedPNL, String accountName) {}

    @Override
    public void updateAccountTime(String timeStamp) {}

    @Override
    public void accountDownloadEnd(String accountName) {}

    @Override
    public void bondContractDetails(int reqId, ContractDetails contractDetails) {}

    @Override
    public void execDetails(int reqId, Contract contract, Execution execution) {}

    @Override
    public void execDetailsEnd(int reqId) {}

    @Override
    public void updateMktDepth(int tickerId, int position, int operation, int side, double price, Decimal size) {}

    @Override
    public void updateMktDepthL2(int tickerId, int position, String marketMaker, int operation, int side, double price, Decimal size, boolean isSmartDepth) {}

    @Override
    public void updateNewsBulletin(int msgId, int msgType, String message, String origExchange) {}

    @Override
    public void managedAccounts(String accountsList) {}

    @Override
    public void receiveFA(int faDataType, String xml) {}

    @Override
    public void scannerParameters(String xml) {}

    @Override
    public void scannerData(int reqId, int rank, ContractDetails contractDetails, String distance, String benchmark, String projection, String legsStr) {}

    @Override
    public void scannerDataEnd(int reqId) {}

    @Override
    public void currentTime(long time) {}

    @Override
    public void fundamentalData(int reqId, String data) {}

    @Override
    public void deltaNeutralValidation(int reqId, DeltaNeutralContract deltaNeutralContract) {}

    @Override
    public void tickSnapshotEnd(int reqId) {}

    @Override
    public void marketDataType(int reqId, int marketDataType) {}

    @Override
    public void position(String account, Contract contract, Decimal pos, double avgCost) {}

    @Override
    public void positionEnd() {}

    @Override
    public void accountSummary(int reqId, String account, String tag, String value, String currency) {}

    @Override
    public void accountSummaryEnd(int reqId) {}

    @Override
    public void verifyMessageAPI(String apiData) {}

    @Override
    public void verifyCompleted(boolean isSuccessful, String errorText) {}

    @Override
    public void verifyAndAuthMessageAPI(String apiData, String xyzChallenge) {}

    @Override
    public void verifyAndAuthCompleted(boolean isSuccessful, String errorText) {}

    @Override
    public void displayGroupList(int reqId, String groups) {}

    @Override
    public void displayGroupUpdated(int reqId, String contractInfo) {}

    @Override
    public void positionMulti(int reqId, String account, String modelCode, Contract contract, Decimal pos, double avgCost) {}

    @Override
    public void positionMultiEnd(int reqId) {}

    @Override
    public void accountUpdateMulti(int reqId, String account, String modelCode, String key, String value, String currency) {}

    @Override
    public void accountUpdateMultiEnd(int reqId) {}

    @Override
    public void softDollarTiers(int reqId, SoftDollarTier[] tiers) {}

    @Override
    public void familyCodes(FamilyCode[] familyCodes) {}

    @Override
    public void symbolSamples(int reqId, ContractDescription[] contractDescriptions) {}

    @Override
    public void mktDepthExchanges(DepthMktDataDescription[] depthMktDataDescriptions) {}

    @Override
    public void tickNews(int tickerId, long timeStamp, String providerCode, String articleId, String headline, String extraData) {}

    @Override
    public void smartComponents(int reqId, Map<Integer, Map.Entry<String, Character>> theMap) {}

    @Override
    public void tickReqParams(int tickerId, double minTick, String bboExchange, int snapshotPermissions) {}

    @Override
    public void newsProviders(NewsProvider[] newsProviders) {}

    @Override
    public void newsArticle(int requestId, int articleType, String articleText) {}

    @Override
    public void historicalNews(int requestId, String time, String providerCode, String articleId, String headline) {}

    @Override
    public void historicalNewsEnd(int requestId, boolean hasMore) {}

    @Override
    public void headTimestamp(int reqId, String headTimestamp) {}

    @Override
    public void histogramData(int reqId, List<HistogramEntry> items) {}

    @Override
    public void historicalDataUpdate(int reqId, Bar bar) {}

    @Override
    public void rerouteMktDataReq(int reqId, int conId, String exchange) {}

    @Override
    public void rerouteMktDepthReq(int reqId, int conId, String exchange) {}

    @Override
    public void marketRule(int marketRuleId, PriceIncrement[] priceIncrements) {}

    @Override
    public void pnl(int reqId, double dailyPnL, double unrealizedPnL, double realizedPnL) {}

    @Override
    public void pnlSingle(int reqId, Decimal pos, double dailyPnL, double unrealizedPnL, double realizedPnL, double value) {}

    @Override
    public void historicalTicks(int reqId, List<HistoricalTick> ticks, boolean done) {}

    @Override
    public void historicalTicksBidAsk(int reqId, List<HistoricalTickBidAsk> ticks, boolean done) {}

    @Override
    public void historicalTicksLast(int reqId, List<HistoricalTickLast> ticks, boolean done) {}

    @Override
    public void tickByTickAllLast(int reqId, int tickType, long time, double price, Decimal size, TickAttribLast tickAttribLast, String exchange, String specialConditions) {}

    @Override
    public void tickByTickBidAsk(int reqId, long time, double bidPrice, double askPrice, Decimal bidSize, Decimal askSize, TickAttribBidAsk tickAttribBidAsk) {}

    @Override
    public void tickByTickMidPoint(int reqId, long time, double midPoint) {}

    @Override
    public void orderBound(long orderId, int apiClientId, int apiOrderId) {}

    @Override
    public void completedOrder(Contract contract, Order order, OrderState orderState) {}

    @Override
    public void completedOrdersEnd() {}

    @Override
    public void replaceFAEnd(int reqId, String text) {}

    @Override
    public void wshMetaData(int reqId, String dataJson) {}

    @Override
    public void wshEventData(int reqId, String dataJson) {}

    @Override
    public void historicalSchedule(int reqId, String startDateTime, String endDateTime, String timeZone, List<HistoricalSession> sessions) {}

    @Override
    public void userInfo(int reqId, String whiteBrandingId) {}

    @Override
    public void currentTimeInMillis(long l) {

    }

    @Override
    public void orderStatusProtoBuf(OrderStatusProto.OrderStatus orderStatus) {

    }

    @Override
    public void openOrderProtoBuf(OpenOrderProto.OpenOrder openOrder) {

    }

    @Override
    public void openOrdersEndProtoBuf(OpenOrdersEndProto.OpenOrdersEnd openOrdersEnd) {

    }

    @Override
    public void errorProtoBuf(ErrorMessageProto.ErrorMessage errorMessage) {

    }

    @Override
    public void execDetailsProtoBuf(ExecutionDetailsProto.ExecutionDetails executionDetails) {

    }

    @Override
    public void execDetailsEndProtoBuf(ExecutionDetailsEndProto.ExecutionDetailsEnd executionDetailsEnd) {

    }

    @Override
    public void completedOrderProtoBuf(CompletedOrderProto.CompletedOrder completedOrder) {

    }

    @Override
    public void completedOrdersEndProtoBuf(CompletedOrdersEndProto.CompletedOrdersEnd completedOrdersEnd) {

    }

    @Override
    public void orderBoundProtoBuf(OrderBoundProto.OrderBound orderBound) {

    }

    @Override
    public void contractDataProtoBuf(ContractDataProto.ContractData contractData) {

    }

    @Override
    public void bondContractDataProtoBuf(ContractDataProto.ContractData contractData) {

    }

    @Override
    public void contractDataEndProtoBuf(ContractDataEndProto.ContractDataEnd contractDataEnd) {

    }

    @Override
    public void tickPriceProtoBuf(TickPriceProto.TickPrice tickPrice) {

    }

    @Override
    public void tickSizeProtoBuf(TickSizeProto.TickSize tickSize) {

    }

    @Override
    public void tickOptionComputationProtoBuf(TickOptionComputationProto.TickOptionComputation tickOptionComputation) {

    }

    @Override
    public void tickGenericProtoBuf(TickGenericProto.TickGeneric tickGeneric) {

    }

    @Override
    public void tickStringProtoBuf(TickStringProto.TickString tickString) {

    }

    @Override
    public void tickSnapshotEndProtoBuf(TickSnapshotEndProto.TickSnapshotEnd tickSnapshotEnd) {

    }

    @Override
    public void updateMarketDepthProtoBuf(MarketDepthProto.MarketDepth marketDepth) {

    }

    @Override
    public void updateMarketDepthL2ProtoBuf(MarketDepthL2Proto.MarketDepthL2 marketDepthL2) {

    }

    @Override
    public void marketDataTypeProtoBuf(MarketDataTypeProto.MarketDataType marketDataType) {

    }

    @Override
    public void tickReqParamsProtoBuf(TickReqParamsProto.TickReqParams tickReqParams) {

    }

    @Override
    public void updateAccountValueProtoBuf(AccountValueProto.AccountValue accountValue) {

    }

    @Override
    public void updatePortfolioProtoBuf(PortfolioValueProto.PortfolioValue portfolioValue) {

    }

    @Override
    public void updateAccountTimeProtoBuf(AccountUpdateTimeProto.AccountUpdateTime accountUpdateTime) {

    }

    @Override
    public void accountDataEndProtoBuf(AccountDataEndProto.AccountDataEnd accountDataEnd) {

    }

    @Override
    public void managedAccountsProtoBuf(ManagedAccountsProto.ManagedAccounts managedAccounts) {

    }

    @Override
    public void positionProtoBuf(PositionProto.Position position) {

    }

    @Override
    public void positionEndProtoBuf(PositionEndProto.PositionEnd positionEnd) {

    }

    @Override
    public void accountSummaryProtoBuf(AccountSummaryProto.AccountSummary accountSummary) {

    }

    @Override
    public void accountSummaryEndProtoBuf(AccountSummaryEndProto.AccountSummaryEnd accountSummaryEnd) {

    }

    @Override
    public void positionMultiProtoBuf(PositionMultiProto.PositionMulti positionMulti) {

    }

    @Override
    public void positionMultiEndProtoBuf(PositionMultiEndProto.PositionMultiEnd positionMultiEnd) {

    }

    @Override
    public void accountUpdateMultiProtoBuf(AccountUpdateMultiProto.AccountUpdateMulti accountUpdateMulti) {

    }

    @Override
    public void accountUpdateMultiEndProtoBuf(AccountUpdateMultiEndProto.AccountUpdateMultiEnd accountUpdateMultiEnd) {

    }

    @Override
    public void historicalDataProtoBuf(HistoricalDataProto.HistoricalData historicalData) {

    }

    @Override
    public void historicalDataUpdateProtoBuf(HistoricalDataUpdateProto.HistoricalDataUpdate historicalDataUpdate) {

    }

    @Override
    public void historicalDataEndProtoBuf(HistoricalDataEndProto.HistoricalDataEnd historicalDataEnd) {

    }

    @Override
    public void realTimeBarTickProtoBuf(RealTimeBarTickProto.RealTimeBarTick realTimeBarTick) {

    }

    @Override
    public void headTimestampProtoBuf(HeadTimestampProto.HeadTimestamp headTimestamp) {

    }

    @Override
    public void histogramDataProtoBuf(HistogramDataProto.HistogramData histogramData) {

    }

    @Override
    public void historicalTicksProtoBuf(HistoricalTicksProto.HistoricalTicks historicalTicks) {

    }

    @Override
    public void historicalTicksBidAskProtoBuf(HistoricalTicksBidAskProto.HistoricalTicksBidAsk historicalTicksBidAsk) {

    }

    @Override
    public void historicalTicksLastProtoBuf(HistoricalTicksLastProto.HistoricalTicksLast historicalTicksLast) {

    }

    @Override
    public void tickByTickDataProtoBuf(TickByTickDataProto.TickByTickData tickByTickData) {

    }

    @Override
    public void updateNewsBulletinProtoBuf(NewsBulletinProto.NewsBulletin newsBulletin) {

    }

    @Override
    public void newsArticleProtoBuf(NewsArticleProto.NewsArticle newsArticle) {

    }

    @Override
    public void newsProvidersProtoBuf(NewsProvidersProto.NewsProviders newsProviders) {

    }

    @Override
    public void historicalNewsProtoBuf(HistoricalNewsProto.HistoricalNews historicalNews) {

    }

    @Override
    public void historicalNewsEndProtoBuf(HistoricalNewsEndProto.HistoricalNewsEnd historicalNewsEnd) {

    }

    @Override
    public void wshMetaDataProtoBuf(WshMetaDataProto.WshMetaData wshMetaData) {

    }

    @Override
    public void wshEventDataProtoBuf(WshEventDataProto.WshEventData wshEventData) {

    }

    @Override
    public void tickNewsProtoBuf(TickNewsProto.TickNews tickNews) {

    }

    @Override
    public void scannerParametersProtoBuf(ScannerParametersProto.ScannerParameters scannerParameters) {

    }

    @Override
    public void scannerDataProtoBuf(ScannerDataProto.ScannerData scannerData) {

    }

    @Override
    public void fundamentalsDataProtoBuf(FundamentalsDataProto.FundamentalsData fundamentalsData) {

    }

    @Override
    public void pnlProtoBuf(PnLProto.PnL pnL) {

    }

    @Override
    public void pnlSingleProtoBuf(PnLSingleProto.PnLSingle pnLSingle) {

    }

    @Override
    public void receiveFAProtoBuf(ReceiveFAProto.ReceiveFA receiveFA) {

    }

    @Override
    public void replaceFAEndProtoBuf(ReplaceFAEndProto.ReplaceFAEnd replaceFAEnd) {

    }

    @Override
    public void commissionAndFeesReportProtoBuf(CommissionAndFeesReportProto.CommissionAndFeesReport commissionAndFeesReport) {

    }

    @Override
    public void historicalScheduleProtoBuf(HistoricalScheduleProto.HistoricalSchedule historicalSchedule) {

    }

    @Override
    public void rerouteMarketDataRequestProtoBuf(RerouteMarketDataRequestProto.RerouteMarketDataRequest rerouteMarketDataRequest) {

    }

    @Override
    public void rerouteMarketDepthRequestProtoBuf(RerouteMarketDepthRequestProto.RerouteMarketDepthRequest rerouteMarketDepthRequest) {

    }

    @Override
    public void secDefOptParameterProtoBuf(SecDefOptParameterProto.SecDefOptParameter secDefOptParameter) {

    }

    @Override
    public void secDefOptParameterEndProtoBuf(SecDefOptParameterEndProto.SecDefOptParameterEnd secDefOptParameterEnd) {

    }

    @Override
    public void softDollarTiersProtoBuf(SoftDollarTiersProto.SoftDollarTiers softDollarTiers) {

    }

    @Override
    public void familyCodesProtoBuf(FamilyCodesProto.FamilyCodes familyCodes) {

    }

    @Override
    public void symbolSamplesProtoBuf(SymbolSamplesProto.SymbolSamples symbolSamples) {

    }

    @Override
    public void smartComponentsProtoBuf(SmartComponentsProto.SmartComponents smartComponents) {

    }

    @Override
    public void marketRuleProtoBuf(MarketRuleProto.MarketRule marketRule) {

    }

    @Override
    public void userInfoProtoBuf(UserInfoProto.UserInfo userInfo) {

    }

    @Override
    public void nextValidIdProtoBuf(NextValidIdProto.NextValidId nextValidId) {

    }

    @Override
    public void currentTimeProtoBuf(CurrentTimeProto.CurrentTime currentTime) {

    }

    @Override
    public void currentTimeInMillisProtoBuf(CurrentTimeInMillisProto.CurrentTimeInMillis currentTimeInMillis) {

    }

    @Override
    public void verifyMessageApiProtoBuf(VerifyMessageApiProto.VerifyMessageApi verifyMessageApi) {

    }

    @Override
    public void verifyCompletedProtoBuf(VerifyCompletedProto.VerifyCompleted verifyCompleted) {

    }

    @Override
    public void displayGroupListProtoBuf(DisplayGroupListProto.DisplayGroupList displayGroupList) {

    }

    @Override
    public void displayGroupUpdatedProtoBuf(DisplayGroupUpdatedProto.DisplayGroupUpdated displayGroupUpdated) {

    }

    @Override
    public void marketDepthExchangesProtoBuf(MarketDepthExchangesProto.MarketDepthExchanges marketDepthExchanges) {}
    
    @Override
    public void commissionAndFeesReport(CommissionAndFeesReport commissionAndFeesReport) {}
}
