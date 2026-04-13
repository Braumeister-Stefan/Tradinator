"""
Tradinator — Orchestrator.

Instantiates all pipeline components and runs them in the correct sequence.
This is the only place where components are aware of each other's existence.
Data flows strictly forward through the pipeline: steps 1 → 10.
"""

from model_components import (
    BrokerConnector,
    DataPipeline,
    SignalEngine,
    StrategyEval,
    PortfolioConstructor,
    OrderGenerator,
    OrderExecutor,
    PortfolioLedger,
    PortfolioAnalytics,
    PerformanceMonitoring,
)


class Model:
    """Orchestrates the full trading pipeline."""

    def __init__(self, config):
        self.config = config
        self.broker_connector = BrokerConnector(config)
        self.data_pipeline = DataPipeline(config)
        self.signal_engine = SignalEngine(config)
        self.strategy_eval = StrategyEval(config)
        self.portfolio_constructor = PortfolioConstructor(config)
        self.order_generator = OrderGenerator(config)
        self.order_executor = OrderExecutor(config)
        self.portfolio_ledger = PortfolioLedger(config)
        self.portfolio_analytics = PortfolioAnalytics(config)
        self.performance_monitoring = PerformanceMonitoring(config)

    def run(self):
        """Execute the full pipeline: Gather → Decide → Execute → Report."""

        # Phase 1: GATHER
        broker_state = self.broker_connector.run()
        market_data = self.data_pipeline.run(broker_state)

        # Phase 2: DECIDE
        signals = self.signal_engine.run(market_data)
        validated_signals = self.strategy_eval.run(signals, market_data)
        target_portfolio = self.portfolio_constructor.run(
            validated_signals, broker_state
        )

        # Phase 3: EXECUTE
        orders = self.order_generator.run(target_portfolio, broker_state, market_data)
        execution_log = self.order_executor.run(orders, broker_state)

        # Phase 4: RECORD & REPORT
        # Re-fetch broker state after execution so ledger records post-trade reality.
        broker_state = self.broker_connector.run()
        ledger_snapshot = self.portfolio_ledger.run(execution_log, broker_state)
        analytics = self.portfolio_analytics.run(ledger_snapshot)
        self.performance_monitoring.run(analytics)
