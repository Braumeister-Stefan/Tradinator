"""
Tradinator — Orchestrator.

Instantiates all pipeline components and runs them in the correct sequence.
This is the only place where components are aware of each other's existence.
Data flows strictly forward through the pipeline: steps 1 → 10.
"""

from .handoff import Handoff
from .model_components import (
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
    Reconciliation,
    UniverseRefresher,
)
from .model_components.data_pipeline import filter_by_gaps, filter_by_history, load_universe


class Model:
    """Orchestrates the full trading pipeline."""

    def __init__(self, config):
        self.config = config
        self._maybe_refresh_universe()
        self.broker_connector = BrokerConnector(config)
        self.reconciliation = Reconciliation(config)
        self.data_pipeline = DataPipeline(config)
        self.signal_engine = SignalEngine(config)
        self.strategy_eval = StrategyEval(config)
        self.portfolio_constructor = PortfolioConstructor(config)
        self.order_generator = OrderGenerator(config)
        self.order_executor = OrderExecutor(config)
        self.portfolio_ledger = PortfolioLedger(config)
        self.portfolio_analytics = PortfolioAnalytics(config)
        self.performance_monitoring = PerformanceMonitoring(config)

    def _maybe_refresh_universe(self):
        if not self.config.get("refresh_universe", False):
            return
        print("[Model] refresh_universe=True \u2014 validating candidates against broker.")
        bc = BrokerConnector(self.config)
        adapter = bc._create_adapter()
        adapter.connect()
        try:
            if self.config.get("push_candidates", False):
                # Discover + merge new candidates first (shares the adapter).
                from data.input import stock_scoper
                stock_scoper.run(self.config, adapter)
            UniverseRefresher(self.config).run(adapter)
        finally:
            try:
                if getattr(adapter, "_ib", None) is not None:
                    adapter._ib.disconnect()
            except Exception:
                pass
        self.config["universe"] = load_universe(self.config["universe_path"])
        self.config["universe"] = filter_by_history(self.config["universe"], self.config)
        self.config["universe"] = filter_by_gaps(self.config["universe"], self.config)
        print(f"[Universe] Eligible for pricing: {len(self.config['universe'])} instrument(s).")

    def run_research(self):
        """Execute the research pipeline: Gather → Decide."""
        # Phase 1: GATHER
        broker_state = self.broker_connector.run()
        broker_state = self.reconciliation.run(broker_state)
        revalidate = self.config.get("revalidate", False)
        market_data = self.data_pipeline.run(broker_state, revalidate=revalidate)

        # Phase 2: DECIDE
        signals = self.signal_engine.run(market_data)
        validated_signals = self.strategy_eval.run(signals, market_data)
        target_portfolio = self.portfolio_constructor.run(
            validated_signals, broker_state
        )

        return {
            "broker_state": broker_state,
            "market_data": market_data,
            "signals": signals,
            "validated_signals": validated_signals,
            "target_portfolio": target_portfolio,
        }

    def run_execution(self, research_output=None):
        """Execute the trading pipeline: Execute → Record → Report."""
        if research_output is None:
            research_output = Handoff.read(
                self.config.get("output_dir", "data/output"),
                self.config.get("max_handoff_age_seconds", 7200),
            )
            if research_output is None:
                print("[Model] No valid research available, skipping execution.")
                return
            broker_state = self.broker_connector.run()
            research_output["broker_state"] = broker_state

        broker_state = research_output["broker_state"]
        market_data = research_output["market_data"]
        target_portfolio = research_output["target_portfolio"]

        # Phase 3: EXECUTE
        orders = self.order_generator.run(target_portfolio, broker_state, market_data)
        execution_log = self.order_executor.run(orders, broker_state, market_data)

        # Phase 4: RECORD & REPORT
        # Re-fetch broker state after execution so ledger records post-trade reality.
        broker_state = self.broker_connector.run()
        broker_state = self.reconciliation.run(broker_state)
        ledger_snapshot = self.portfolio_ledger.run(execution_log, broker_state)
        analytics = self.portfolio_analytics.run(ledger_snapshot)
        self.performance_monitoring.run(analytics)

    def run(self):
        """Execute the full pipeline: Gather → Decide → Execute → Report."""
        research_output = self.run_research()
        self.run_execution(research_output)

    def shutdown(self):
        """Release broker resources. Safe to call multiple times."""
        try:
            self.broker_connector.close()
        except Exception:
            pass
