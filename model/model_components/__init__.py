"""model_components — Exports all Tradinator pipeline components."""

from .broker_adapter import BrokerAdapter
from .ig_adapter import IGBrokerAdapter
from .broker_connector import BrokerConnector
from .data_pipeline import DataPipeline
from .signal_engine import SignalEngine
from .strategy_eval import StrategyEval
from .portfolio_constructor import PortfolioConstructor
from .order_generator import OrderGenerator
from .order_executor import OrderExecutor
from .portfolio_ledger import PortfolioLedger
from .portfolio_analytics import PortfolioAnalytics
from .performance_monitoring import PerformanceMonitoring
from .reconciliation import Reconciliation

# Optional adapter — import succeeds only when ib_async is installed.
try:
    from .ibkr_adapter import IBKRBrokerAdapter  # noqa: F401
except ImportError:
    pass
