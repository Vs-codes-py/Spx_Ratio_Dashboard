"""
Real Sentiment Engine.
Derives directional order flow sentiment from actual FlowEngine metrics without hard-coded fallbacks.
"""

from typing import Dict, Any


class SentimentEngine:
    """Evaluates market flow sentiment directly from aggregated real volume statistics."""

    def __init__(self, flow_engine, spot_estimator=None, config=None):
        self.flow_engine = flow_engine
        self.spot_estimator = spot_estimator
        self.config = config

    def calculate_sentiment(self, timeframe: str = '1m') -> Dict[str, Any]:
        """Compute directional score, ratio, and reasoning factors."""
        snapshot = self.flow_engine.get_dashboard_snapshot(timeframe=timeframe)

        call_buy = snapshot["call_buy"]
        call_sell = snapshot["call_sell"]
        put_buy = snapshot["put_buy"]
        put_sell = snapshot["put_sell"]

        bullish_vol = snapshot["bullish_volume"]  # Call Buy + Put Sell
        bearish_vol = snapshot["bearish_volume"]  # Call Sell + Put Buy
        total_directional = bullish_vol + bearish_vol

        if total_directional == 0:
            return {
                "sentiment": "NEUTRAL",
                "bull_score": 50.0,
                "bear_score": 50.0,
                "confidence": 0.0,
                "reasons": ["Insufficient directional order flow data."],
            }

        bull_score = round((bullish_vol / total_directional) * 100.0, 1)
        bear_score = round((bearish_vol / total_directional) * 100.0, 1)

        # Confidence calculation based on volume depth and directional dominance
        imbalance = abs(bull_score - bear_score)
        confidence = round(min(100.0, (snapshot["total_volume"] / 1000.0) * 10.0 + imbalance), 1)

        if bull_score >= 55.0:
            sentiment = "BULLISH"
        elif bear_score >= 55.0:
            sentiment = "BEARISH"
        else:
            sentiment = "NEUTRAL"

        reasons = []
        if call_buy > call_sell:
            reasons.append(f"Strong Call Buying aggressors ({call_buy:,.0f} contracts).")
        if put_sell > put_buy:
            reasons.append(f"Put Selling support active ({put_sell:,.0f} contracts).")
        if call_sell > call_buy:
            reasons.append(f"Call Selling overhead resistance ({call_sell:,.0f} contracts).")
        if put_buy > put_sell:
            reasons.append(f"Aggressive Put Buying protection ({put_buy:,.0f} contracts).")

        return {
            "sentiment": sentiment,
            "bull_score": bull_score,
            "bear_score": bear_score,
            "confidence": confidence,
            "reasons": reasons if reasons else ["Order flow balanced between buyers and sellers."],
        }

    def analyze(self, spot: float = 0.0, timeframe: str = '1m') -> Dict[str, Any]:
        """Convenience method returning UI-formatted sentiment dict."""
        s = self.calculate_sentiment(timeframe=timeframe)
        return {
            "Sentiment": s.get("sentiment", "Neutral").capitalize(),
            "Bull Score": int(s.get("bull_score", 50)),
            "Bear Score": int(s.get("bear_score", 50)),
            "Confidence": int(s.get("confidence", 75)),
            "Reasons": s.get("reasons", ["Order flow balanced between buyers and sellers."]),
        }