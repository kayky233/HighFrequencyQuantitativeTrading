from __future__ import annotations

from hfqt.agents.risk import RiskAgent
from hfqt.config import AppConfig
from hfqt.schemas import InputEvent, RiskDecision, RiskStatus, TradeAction, TradeIntent


class BasicRiskEngine:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.risk_agent = RiskAgent(config)

    async def evaluate(self, intent: TradeIntent, prior_order_count: int, event: InputEvent | None = None) -> RiskDecision:
        reasons: list[str] = []
        notional = None
        resolved_min_confidence, threshold_mode = self._resolve_min_confidence(event)

        if intent.action == TradeAction.HOLD:
            reasons.append("Intent action is HOLD.")
        if intent.symbol not in self.config.allowed_symbols:
            reasons.append(f"Symbol {intent.symbol} is not on the allowlist.")
        if intent.confidence < resolved_min_confidence:
            reasons.append(
                f"Intent confidence {intent.confidence:.2f} is below dynamic minimum {resolved_min_confidence:.2f}."
            )
        if intent.quantity <= 0:
            reasons.append("Quantity must be positive.")
        if intent.limit_price is None:
            reasons.append("Limit price is required for ignition-phase orders.")
        elif intent.limit_price <= 0:
            reasons.append("Limit price must be positive.")

        if intent.limit_price is not None:
            notional = intent.quantity * intent.limit_price
            if notional > self.config.max_notional_per_order:
                reasons.append(
                    f"Notional {notional:.2f} exceeds max per order {self.config.max_notional_per_order:.2f}."
                )

        if prior_order_count >= self.config.max_orders_per_day:
            reasons.append(
                f"Daily order count {prior_order_count} has reached max {self.config.max_orders_per_day}."
            )

        risk_agent_result = None
        if event is not None and self.risk_agent.enabled:
            risk_agent_result = await self.risk_agent.assess(
                event=event,
                intent=intent,
                prior_order_count=prior_order_count,
                resolved_min_confidence=resolved_min_confidence,
                estimated_notional=notional,
            )
            reasons.extend(self._merge_risk_agent_constraints(intent, notional, risk_agent_result))

        status = RiskStatus.REJECT if reasons else RiskStatus.ALLOW
        if not reasons:
            approval_reason = "All ignition-phase checks passed."
            if risk_agent_result and str(risk_agent_result.get("rationale") or "").strip():
                approval_reason = f"{approval_reason} Risk agent approved: {risk_agent_result.get('rationale')}"
            reasons.append(approval_reason)

        return RiskDecision(
            intent_id=intent.intent_id,
            status=status,
            reasons=reasons,
            notional=notional,
            metadata={
                "resolved_min_confidence": resolved_min_confidence,
                "threshold_mode": threshold_mode,
                "risk_agent": {
                    "enabled": self.risk_agent.enabled,
                    "status": "APPROVE" if not risk_agent_result or bool(risk_agent_result.get("approved", True)) else "REJECT",
                    "provider": (risk_agent_result or {}).get("agent_provider"),
                    "model": (risk_agent_result or {}).get("agent_model"),
                    "risk_level": (risk_agent_result or {}).get("risk_level"),
                    "rationale": (risk_agent_result or {}).get("rationale"),
                    "confidence_cap": (risk_agent_result or {}).get("confidence_cap"),
                    "quantity_cap": (risk_agent_result or {}).get("quantity_cap"),
                    "max_notional": (risk_agent_result or {}).get("max_notional"),
                },
            },
        )

    def _resolve_min_confidence(self, event: InputEvent | None) -> tuple[float, str]:
        if event is None:
            return self.config.min_confidence, "static"

        price_action = (event.metadata or {}).get("price_action") or {}
        range_pct = float(price_action.get("intraday_range_pct_30m") or 0.0)
        base = self.config.min_confidence

        if range_pct <= self.config.dynamic_threshold_low_vol_pct:
            return min(0.95, base + self.config.dynamic_threshold_low_vol_bump), "low_volatility"
        if range_pct >= self.config.dynamic_threshold_high_vol_pct:
            return max(0.35, base - self.config.dynamic_threshold_high_vol_discount), "high_volatility"
        return base, "normal_volatility"

    @staticmethod
    def _merge_risk_agent_constraints(intent: TradeIntent, notional: float | None, result: dict | None) -> list[str]:
        if not result:
            return []

        reasons: list[str] = []
        approved = bool(result.get("approved", True))
        rationale = str(result.get("rationale") or "").strip()
        if not approved:
            reasons.append(f"Risk agent rejected the trade. {rationale}".strip())

        confidence_cap = result.get("confidence_cap")
        if confidence_cap not in {None, ""}:
            confidence_cap_value = max(0.0, min(1.0, float(confidence_cap)))
            if intent.confidence > confidence_cap_value:
                reasons.append(
                    f"Risk agent capped confidence at {confidence_cap_value:.2f}, below current intent confidence {intent.confidence:.2f}."
                )

        quantity_cap = result.get("quantity_cap")
        if quantity_cap not in {None, ""}:
            quantity_cap_value = max(0.0, float(quantity_cap))
            if intent.quantity > quantity_cap_value:
                reasons.append(
                    f"Risk agent capped quantity at {quantity_cap_value:.2f}, below current quantity {intent.quantity:.2f}."
                )

        max_notional = result.get("max_notional")
        if max_notional not in {None, ""} and notional is not None:
            max_notional_value = max(0.0, float(max_notional))
            if notional > max_notional_value:
                reasons.append(
                    f"Risk agent capped notional at {max_notional_value:.2f}, below current notional {notional:.2f}."
                )
        return reasons
