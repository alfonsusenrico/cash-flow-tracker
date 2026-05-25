"""
Tests for Phase 2-5 pure service functions.
No DB required — all functions are pure.
"""
import sys
import os
import pathlib
import unittest
from datetime import date

BACKEND_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

os.environ.setdefault("DATABASE_URL", "postgresql://x:x@localhost/x")
os.environ.setdefault("SESSION_SECRET", "test-secret")


class TestProjectionMath(unittest.TestCase):
    def test_months_until_future(self):
        from app.services.projection import months_until
        future = date(date.today().year + 1, date.today().month, 1)
        self.assertGreater(months_until(future), 0)

    def test_months_until_past_returns_one(self):
        from app.services.projection import months_until
        past = date(2000, 1, 1)
        self.assertEqual(months_until(past), 1)

    def test_inflation_adjusted_target_zero_inflation(self):
        from app.services.projection import inflation_adjusted_target
        result = inflation_adjusted_target(1_000_000, 0.0, 12)
        self.assertEqual(result, 1_000_000)

    def test_inflation_adjusted_target_positive(self):
        from app.services.projection import inflation_adjusted_target
        result = inflation_adjusted_target(1_000_000, 0.05, 12)
        self.assertGreater(result, 1_000_000)

    def test_required_monthly_already_funded(self):
        from app.services.projection import required_monthly_contribution
        result = required_monthly_contribution(1_000_000, 1_000_000, 12, 0.06)
        self.assertEqual(result, 0)

    def test_required_monthly_zero_return(self):
        from app.services.projection import required_monthly_contribution
        result = required_monthly_contribution(1_200_000, 0, 12, 0.0)
        self.assertEqual(result, 100_000)

    def test_required_monthly_with_return(self):
        from app.services.projection import required_monthly_contribution
        result = required_monthly_contribution(1_200_000, 0, 12, 0.06)
        # With 6% annual return over 12 months, PMT is close to 100_000 (within 10%)
        self.assertGreater(result, 0)
        self.assertLess(result, 110_000)

    def test_goal_projection_no_date(self):
        from app.services.projection import goal_projection
        result = goal_projection(
            target_amount=10_000_000,
            current_amount=2_000_000,
            target_date=None,
            inflation_rate=0.05,
            expected_return=0.06,
        )
        self.assertEqual(result["months_remaining"], None)
        self.assertEqual(result["required_monthly"], None)
        self.assertAlmostEqual(result["progress_pct"], 20.0, delta=1.0)

    def test_goal_projection_with_date(self):
        from app.services.projection import goal_projection
        future = date(date.today().year + 2, date.today().month, 1)
        result = goal_projection(
            target_amount=12_000_000,
            current_amount=0,
            target_date=future,
            inflation_rate=0.0,
            expected_return=0.0,
        )
        self.assertIsNotNone(result["required_monthly"])
        self.assertGreater(result["required_monthly"], 0)

    def test_goal_projection_eta(self):
        from app.services.projection import goal_projection
        result = goal_projection(
            target_amount=1_200_000,
            current_amount=0,
            target_date=None,
            inflation_rate=0.0,
            expected_return=0.0,
            monthly_contribution=100_000,
        )
        self.assertEqual(result["eta_months"], 12)


class TestMetrics(unittest.TestCase):
    def test_safe_to_spend_positive(self):
        from app.services.metrics import safe_to_spend
        result = safe_to_spend(5_000_000, 1_000_000, 500_000)
        self.assertEqual(result["value"], 3_500_000)
        self.assertEqual(result["status"], "ok")

    def test_safe_to_spend_zero(self):
        from app.services.metrics import safe_to_spend
        result = safe_to_spend(1_000_000, 1_500_000, 0)
        self.assertEqual(result["value"], 0)
        self.assertEqual(result["status"], "critical")

    def test_emergency_fund_ok(self):
        from app.services.metrics import emergency_fund_coverage
        result = emergency_fund_coverage(6_000_000, 1_000_000)
        self.assertEqual(result["months"], 6.0)
        self.assertEqual(result["status"], "ok")

    def test_emergency_fund_warn(self):
        from app.services.metrics import emergency_fund_coverage
        result = emergency_fund_coverage(4_000_000, 1_000_000)
        self.assertEqual(result["status"], "warn")

    def test_emergency_fund_critical(self):
        from app.services.metrics import emergency_fund_coverage
        result = emergency_fund_coverage(2_000_000, 1_000_000)
        self.assertEqual(result["status"], "critical")

    def test_savings_rate_ok(self):
        from app.services.metrics import savings_rate
        result = savings_rate(10_000_000, 7_000_000)
        self.assertEqual(result["pct"], 30.0)
        self.assertEqual(result["status"], "ok")

    def test_savings_rate_critical_negative(self):
        from app.services.metrics import savings_rate
        result = savings_rate(5_000_000, 6_000_000)
        self.assertLess(result["pct"], 0)
        self.assertEqual(result["status"], "critical")

    def test_monthly_drift_ok(self):
        from app.services.metrics import monthly_drift
        result = monthly_drift(5_000_000, 5_100_000)
        self.assertEqual(result["pct"], 2.0)
        self.assertEqual(result["status"], "ok")

    def test_monthly_drift_critical(self):
        from app.services.metrics import monthly_drift
        result = monthly_drift(5_000_000, 6_500_000)
        self.assertEqual(result["status"], "critical")

    def test_goal_feasibility_ok(self):
        from app.services.metrics import goal_feasibility
        result = goal_feasibility("House", 500_000, 1_000_000)
        self.assertTrue(result["feasible"])
        self.assertEqual(result["status"], "ok")

    def test_goal_feasibility_critical(self):
        from app.services.metrics import goal_feasibility
        result = goal_feasibility("House", 1_000_000, 100_000)
        self.assertFalse(result["feasible"])
        self.assertEqual(result["status"], "critical")


if __name__ == "__main__":
    unittest.main()
