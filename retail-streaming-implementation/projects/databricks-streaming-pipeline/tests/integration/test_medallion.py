from decimal import Decimal
from retail.transforms import parse_events, validate, order_changes, flatten_items, sales_summary, window_metrics
from tests.unit.test_transforms import frame, NOW
from scripts.generate import event

def test_bronze_to_gold(spark):
    data = validate(parse_events(frame(spark, [event(1, NOW), event(2, NOW)]))).filter("is_valid")
    orders = order_changes(data)
    summaries = sales_summary(flatten_items(orders)).collect()
    assert sum(r.gross_revenue for r in summaries) == Decimal("303000.00")
    assert sum(r.total_quantity for r in summaries) == 6
    metrics = window_metrics(orders).first()
    assert metrics.order_count == 2 and metrics.quantity == 6
    assert metrics.average_order_value == Decimal("151500.00")
