"""The benchmark query suite and its literals.

Shared by bench/verify_ab.py and bench/run_bench.py so the plan-difference gate
and the timing run provably exercise the same four queries with the same four
values. If these drifted apart, the gate would be vouching for something the
benchmark never ran.
"""
from __future__ import annotations

QUERIES = [
    # (id, table, column, cardinality class, SQL template)
    ("q1_order_id",    "transactions", "order_id",    "high",
     "SELECT * FROM public.transactions WHERE order_id = %(v)s"),
    ("q2_customer_id", "transactions", "customer_id", "high",
     "SELECT * FROM public.transactions WHERE customer_id = %(v)s"),
    ("q3_product_id",  "transactions", "product_id",  "high",
     "SELECT * FROM public.transactions WHERE product_id = %(v)s"),
    # Deliberate negative control. A GSI on a low-cardinality column has a
    # posting for nearly every row group, so the index lookup is pure overhead
    # on top of a scan it cannot avoid. If this regresses, the README says so.
    ("q4_age",         "customers",    "age",         "low",
     "SELECT * FROM public.customers WHERE age = %(v)s"),
]


def pick_literals(connect, args) -> dict[str, object]:
    """Choose one real value per indexed column, deterministically.

    Values come from the data rather than being hardcoded, so the suite
    survives a regenerated lake or a different --target. The generator is
    seeded, so these selections are stable across runs at a given target.

    q2 and q3 deliberately pick the *most frequent* customer_id and product_id
    rather than a typical one. That is the worst case for a global secondary
    index: the hottest value is the one whose postings span the most row
    groups, so the index avoids the least I/O. Reported speedups for those two
    columns are therefore a lower bound, not a best case.
    """
    out: dict[str, object] = {}
    conn = connect(args)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT order_id FROM public.transactions "
                        "ORDER BY order_id OFFSET 1000 LIMIT 1")
            out["q1_order_id"] = cur.fetchone()[0]
            cur.execute("SELECT customer_id FROM public.transactions "
                        "GROUP BY customer_id ORDER BY count(*) DESC, customer_id LIMIT 1")
            out["q2_customer_id"] = cur.fetchone()[0]
            cur.execute("SELECT product_id FROM public.transactions "
                        "GROUP BY product_id ORDER BY count(*) DESC, product_id LIMIT 1")
            out["q3_product_id"] = cur.fetchone()[0]
            # A mid-range age that every generated chunk covers. customers.age
            # is drawn from randint(18 + chunk_index, 80), so a value near the
            # middle exists in every file at every --target.
            out["q4_age"] = 30
    finally:
        conn.close()
    return out
