| Query | Card. | Cache | No GSI (median ms) | GSI (median ms) | Speedup | Row groups read (no GSI -> GSI) | Rows returned | n |
|---|---|---|---|---|---|---|---|---|
| `q1_order_id` | high | cold | 1653.5 | 36.8 | 44.96x | 48 -> 1 | 1 | 5 |
| `q1_order_id` | high | warm | 1590.5 | 28.8 | 55.21x | 48 -> 1 | 1 | 5 |
| `q2_customer_id` | high | cold | 1631.9 | 783.2 | 2.08x | 48 -> 23 | 29 | 5 |
| `q2_customer_id` | high | warm | 1596.8 | 759.1 | 2.10x | 48 -> 23 | 29 | 5 |
| `q3_product_id` | high | cold | 1622.6 | 1327.4 | 1.22x | 48 -> 38 | 66 | 5 |
| `q3_product_id` | high | warm | 1604.2 | 1299.5 | 1.23x | 48 -> 38 | 66 | 5 |
| `q4_age` | low | cold | 134.3 | 135.1 | 0.99x (within run-to-run spread) | 4 -> 4 | 3246 | 5 |
| `q4_age` | low | warm | 123.3 | 122.4 | 1.01x (within run-to-run spread) | 4 -> 4 | 3246 | 5 |
