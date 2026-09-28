# Data Model

## Sources
- customers: `customer_id`
- products: `product_id`
- orders: `order_id`, FK `customer_id`
- order_items: `order_item_id`, FK `order_id`, `product_id`

## Gold
- `dim_customer`: SCD Type 2 on `customer_id`, sequenced by `updated_at`
- `dim_product`: current product attributes
- `dim_date`: calendar attributes derived from order dates
- `fact_sales`: one row per non-cancelled order item

Measures:
- quantity
- unit_price
- discount_amount
- gross_sales
- net_sales
