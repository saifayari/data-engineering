-- Thin view on raw: NO filtering on purpose, so the tests below see the bad rows.
select
    order_id,
    customer_id,
    order_date,
    status,
    amount,
    _loaded_at
from {{ source('raw', 'orders') }}
