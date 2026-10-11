-- Published table read by dashboards. Only built if staging tests pass.
select
    o.order_date,
    c.country,
    count(*)                                                    as orders,
    count(*) filter (where o.status in ('paid', 'shipped'))     as paid_orders,
    sum(o.amount) filter (where o.status in ('paid', 'shipped')) as revenue
from {{ ref('stg_orders') }} o
left join {{ ref('stg_customers') }} c using (customer_id)
group by 1, 2
