
with customers_data as (
    select * from {{ref('stg_jaffle_shop_customers')}}

),

orders as (

    select * from {{ref('stg_jaffle_shop_orders')}}

),

customer_orders as (

    select
        customer_id,

        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders
    from orders

    group by 1

),


final as (

    select
        customers_data.customer_id,
        customers_data.first_name,
        customers_data.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders

    from customers_data

    left join customer_orders using (customer_id)

)

select * from final
