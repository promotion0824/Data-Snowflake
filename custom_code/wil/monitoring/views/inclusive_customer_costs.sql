-- this is redone in:   R__013_create_view_dataset_allocated_details
create or replace view COSTS.INCLUSIVE_CUSTOMER_COSTS
comment = 'Customer daily costs, with shared costs allocated across all customers in a subscription'
as

select
    case sl.vertical
      when 'Real Estate' then
        case strtok(d.resource_group,'-',4)
          when 'lda' then 'IoT Services'
          when 'alm' then 'IoT Services'
          when 'mkp' then 'Marketplace'
          else 'Real Estate'
        end
      else sl.vertical
    end as vertical,
    sl.environment_category as category,
    cl.name as customerName,
    d.* 
from 
(
with summary_table as (
    select 
    date,
    coalesce (
      iff(subscription_name = 'Experience-PRD','inv',null),
      iff(resource_group = 'wr-prod-ccvw', 'stu', null),
      iff(resource_group = 'wr-prod-apps', 'stu', null),
      iff(subscription_name = 'Rail-PRD' and resource_location = 'WESTEUROPE', 'stu', null),
      iff(subscription_name = 'Rail-PRD' and resource_location = 'AUSTRALIASOUTHEAST', 'bhp', null),
      iff(subscription_name = 'Rail-PRE', 'bhp', null),
      iff(subscription_name = 'Mining-POC', 'ncp', null),
      customer,
      iff(lower(strtok(resource_group,'-',4)) = 'lda', strtok(resource_group,'-',5), null),
      iff(lower(strtok(resource_group,'-',1)) = 'wdt', strtok(resource_group,'-',3), null),
      'shared'
    ) as cust,
    resource_group,
    subscription_name,
    any_value(subscription_guid) as id,
    resource_location,
    sum(iff(cust = 'shared', COST_IN_USD, 0)) as shared_cost,
    sum(cost_in_usd) as cost,
    sum(iff(cust = 'shared', 0, COST_IN_USD)) as allocated_cost,
    service_family

    from daily_resource_cost
    
    group by date, customer, client, resource_group, subscription_name, service_family, resource_location
)
select 
  date,
  cust as customer, 
  subscription_name,
  any_value(id) as subscription_id,
  resource_group,
  sum(cost) as totalCost,
  sum(allocated_cost) as allocatedCost,
  SUM(allocatedcost) over (partition by date, subscription_name, cust) as custAllocatedCost,
  SUM(allocatedcost) over (partition by date, subscription_name) as allAllocatedCost,
  sum(shared_cost) as sharedCost,
  SUM(sharedCost) over (partition by date, subscription_name) as totalSharedCost,
  iff(custAllocatedCost = 0, 0, to_number(custAllocatedCost, 18,10) / to_number(allAllocatedCost, 18, 10)) as customerProportionPct,
  iff(allocatedCost = 0, 0, to_number(allocatedCost, 18, 10) / to_number(custAllocatedCost, 18, 10)) as itemProportionForCustPct,
  to_number(totalSharedCost * customerProportionPct * itemProportionForCustPct) + allocatedCost as fullyAllocatedItemCost

  from  summary_table
  group by date, subscription_name, resource_group, cust
  order by cust, subscription_name, resource_group, date
) d
left join subscription_lookup sl on d.subscription_name = sl.subscription
left join customer_lookup cl on d.customer = cl.abbreviation
where category = 'Production'
order by date, customerName, subscription_name
;