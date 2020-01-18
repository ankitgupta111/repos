with zoh as ( Select tab_id,status,reject_message_name,city_name,country_name,res_id,res_name,platform,clubbing_flag,paytm_flag,
                     otof_flag,new_user_flag,rpf_flag,gold_order_flag,user_karma_score,ps_segment,payment_method_type,
                     logistics_partner_id,hybrid_flag,single_serve_flag,coalesce(assigned_at,driver_assigned_at) AS assigned_at,
                     linked_at,nps,user_paid_amount,user_id,date_add('minute',cast(pickup_eta as int),assigned_at) AS est_arrival_time,date_add('minute',cast(kpt as int),linked_at) AS est_kitchen_prep_time,
                     coalesce(picked_at,shipped_at) as picked_at,date_add('minute',cast(edt as int),linked_at) AS est_del_time,
                     coalesce(delivered_at,complete_at) delivered_at ,coalesce(reached_shop_at,arrived_at) AS arrived_at,
                     date_add('minute',cast(ddt as int),linked_at) AS est_ddt,rejected_at,
                     (case when date_add('minute',cast(pickup_eta as int),assigned_at)>date_add('minute',cast(kpt as int),linked_at) then date_add('minute',(cast(pickup_eta as int)+2),assigned_at) else  date_add('minute',cast(kpt as int)+2,linked_at) end) AS est_pickup_time,created_at
                 
                 from jumbo_derived.zomato_order_history
                 where dt between format_datetime(date_add('day', -60, date_parse('{{start_dt}}', '%Y%m%d')), 'yyyyMMdd') and   '{{end_dt}}'
            ),
            
      ch1 as ( Select user_id,session_id,landed_at,assigned_at,resolved_at,channel_id,aht,agent_id,agent_email,tl_name,tl_email,
                     resolution_tag,lrt,rt_lrt,status,reopen_flag,fr_given_by,agent_frt,frt,frt_timestamp,total_irs,good_irs,tab_id
                     ,csat,rated_at,dt
               from jumbo_derived.chat_history
              where  agent_id<>1
                and status <>2 
                and dt between '{{start_dt}}'  and  '{{end_dt}}'),
                
  mtm as (SELECT tab_id,  case when a = '1' then 'Delay'
                                when a = '2' then 'Rejection' 
                                when a = '3' then 'Cancellation' 
                                when a = '4' then 'Missing Item' 
                                when a = '5' then 'Billing/Others' 
                                when a = '6' then 'Z Logistics' 
                                when a = '7' then 'Poor Quality' 
                                when a = '8' then 'Referral'
                                when a = '9' then 'Promo' 
                                when a = '10' then 'Card/Netbanking' 
                                when a = '11' then 'PayTM' 
                                when a = '12' then 'Mobikwik'
                                when a = '13' then 'FND' 
                                when a = '14' then 'Others'
                                when a = '15' then 'Modifications' 
                                when a = '16' then 'Z Query'
                                when a = '17' then 'Order Status' 
                                when a = '18' then 'Social'
                                when a = '19' then 'Crystal'
                                when a = '20' then 'Websupport'
                                when a = '21' then 'Instructions'
                                when a = '22' then 'FSSAI-PoorQuality'
                                when a = '23' then 'TreatsMissing'
                                when a = '24' then 'GST'
                                when a = '25' then 'Wrongorder'
                                when a = '26' then 'CDP1'
                                when a = '27' then 'not_dispatched'
                                when a = '28' then 'Instructions_not_followed'
                                when a = '29' then 'wallet_issue'
                                when a = '30' then 'RiderBehaviour'
                                when a = '31' then 'DCissue'
                                when a = '32' then 'RunnerTips'
                                when a = '33' then 'PackagingIssue'
                                when a = '34' then 'CC_Pilot'
                                when a = '35' then 'PiggyBank'
                                when a = '36' then 'ArrangeAMeal'
                                when a = '37' then 'pickupconfusion'
                                when a = '38' then 'PaytmRefundIssue'
                                when a = '39' then 'PaytmcashbackIssue'
                                When a ='40' then 'Cashback Issue'
                                When a = '41' then 'Health Issue'
                                When a = '42' then 'Tamper Proof Packaging'
                                When a = '44' then 'WRONG ORDER - NO INVOICE'
                                When a = '45' then 'WRONG ORDER - FOOD AND INVOICE DO NOT MATCH'
                                When a = '46' then 'WRONG ORDER - FOOD AND INVOICE MATCH' END AS tags,b.added_on
            FROM dynamodb.mint_tabs_meta
            CROSS JOIN UNNEST(MINT_TAB_TAGS) AS t (a, b)
            where mint_tab_tags is not null),

 ch2 as ( Select ch1.*,max_by(mtm.tags,mtm.added_on) AS tags
           from ch1
          left join mtm on ch1.tab_id=mtm.tab_id and ch1.resolved_at>=mtm.added_on
          group by user_id,session_id,landed_at,assigned_at,resolved_at,channel_id,aht,agent_id,agent_email,tl_name,tl_email,resolution_tag,lrt,rt_lrt,status,reopen_flag,fr_given_by,agent_frt,frt,frt_timestamp,total_irs,good_irs,ch1.tab_id,csat,rated_at,dt
        ),
        
        
t as (Select tab_id,node,created_at,(lag(node) over( partition by user_id order by created_at)) AS t,user_selected_option
        from jumbo_derived.chat_bot_events
        where dt between '{{start_dt}}' and '{{end_dt}}'
        ),
        
b as ( Select tab_id,(case when node='CHAT_WITH_US  :-I have another issue' then 'CHAT_WITH_US' else node end) AS node,created_at
         from t
    where (node in ('WHERE_IS_MY_ORDER','OTHER_ISSUES','INCORRECT_ORDER','POOR_QUALITY','ORDER_CANCELLATION','CANT_FIND_MY_ORDER','DELIVERY_INSTRUCTION_OTHER','COOKING_INSTRUCTION_OTHER','ORDER_SPILLED','FOOD_NOT_DELIVERED','BILLING_ISSUES','PAYMENT_REFUND_QUERY','MODIFY_ORDER','REFERRAL_ISSUES','ISSUE_WITH_VALET','POOR_QUANTITY','VALET_NOT_ASSIGNED','CANCELLATION_FEE_REASON','PROMO_CODE_ISSUES','ORDER_CANCELLATION_REASON','CHAT_WITH_US  :-I have another issue','REFUND_QUERY','ZOMATOGOLD_ISSUES','OTOF_ORDER_QUERY','OTOF_ORDER_ISSUES','ISSUE_WITH_RECEIVED_ORDER')
            or (node='CHAT_WITH_US' and t='HELP_WITH_ORDER' and user_selected_option='Connect to an agent')   )
     ),
     
 ch as ( Select ch2.*,max_by(b.node,created_at) AS node
           from ch2
         left join b on ch2.tab_id=b.tab_id and ch2.assigned_at>=b.created_at
         group by user_id,session_id,landed_at,assigned_at,resolved_at,channel_id,aht,agent_id,agent_email,tl_name,tl_email,resolution_tag,lrt,rt_lrt,status,reopen_flag,fr_given_by,agent_frt,frt,frt_timestamp,total_irs,good_irs,ch2.tab_id,csat,rated_at,dt,tags
),

p1 as (Select ch.*,zoh.status AS order_status,reject_message_name,city_name,country_name,res_name,res_id,platform,clubbing_flag,paytm_flag,hybrid_flag,otof_flag,new_user_flag,rpf_flag,gold_order_flag,
     case when zoh.user_id =0 then 'loggedout' 
          when user_karma_score is null then 'uncategorized'
          else lower(user_karma_score) end AS karma_score,
      case when zoh.tab_id is not null and ps_segment is null then 'NA' else ps_segment end AS ps_segment,
      payment_method_type,
      case when logistics_partner_id=0 then 'nonlogs' 
               when zoh.tab_id is not null then 'logs' end AS driver_partner,single_serve_flag,nps,
    case when zoh.tab_id is not null and (zoh.assigned_at>landed_at or zoh.assigned_at is null) then 0 
         when zoh.tab_id is not null then 1 end AS  fe_assigned_flag, 
    case when  zoh.tab_id is not null and (zoh.linked_at>landed_at or zoh.linked_at is null) then 0 
       when zoh.tab_id is not null then 1 end AS order_acceptance_flag,
    (case when zoh.tab_id is null then null
       when user_paid_amount >=1000 then '1000+'
       when user_paid_amount >=600 and user_paid_amount<1000 then '600-1000'
       when user_paid_amount >=400 and user_paid_amount<600 then '400-600'
       when (floor(user_paid_amount/50)*50)=(ceiling((user_paid_amount)/50)*50) then concat(cast(cast(floor(user_paid_amount/50)*50 as int) as varchar),'-',cast(cast((ceiling(user_paid_amount/50)*50) as int)+50  as varchar))
       
       when user_paid_amount>= (floor(user_paid_amount/50)*50) and user_paid_amount < (ceiling((user_paid_amount)/50)*50) then concat(cast(cast(floor(user_paid_amount/50)*50 as int) as varchar),'-',cast(cast((ceiling(user_paid_amount/50)*50) as int)  as varchar)) end) AS order_value_bucket,user_paid_amount,
          
      (case when (ch.assigned_at>created_at and (linked_at>ch.assigned_at or ((rejected_at<=linked_at or linked_at is null) and rejected_at>ch.assigned_at))) then 'before acceptance'
           when (ch.assigned_at>=rejected_at or zoh.status=7) then 'after rejection'
           when ch.assigned_at>=delivered_at and (ch.assigned_at<rejected_at or rejected_at is null) then 'after delivery(before rejection or no rejection)'
           when (ch.assigned_at>=linked_at  and logistics_partner_id=0 and ch.assigned_at<=est_ddt and (ch.assigned_at<rejected_at or rejected_at is null))  then 'after acceptance before ddt'
           when (ch.assigned_at>=linked_at and logistics_partner_id=0 and ch.assigned_at>est_ddt and (ch.assigned_at<rejected_at or rejected_at is null) and ch.assigned_at<=est_del_time) then 'after ddt before edt'
           when (ch.assigned_at>=linked_at and logistics_partner_id=0 and ch.assigned_at>est_ddt and ch.assigned_at>est_del_time
           and (ch.assigned_at<rejected_at or rejected_at is null)) then 'after edt before delivery'
           when (ch.assigned_at>linked_at and logistics_partner_id>0 and (ch.assigned_at<=zoh.assigned_at or (zoh.assigned_at is null and rejected_at>=ch.assigned_at))) then 'after acceptance and before rider assignment'
           when (logistics_partner_id>0 and ch.assigned_at>linked_at and ch.assigned_at>zoh.assigned_at and (ch.assigned_at<=zoh.arrived_at or (zoh.arrived_at is null and rejected_at>=ch.assigned_at))) then 'after rider assignment and before fe arrival'
           when (ch.assigned_at>linked_at and logistics_partner_id>0 and ch.assigned_at>zoh.assigned_at and ch.assigned_at>zoh.arrived_at and  (ch.assigned_at< picked_at  or (picked_at is null and ch.assigned_at<=rejected_at))) then 'after fe arrival before pickup'
           when (ch.assigned_at>linked_at and logistics_partner_id>0 and ch.assigned_at>zoh.assigned_at and ch.assigned_at>picked_at  and (ch.assigned_at<=est_ddt and (ch.assigned_at<=rejected_at or rejected_at is null))) then 'after pickup before ddt'
           when (ch.assigned_at>linked_at  and logistics_partner_id>0 and ch.assigned_at>zoh.assigned_at and ch.assigned_at> picked_at and ch.assigned_at>est_ddt and ch.assigned_at<=est_del_time and (ch.assigned_at<=rejected_at or rejected_at is null)) then 'after ddt before edt'
           when (ch.assigned_at>linked_at and logistics_partner_id>0 and ch.assigned_at>zoh.assigned_at and ch.assigned_at> picked_at and ch.assigned_at>est_ddt and ch.assigned_at>est_del_time and (ch.assigned_at<=delivered_at or (delivered_at is null and rejected_at>=ch.assigned_at))) then 'after edt before delivery'
           when (ch.assigned_at>linked_at and logistics_partner_id>0 and ch.assigned_at>zoh.assigned_at and ch.assigned_at> picked_at and ch.assigned_at>est_ddt and ch.assigned_at>est_del_time and ch.assigned_at>delivered_at and (rejected_at is null or rejected_at>=ch.assigned_at)) then 'after delivery(before rejection or no rejection)'
         end) AS order_stage,
           
      (case when (coalesce(linked_at,rejected_at)>=date_add('second',120,created_at)) then 'acceptance_breach'
           when (coalesce(linked_at,rejected_at)<date_add('second',120,created_at) and logistics_partner_id>0 and coalesce(zoh.assigned_at,rejected_at)>=date_add('second',300,created_at)) then 'fe_assignment_breach'
           when (logistics_partner_id>0 and coalesce(linked_at,rejected_at)<date_add('second',120,created_at) and coalesce(zoh.assigned_at,rejected_at)<date_add('second',300,created_at) and  coalesce(arrived_at,rejected_at)>=est_pickup_time) then 'fe_arrival_breach'
           when (logistics_partner_id>0 and coalesce(linked_at,rejected_at)<date_add('second',120,created_at) and coalesce(zoh.assigned_at,rejected_at)<date_add('second',300,created_at) and coalesce(arrived_at,rejected_at)<=est_pickup_time and coalesce(picked_at,rejected_at)>=est_pickup_time) then 'kpt_breach'
           when (logistics_partner_id>0  and coalesce(linked_at,rejected_at)<date_add('second',120,created_at) and coalesce(zoh.assigned_at,rejected_at)<date_add('second',300,created_at) and coalesce(arrived_at,rejected_at)<=est_pickup_time and coalesce(picked_at,rejected_at)<=est_pickup_time and coalesce(delivered_at,rejected_at)>=est_del_time) then 'edt_breach'
           when logistics_partner_id>0 then 'No-breach' end) AS breach,
           created_at,linked_at,rejected_at,zoh.assigned_at AS assigned_at_fe,est_pickup_time,arrived_at,picked_at,est_del_time,delivered_at
               
from  ch
left join  zoh on ch.tab_id=zoh.tab_id

),

p2 as (Select *,
              (case when breach='acceptance_breach' then date_diff('second',date_add('second',120,created_at),coalesce(linked_at,rejected_at))
               when breach='fe_assignment_breach' then date_diff('second',date_add('second',300,created_at),coalesce(assigned_at_fe,rejected_at))
               when breach='fe_arrival_breach' then  date_diff('second',est_pickup_time,coalesce(arrived_at,rejected_at))
               when breach='kpt_breach' then date_diff('second',est_pickup_time,coalesce(picked_at,rejected_at))
               when breach='edt_breach' then date_diff('second',est_del_time,coalesce(delivered_at,rejected_at)) end) AS breach_time
               
         from p1
)
 


Select  localtimestamp AS row_computed_at,user_id,session_id,landed_at,assigned_at,resolved_at,channel_id,aht,agent_id,agent_email,tl_name,tl_email,resolution_tag,lrt,rt_lrt,status,reopen_flag,fr_given_by,agent_frt,frt,frt_timestamp,total_irs,good_irs,tab_id,csat,rated_at,tags,node,order_status,reject_message_name,city_name,country_name,res_name,res_id,platform,clubbing_flag,paytm_flag,hybrid_flag,otof_flag,new_user_flag,rpf_flag,gold_order_flag,karma_score,ps_segment,payment_method_type,driver_partner,single_serve_flag,nps,fe_assigned_flag,order_acceptance_flag,order_value_bucket,user_paid_amount,order_stage,breach,case when breach_time >=0 and breach_time<300 then '0 to 5 minutes'  
             when breach_time >=300 and breach_time<600 then '5 to 10 minutes'
             when breach_time >=600 and breach_time<900 then '10 to 15 minutes'
             when breach_time >=900 and breach_time<1200 then '15 to 20 minutes'
             when breach_time >=1200 and breach_time<1500 then '20 to 25 minutes'
             when breach_time >=1500 and breach_time<1800 then '25 to 30 minutes' 
             when breach_time is not null then '30+ minutes' end AS breach_time,dt
   from p2
 
order by assigned_at
