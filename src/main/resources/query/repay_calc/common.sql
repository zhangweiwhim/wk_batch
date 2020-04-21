with ods_repayschedule_format as (
    select applyid,
           applycd,
           seqno,
           paytype,
           amount,
           resultflg,
           date_format(repaydate, 'yyyy-MM-dd')    as repaydate,
           date_format(realpaiddate, 'yyyy-MM-dd') as realpaiddate,
           realpaiddate as realpaiddateAll,
           repaymonth
    from ods.ods_t_repayschedule
),

     calc_org as (
         select t1.applyid,
                t1.applycd,
                sum(if(t2.paytype in (12) and t2.realpaiddate is null, t2.amount, 0))                                           as remainseed,
                sum(if(t2.paytype in (13, 22) and t2.realpaiddate is null, t2.amount, 0))                                       as remaininterest,
                sum(if(t2.paytype in (14) and t2.realpaiddate is null, t2.amount, 0))                                           as remainremain,
                if(sum(if(t2.paytype in (12) and t2.realpaiddate is null, 1, 0)) = 0,
                   sum(if(t2.paytype = 14 and t2.realpaiddate is null, 1, 0)),
                   sum(if(t2.paytype in (12) and t2.realpaiddate is null, 1, 0)))                                               as remaincount,
                sum(if(t2.paytype in (12, 13, 22) and t2.realpaiddate is not null, t2.amount, 0))                               as realreceiverefundamount,
                sum(if(t2.paytype in (12, 13, 14, 22) and t2.realpaiddate is null and t2.repaydate >= ${exec_date}, amount, 0)) as remainamount,
                sum(if(t2.paytype in (9) and t2.realpaiddate is null, t2.amount, 0))                                            as legalfee,
                sum(if(t2.paytype between 2 and 7 and t2.realpaiddate is null, t2.amount, 0))                                   as collectfee,
                max(t2.realpaiddateAll)                                                                                            as recentdeducttime
         from ods.ods_c_applyinfo t1
                  left join ods_repayschedule_format t2 on t1.applyid = t2.applyid
         where t1.dt = ${exec_date}
           and t1.closedate is null
           and t1.fininstid in (4, 5, 6)
           and t1.applycd not in ('BAH1211701014788', 'BAH12116119541', 'BAH1211612350823', 'BAH1211701002653')
         group by t1.applyid, t1.applycd
     ),

     overduefine as (
         select t1.applyid,
                sum(if(t2.paytype in (12, 13, 22), t2.amount, 0))                                                                 as receiverefundamount,
                sum(if(t2.paytype in (16) and t2.realpaiddate is null and t2.resultflg <> 6 and t2.resultflg <> 8, t2.amount, 0)) as overduefine
         from ods.ods_c_applyinfo t1
                  left join ods_repayschedule_format t2 on t1.applyid = t2.applyid
         where t1.dt = ${exec_date}
           and t1.closedate is null
           and t2.repaydate < ${exec_date}
           and t2.repaymonth <= date_format(${exec_date}, 'yyyyMM')
         group by t1.applyid
     ),


     overdue_items as (
         select t.applyid,
                count(distinct t.seqno)                   as dueterms,
                sum(if(t.paytype in (12), amount, 0))     as dueseed,
                sum(if(t.paytype in (13, 22), amount, 0)) as dueinterest,
                sum(if(t.paytype in (14), amount, 0))     as dueremain,
                min(t.seqno)                              as dueseqno,
                max(t.repaydate)                          as maxduedate,
                if(min(t.resultflg) = 3, 1, 0)            as chuckflg
         from ods_repayschedule_format t
         where t.seqno > 0
           and t.repaydate < ${exec_date}
           and t.repaymonth <= date_format(${exec_date}, 'yyyyMM')
           and t.realpaiddate is null
--            and t.paytype != 19 -- #17152delete bug point
           and not exists (select 1
                           from ods_repayschedule_format
                           where applyid = t.applyid
                             and seqno = t.seqno
                             and paytype = 16
                             and resultflg = 6)
         group by t.applyid
     ),

     duedate as (
         select t.applyid,
                t.seqno,
                min(t.repaydate)                         as duedate,
                datediff(${exec_date}, min(t.repaydate)) as overduedays
         from ods_repayschedule_format t
         group by t.applyid, t.seqno
     ),

     leasehold as (
         select applyid,
                    count(distinct seqno) as leasehold
         from ods_repayschedule_format t
         where paytype in (12, 14)
--            and not exists (select 1
--                            from ods_repayschedule_format
--                            where applyid = t.applyid
--                              and seqno = t.seqno
--                              and (paytype = 16 or paytype = 13)
--                              and realpaiddate is null)  -- #17152delete bug point
           and realpaiddate is not null
         group by applyid
     ),

     -- wangshangbank
     mybank_org as (
         select applyid,
                0                              as remainremain,
                (ovdprinpenint + ovdintpenint) as overduefine,
                ovdprin                        as dueseed,
                ovdint                         as dueinterest,
                0                              as dueremain
         from ods.ods_t_mybank_repayplan
     ),

     mybank_repay as (
         select applyid,
                applycd,
                sum(if(status = 'ovd', 1, 0))                                                                as dueterms,
                sum(if(status != 'clear', 1, 0))                                                             as remaincount,
                sum(if(status != 'clear', termnomprin + termovdprin, 0))                                     as remainseed,
                sum(if(status != 'clear', termnomint + termovdint, 0))                                       as remaininterest,
                sum(if(status = 'clear', 1, 0))                                                              as leasehold,
                sum(if(termenddate >= ${exec_date}, termnomprin + termnomint + termovdprin + termovdint, 0)) as remainamount,
                sum(if(termenddate < ${exec_date}, termnomprin + termnomint + paidprinbal + paidintbal, 0))  as receiverefundamount,
                sum(if(termenddate < ${exec_date}, paidprinbal + paidintbal, 0))                             as realreceiverefundamount,
                min(if(termovdprin + termovdint > 0, termenddate, null))                                     as duedate,
                min(if(termovdprin + termovdint > 0, termno, null))                                          as dueseqno,
                max(if(termovdprin + termovdint > 0, termenddate, null))                                     as maxduedate,
                datediff(${exec_date}, min(if(termovdprin + termovdint > 0, termenddate, null)))             as overduedays
         from ods.ods_t_mybank_repayplanlist otmr
         group by applyid, applycd
     ),

     -- result
     final as (
         select t1.applyid,
                t1.applycd,
                t1.remainseed,
                t1.remaininterest,
                t1.remainremain,
                t1.remaincount,
                t1.realreceiverefundamount,
                t1.remainamount,
                if(t1.legalfee is not null,t1.legalfee,0) as legalfee, -- bug point
                if(t1.collectfee is not null,t1.collectfee,0) as collectfee, -- bug point
                t1.recentdeducttime,
                if(t2.overduefine is not null,t2.overduefine,0) as overduefine, -- bug point
                if(t2.receiverefundamount is not null,t2.receiverefundamount,0) as receiverefundamount, -- bug point
                t3.dueseqno,
                if(t3.chuckflg is not null,t3.chuckflg,0) as chuckflg, -- bug point
                t3.dueinterest,
                t3.dueremain,
                t3.dueseed,
                if(t3.dueterms is not null ,t3.dueterms,0) as dueterms, -- bug point
                t3.maxduedate,
                t4.duedate,
                t4.overduedays,
                if(t5.leasehold is not null ,t5.leasehold,0) as leasehold -- bug point
         from calc_org t1
                  left join overduefine t2 on t1.applyid = t2.applyid
                  left join overdue_items t3 on t1.applyid = t3.applyid
                  left join duedate t4 on t3.applyid = t4.applyid and t3.dueseqno = t4.seqno
                  left join leasehold t5 on t1.applyid = t5.applyid
         union all
         select mybank_repay.applyid       as applyid,
                applycd,
                remainseed,
                remaininterest,
                mybank_org.remainremain    as remainremain,
                remaincount,
                realreceiverefundamount,
                remainamount,
                0                       as legalfee,  -- bug point
                0                       as collectfee,  -- bug point
                null                       as recentdeducttime,
                if(mybank_org.overduefine is not null ,mybank_org.overduefine,0) as overduefine, -- bug point
                if(receiverefundamount is not null ,receiverefundamount,0) as receiverefundamount, -- bug point
                dueseqno,
                0                       as chuckflg,  -- bug point
                mybank_org.dueinterest     as dueinterest,
                mybank_org.dueremain       as dueremain,
                mybank_org.dueseed         as dueseed,
                if(dueterms is not null ,dueterms,0) as dueterms, -- bug point
                cast(maxduedate as string) as maxduedate,
                cast(duedate as string)    as duedate,
                overduedays,
                if(leasehold is not null ,leasehold,0) as leasehold -- bug point
         from mybank_repay
                  inner join mybank_org on mybank_org.applyid = mybank_repay.applyid
     )

select *
from final where applycd not in ('BAH1211701014788', 'BAH12116119541', 'BAH1211612350823', 'BAH1211701002653')
