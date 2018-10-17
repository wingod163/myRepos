#!/bin/bash
source /e3base/.bash_profile
#######################################
# title:  各项营销活动拉新促活效果分析数据需求 
# author: zhaojl_bds
# date:   2018-09-26
# note:   此脚本取2018092-20180930历史数据，历史表采用新分区，因为要跨月分区需采用连接的方式。
#######################################

#参数判断
if [ $# -eq 0 ];then
    v_thisyyyymmdd=`date -d "1 day ago" +%Y%m%d`                          #当天YYYYMMDD
else
    v_thisyyyymmdd=$1
fi


startTime=`date  +%H:%M:%S`
systime=`date  +%Y%m%d%H%M%S`

# 常用参数赋值
#----------------------------------------------------------------------------------
#v_thisyyyymmdd=$1                                                    #当天YYYYMMDD
v_nextyyyymmdd_=`date -d "$v_thisyyyymmdd" +%Y-%m-%d`                 #当天YYYY-MM-DD
v_thisyyyymmdds=`date -d "$v_thisyyyymmdd" +%s`                       #当天时间戳
v_nextyyyymmdd=`date -d "next-day $v_thisyyyymmdd" +%Y%m%d`           #明天YYYYMMDD
v_nextyyyymmdds=`date -d "$v_nextyyyymmdd" +%s`                       #明天时间戳
#----------------------------------------------------------------------------------
v_thisyyyymmfirst=`date  -d "$v_thisyyyymmdd" +%Y%m`01                #本月1号YYYYMMDD
v_thisyyyymm=`date  -d "$v_thisyyyymmdd" +%Y%m`                       #本月YYYYMM
v_thisyyyy=`date -d "$v_thisyyyymmdd" +%Y`                            #本年YYYY
v_thismm=`date -d "$v_thisyyyymmdd" +%m`                              #本月MM
v_thisdd=`date -d "$v_thisyyyymmdd" +%d`                              #当天DD
v_thisyyyymmfirsts=`date -d "$v_thisyyyymmfirst" +%s`                 #本月1号时间戳
#----------------------------------------------------------------------------------
v_dealyyyymmdd=`date -d "1 day ago $v_thisyyyymmfirst" +%Y%m%d`       #上月月末YYYYMMDD
v_dealyyyymmfirst=`date -d "1 day ago $v_thisyyyymmfirst" +%Y%m`01      #上月1号YYYYMMDD
v_dealyyyymm=`date -d "1 day ago $v_thisyyyymmfirst" +%Y%m`           #上月YYYYMM
v_dealyyyy=`date -d "1 day ago $v_thisyyyymmfirst" +%Y`               #上月YYYY
v_dealmm=`date -d "1 day ago $v_thisyyyymmfirst" +%m`           	  #上月MM
v_dealdd=`date -d "1 day ago $v_thisyyyymmfirst" +%d`                 #上月DD
#----------------------------------------------------------------------------------
v_lastyyyymmdd=`date -d "1 day ago $v_dealyyyymmfirst" +%Y%m%d`       #上两月月末YYYYMMDD
v_lastyyyymmfirst=`date -d "1 day ago $v_dealyyyymmfirst" +%Y%m`01    #上两月1号YYYYMMDD
v_lastyyyymm=`date -d "1 day ago $v_dealyyyymmfirst" +%Y%m`           #上两月YYYYMM
v_lastyyyy=`date -d "1 day ago $v_dealyyyymmfirst" +%Y`               #上两月YYYY
v_lastmm=`date -d "1 day ago $v_dealyyyymmfirst" +%m`                 #上两月MM
v_lastdd=`date -d "1 day ago $v_dealyyyymmfirst" +%d`                 #上两月DD
#----------------------------------------------------------------------------------
v_bflastyyyymmdd=`date -d "1 day ago $v_lastyyyymmfirst" +%Y%m%d`     #上三月月末YYYYMMDD
v_bflastyyyymmfirst=`date -d "1 day ago $v_lastyyyymmfirst" +%Y%m`01  #上三月1号YYYYMMDD
v_bflastyyyymm=`date -d "1 day ago $v_lastyyyymmfirst" +%Y%m`         #上三月YYYYMM
v_bflastyyyy=`date -d "1 day ago $v_lastyyyymmfirst" +%Y`             #上三月YYYY 
v_bflastmm=`date -d "1 day ago $v_lastyyyymmfirst" +%m`               #上三月MM
v_bflastdd=`date -d "1 day ago $v_lastyyyymmfirst" +%d`               #上三月DD
#----------------------------------------------------------------------------------
v_lastyearyyyymmdd=`date -d "last year $v_thisyyyymmdd" +%s`          #去年同天时间戳
v_lastyearyyyymmddfirst=`date -d "last year $v_thisyyyymmfirst" +%s`  #去年同月1号时间戳

echo $v_thisyyyymmdd
echo $v_thisyyyy
echo $v_thismm
echo $v_thisdd
echo $v_dealmm
echo $v_lastmm
echo $v_bflastmm

year=${v_thisyyyymmdd:0:4}
echo $year
month=${v_thisyyyymmdd:4:2}
echo $month
aday=${v_thisyyyymmdd:6:2}
echo $aday


############################################################################################################################################

#所有脚本开始

#首先，将文件清空。
> /e3base/zjl/result/tmrt_older2newer_day_${v_thisyyyymmdd}.txt

echo "#######################${v_thisyyyymmdd}各项营销活动拉新促活效果分析#######################" >> /e3base/zjl/result/tmrt_older2newer_day_${v_thisyyyymmdd}.txt
echo "开始时间：startTime = "$startTime >> /e3base/zjl/result/tmrt_older2newer_day_${v_thisyyyymmdd}.txt
echo "系统时间：systime = "$systime >> /e3base/zjl/result/tmrt_older2newer_day_${v_thisyyyymmdd}.txt

echo "参数年月日：v_thisyyyymmdd = "${v_thisyyyymmdd} >> /e3base/zjl/result/tmrt_older2newer_day_${v_thisyyyymmdd}.txt
echo "参数年月：v_thisyyyymm = "${v_thisyyyymm} >> /e3base/zjl/result/tmrt_older2newer_day_${v_thisyyyymmdd}.txt
echo "参数年  ：v_thisyyyy = "${v_thisyyyy} >> /e3base/zjl/result/tmrt_older2newer_day_${v_thisyyyymmdd}.txt
echo "参数月  ：v_thismm = "${v_thismm} >> /e3base/zjl/result/tmrt_older2newer_day_${v_thisyyyymmdd}.txt
echo "参数日  ：v_thisdd = "${v_thisdd} >> /e3base/zjl/result/tmrt_older2newer_day_${v_thisyyyymmdd}.txt


#*********************************************************************************************************************************
#1.1.参与活动的客户情况-目标新用户数

sql="
select '目标新用户数'
,'${v_thisyyyymmdd}'
,count(distinct a1.bind_no) as num
from (select t.bind_no
      from pods.Coupons_V2_TD_PCARD_INFO t
      where t.yearstr=${v_thisyyyy} and t.monthstr=${v_thismm} and t.daystr=${v_thisdd}
      and t.batch_id in ('1036535292015804416','1036539413569601536')
      and regexp_replace(substr(t.bind_time,1,10),'-','')='${v_thisyyyymmdd}'
      and t.if_test='1' 
      group by t.bind_no) a1
            left join (select serial_number
                        from pmrt.app_logins
                        where deal_date='20180901') a2
                        on a1.bind_no=a2.serial_number
where a2.serial_number is null
"


#执行
echo "${sql}"
echo "***********************************************" >> /e3base/zjl/result/tmrt_older2newer_day_${v_thisyyyymmdd}.txt
echo "1.1.参与活动的客户情况-目标新用户数" >> /e3base/zjl/result/tmrt_older2newer_day_${v_thisyyyymmdd}.txt
hive -e "${sql};" >> /e3base/zjl/result/tmrt_older2newer_day_${v_thisyyyymmdd}.txt



#*********************************************************************************************************************************
#1.2.参与活动的客户情况-目标新用户其中用券客户数

sql="
select '目标新用户其中用券客户数'
,'${v_thisyyyymmdd}'
,count(distinct a1.bind_no) as num
from (select t.bind_no
      from pods.Coupons_V2_TD_PCARD_INFO t
      where t.yearstr=${v_thisyyyy} and t.monthstr=${v_thismm} and t.daystr=${v_thisdd}
      and t.batch_id in ('1036535292015804416','1036539413569601536')
      and regexp_replace(substr(t.use_time,1,10),'-','')='${v_thisyyyymmdd}'
      and t.lifecycle_st='10'
      and t.if_test='1' 
      group by t.bind_no) a1
          left join (select serial_number
                      from pmrt.app_logins
                      where deal_date='20180901') a2
                      on a1.bind_no=a2.serial_number
where a2.serial_number is null
"


#执行
echo "${sql}"
echo "***********************************************" >> /e3base/zjl/result/tmrt_older2newer_day_${v_thisyyyymmdd}.txt
echo "1.2.参与活动的客户情况-目标新用户其中用券客户数" >> /e3base/zjl/result/tmrt_older2newer_day_${v_thisyyyymmdd}.txt
hive -e "${sql};" >> /e3base/zjl/result/tmrt_older2newer_day_${v_thisyyyymmdd}.txt




#*********************************************************************************************************************************
#1.3.参与活动的客户情况-目标老用户数

sql="
select '目标老用户数'
,'${v_thisyyyymmdd}'
,count(distinct a1.mb) as num
from (select t.bind_no as mb
      from pods.Coupons_V2_TD_PCARD_INFO t
      where t.yearstr=${v_thisyyyy} and t.monthstr=${v_thismm} and t.daystr=${v_thisdd}
      and t.batch_id in ('1036535292015804416','1036539413569601536')
      and regexp_replace(substr(t.bind_time,1,10),'-','')='${v_thisyyyymmdd}'
      and t.if_test='1' 
      group by t.bind_no) a1
            inner join (select serial_number as mb
                        from pmrt.app_logins
                        where deal_date='20180901') a2
                        on a1.mb=a2.mb
"


#执行
echo "${sql}"
echo "***********************************************" >> /e3base/zjl/result/tmrt_older2newer_day_${v_thisyyyymmdd}.txt
echo "1.3.参与活动的客户情况-目标老用户数" >> /e3base/zjl/result/tmrt_older2newer_day_${v_thisyyyymmdd}.txt
hive -e "${sql};" >> /e3base/zjl/result/tmrt_older2newer_day_${v_thisyyyymmdd}.txt



#*********************************************************************************************************************************
#1.4.参与活动的客户情况-目标老用户其中用券客户数

sql="
select '目标老用户其中用券客户数'
,'${v_thisyyyymmdd}'
,count(distinct a1.bind_no) as num
from (select t.bind_no
      from pods.Coupons_V2_TD_PCARD_INFO t
      where t.yearstr=${v_thisyyyy} and t.monthstr=${v_thismm} and t.daystr=${v_thisdd}
      and t.batch_id in ('1036535292015804416','1036539413569601536')
      and regexp_replace(substr(t.use_time,1,10),'-','')='${v_thisyyyymmdd}'
      and t.lifecycle_st='10'
      and t.if_test='1' 
      group by t.bind_no) a1
            inner join (select serial_number
                        from pmrt.app_logins
                        where deal_date='20180901') a2
                        on a1.bind_no=a2.serial_number
"


#执行
echo "${sql}"
echo "***********************************************" >> /e3base/zjl/result/tmrt_older2newer_day_${v_thisyyyymmdd}.txt
echo "1.4.参与活动的客户情况-目标老用户其中用券客户数" >> /e3base/zjl/result/tmrt_older2newer_day_${v_thisyyyymmdd}.txt
hive -e "${sql};" >> /e3base/zjl/result/tmrt_older2newer_day_${v_thisyyyymmdd}.txt



#*********************************************************************************************************************************
#1.5.参与活动的客户情况-目标老用户平均获得流量券张数

sql="
select '目标老用户数平均获得流量券张数'
,b1.older_num
,b2.pcard_num
,b2.pcard_num/b1.older_num as rate
from (select '${v_thisyyyymmdd}' as d1
      ,count(distinct a1.mb) as older_num
      from (select t.bind_no as mb
            from pods.Coupons_V2_TD_PCARD_INFO t
            where t.yearstr=${v_thisyyyy} and t.monthstr=${v_thismm} and t.daystr=${v_thisdd}
            and t.batch_id in ('1036535292015804416','1036539413569601536')
            and regexp_replace(substr(t.bind_time,1,10),'-','')='${v_thisyyyymmdd}'
            and t.if_test='1' 
              group by t.bind_no) a1
            inner join (select serial_number as mb
                        from pmrt.app_logins
                        where deal_date='20180901') a2
                        on a1.mb=a2.mb) b1
        left join (select '${v_thisyyyymmdd}' as d2
                   ,count(distinct t.pcard_sn) as pcard_num
                   from pods.Coupons_V2_TD_PCARD_INFO t
                   where t.yearstr=${v_thisyyyy} and t.monthstr=${v_thismm} and t.daystr=${v_thisdd}
                   and t.batch_id='1036539413569601536'
                   and regexp_replace(substr(t.bind_time,1,10),'-','')='${v_thisyyyymmdd}'
                   and t.if_test='1') b2
                   on b1.d1=b2.d2
"


#执行
echo "${sql}"
echo "***********************************************" >> /e3base/zjl/result/tmrt_older2newer_day_${v_thisyyyymmdd}.txt
echo "1.5.参与活动的客户情况-目标老用户平均获得流量券张数" >> /e3base/zjl/result/tmrt_older2newer_day_${v_thisyyyymmdd}.txt
hive -e "${sql};" >> /e3base/zjl/result/tmrt_older2newer_day_${v_thisyyyymmdd}.txt



#*********************************************************************************************************************************
#2.1.活跃情况-目标新用户登录APP

sql="
select '目标新用户登录APP'
,'${v_thisyyyymmdd}'
,count(distinct b1.mb) as newer_app_num
from (select a1.mb
      from (select wt_mobile as mb
            from pdwd.tdwd_group_marketing_act_user_dtl_day_orc
            where deal_date='${v_thisyyyymmdd}'
            and busi_type='wt'
            and act_id='2018090701') a1
                  left join (select serial_number as mb
                              from pmrt.app_logins
                              where deal_date='20180901') a2
                              on a1.mb=a2.mb
                              where a2.mb is null
      group by a1.mb) b1
        inner join (select serial_number
                   from pods.BOS_LOGIN_DTL_NEW
                   where year='${v_thisyyyy}' and month = '${v_thismm}' and (day >= '21' and day <= '${v_thisdd}')
                   and serial_number not like '%@%'
                   and length(serial_number)=11
                   and serial_number rlike '^\\\d+$'
                   group by serial_number) b2
                   on b1.mb=b2.serial_number
"


#执行
echo "${sql}"
echo "***********************************************" >> /e3base/zjl/result/tmrt_older2newer_day_${v_thisyyyymmdd}.txt
echo "2.1.活跃情况-目标新用户登录APP" >> /e3base/zjl/result/tmrt_older2newer_day_${v_thisyyyymmdd}.txt
hive -e "${sql};" >> /e3base/zjl/result/tmrt_older2newer_day_${v_thisyyyymmdd}.txt


#*********************************************************************************************************************************
#2.2.活跃情况-目标老用户登录APP

sql="
select '目标老用户登录APP'
,'${v_thisyyyymmdd}'
,count(distinct b1.mb) as newer_app_num
from (select a1.mb
      from (select wt_mobile as mb
            from pdwd.tdwd_group_marketing_act_user_dtl_day_orc
            where deal_date='${v_thisyyyymmdd}'
            and busi_type='wt'
            and act_id='2018090701') a1
                  inner join (select serial_number as mb
                              from pmrt.app_logins
                              where deal_date='20180901') a2
                              on a1.mb=a2.mb
      group by a1.mb) b1
        inner join (select serial_number
                   from pods.BOS_LOGIN_DTL_NEW
                   where year='${v_thisyyyy}' and month = '${v_thismm}' and (day >= '21' and day <= '${v_thisdd}')
                   and serial_number not like '%@%'
                   and length(serial_number)=11
                   and serial_number rlike '^\\\d+$'
                   group by serial_number) b2
                   on b1.mb=b2.serial_number
"


#执行
echo "${sql}"
echo "***********************************************" >> /e3base/zjl/result/tmrt_older2newer_day_${v_thisyyyymmdd}.txt
echo "2.2.活跃情况-目标老用户登录APP" >> /e3base/zjl/result/tmrt_older2newer_day_${v_thisyyyymmdd}.txt
hive -e "${sql};" >> /e3base/zjl/result/tmrt_older2newer_day_${v_thisyyyymmdd}.txt


#*********************************************************************************************************************************
#2.3.活跃情况-沉默用户数

sql="
select '沉默用户数'
,'${v_thisyyyymmdd}'
,count(distinct b1.mb) as slient_num
from (select a1.mb
      from (select t.bind_no as mb
            from pods.Coupons_V2_TD_PCARD_INFO t
            where t.yearstr=${v_thisyyyy} and t.monthstr=${v_thismm} and t.daystr=${v_thisdd}
            and t.batch_id in ('1036535292015804416','1036539413569601536')
            and regexp_replace(substr(t.bind_time,1,10),'-','')='${v_thisyyyymmdd}'
            and t.if_test='1' 
              group by t.bind_no) a1
                  inner join (select serial_number as mb
                              from pmrt.app_logins
                              where deal_date='20180901') a2
                              on a1.mb=a2.mb
      group by a1.mb) b1
        left join (select serial_number
                   from pods.BOS_LOGIN_DTL_NEW
                   where year='${v_thisyyyy}' and month >= '${v_bflastmm}' and month <= '${v_dealmm}'
                   and serial_number not like '%@%'
                   and length(serial_number)=11
                   and serial_number rlike '^\\\d+$'
                   group by serial_number) b2
                   on b1.mb=b2.serial_number
                   where b2.serial_number is null
"


#执行
echo "${sql}"
echo "***********************************************" >> /e3base/zjl/result/tmrt_older2newer_day_${v_thisyyyymmdd}.txt
echo "2.3.活跃情况-沉默用户数" >> /e3base/zjl/result/tmrt_older2newer_day_${v_thisyyyymmdd}.txt
hive -e "${sql};" >> /e3base/zjl/result/tmrt_older2newer_day_${v_thisyyyymmdd}.txt


#*********************************************************************************************************************************
#2.4.活跃情况-沉默用户数活跃数

sql="
select '沉默用户数活跃数'
,'${v_thisyyyymmdd}'
,count(distinct c1.mb) as slient_active_num
from (select b1.mb
      from (select a1.mb
            from (select t.bind_no as mb
                  from pods.Coupons_V2_TD_PCARD_INFO t
                  where t.yearstr=${v_thisyyyy} and t.monthstr=${v_thismm} and t.daystr=${v_thisdd}
                  and t.batch_id in ('1036535292015804416','1036539413569601536')
                  and regexp_replace(substr(t.bind_time,1,10),'-','')='${v_thisyyyymmdd}'
                  and t.if_test='1' 
                  group by t.bind_no) a1
                        inner join (select serial_number as mb
                                    from pmrt.app_logins
                                    where deal_date='20180901') a2
                                    on a1.mb=a2.mb
            group by a1.mb) b1
                  left join (select serial_number
                             from pods.BOS_LOGIN_DTL_NEW
                             where year='${v_thisyyyy}' and month >= '${v_bflastmm}' and month <= '${v_dealmm}'
                             and serial_number not like '%@%'
                             and length(serial_number)=11
                             and serial_number rlike '^\\\d+$'
                             group by serial_number) b2
                               on b1.mb=b2.serial_number
     where b2.serial_number is null
     group by b1.mb) c1
        inner join (select serial_number
                   from pods.BOS_LOGIN_DTL_NEW
                   where year='${v_thisyyyy}' and month = '${v_thismm}' and (day >= '21' and day <= '${v_thisdd}')
                   and serial_number not like '%@%'
                   and length(serial_number)=11
                   and serial_number rlike '^\\\d+$'
                   group by serial_number) c2
                   on c1.mb=c2.serial_number
"


#执行
echo "${sql}"
echo "***********************************************" >> /e3base/zjl/result/tmrt_older2newer_day_${v_thisyyyymmdd}.txt
echo "2.4.活跃情况-沉默用户数活跃数" >> /e3base/zjl/result/tmrt_older2newer_day_${v_thisyyyymmdd}.txt
hive -e "${sql};" >> /e3base/zjl/result/tmrt_older2newer_day_${v_thisyyyymmdd}.txt


#*********************************************************************************************************************************
#2.5.活跃情况-拉新促活

sql="
select'拉新促活'
      ,'${v_thisyyyymmdd}'
      ,case when e.phone_no is not null then 'laxin_user'
            when e.phone_no is null and f.mobile is null then 'cuhuo_user' else 'others' end as catagorys
      ,count(distinct c.wt_mobile) as laxin_cuhuo_num
      from (select act_id
            ,act_name
            ,act_start
            ,act_end
            ,wt_mobile
            from pdwd.tdwd_group_marketing_act_user_dtl_day_orc
            where deal_date='${v_thisyyyymmdd}'
            and busi_type='coupon_bind'
            and act_id='2018090701') c
            left join (select phone_no
                       from pdwd.BUS_USER_INC_INFO_DAY 
                       where deal_date='${v_thisyyyymmdd}'
                       and phone_no rlike '1[0-9]{10}'
                       group by phone_no ) e --新用户判断
                       on c.wt_mobile=e.phone_no
                       left join (select mobile 
                                   from pdwd.tdwd_login_dtl_mon_orc
                                   where deal_month='${v_dealyyyymm}'
                                   and user_type='login3'
                                     group by mobile) f   --沉默用户回归判断
                                   on c.wt_mobile=f.mobile
      group by case when e.phone_no is not null then 'laxin_user'
                    when e.phone_no is null and f.mobile is null then 'cuhuo_user' else 'others' end
"


#执行
echo "${sql}"
echo "***********************************************" >> /e3base/zjl/result/tmrt_older2newer_day_${v_thisyyyymmdd}.txt
echo "2.5.活跃情况-拉新促活" >> /e3base/zjl/result/tmrt_older2newer_day_${v_thisyyyymmdd}.txt
hive -e "${sql};" >> /e3base/zjl/result/tmrt_older2newer_day_${v_thisyyyymmdd}.txt


#*********************************************************************************************************************************
echo "***********************************************" >> /e3base/zjl/result/tmrt_older2newer_day_${v_thisyyyymmdd}.txt
echo "script successful!" >> /e3base/zjl/result/tmrt_older2newer_day_${v_thisyyyymmdd}.txt
echo "script successful"
