#!/bin/sh
source /e3base/.bash_profile
##################################################################
# 程序名称：手厅活跃用户数(日)--生成数据V3.0
# 程序功能：
# 创建信息：2018-01-26  by zhaojl_bds
# 修改信息：从统一认证和手厅和插码数据中取数
##################################################################
if [ $# -eq 0 ];then
echo "缺少参数YYYYMMDD"
exit
fi

startTime=`date  +%H:%M:%S`
systime=`date  +%Y%m%d%H%M%S`
#脚本名称
pname="tmrt_day_app_activeuser_data"

#常用参数赋值
    v_thisyyyymmdd=$1                                                     #当天YYYYMMDD
    v_thisyyyymmdds=`date -d "$v_thisyyyymmdd" +%s`                       #当天时间戳
    v_nextyyyymmdd=`date -d "next-day $v_thisyyyymmdd" +%Y%m%d`           #明天YYYYMMDD
    v_nextyyyymmdds=`date -d "$v_nextyyyymmdd" +%s`                       #明天时间戳
    v_thisyyyymmfirst=`date  -d "$v_thisyyyymmdd" +%Y%m`01                #本月1号YYYYMMDD
    v_dealyyyymm=`date -d "1 day ago $v_thisyyyymmfirst" +%Y%m`           #上月YYYYMM
    v_thisyyyymm=`date  -d "$v_thisyyyymmdd" +%Y%m`                       #本月YYYYMM
    v_thisyyyy=`date -d "$v_thisyyyymmdd" +%Y`                            #本年YYYY
    v_thismm=`date -d "$v_thisyyyymmdd" +%m`                              #本月MM
    v_thisdd=`date -d "$v_thisyyyymmdd" +%d`                              #当天DD
    v_lastyyyymmdd=`date  -d "1 day ago $v_thisyyyymmfirst" +%Y%m%d`      #上月月末YYYYMMDD
    v_lastyearyyyymmdd=`date -d "last year $v_thisyyyymmdd" +%Y%m%d`      #去年同天时间
    v_lastyyyy=`date -d "$v_lastyearyyyymmdd" +%Y`                        #去年同天取年YYYY
    v_lastmm=`date -d "$v_lastyearyyyymmdd" +%m`                          #去年同天取月MM
    v_lastdd=` date -d "$v_lastyearyyyymmdd" +%d`                         #去年同天取日DD

    v_lastyearyyyymmddfirst=`date -d "last year $v_thisyyyymmfirst" +%s`  #去年同月1号时间戳
    v_thisyyyymmfirsts=`date  -d "$v_thisyyyymmfirst" +%s`                #本月1号时间戳

    v_thisyyyymmddss=`date -d "$v_thisyyyymmdd" +%s`                      #当天时间戳
    v_dealyyyymmdd=`date  -d "1 day ago $v_thisyyyymmdd" +%Y%m%d`         #前一天YYYYMMDD
    v_dealyyyy=`date -d "$v_dealyyyymmdd" +%Y`                            #前一天取年YYYY
    v_dealmm=`date -d "$v_dealyyyymmdd" +%m`                              #前一天取月MM
    v_dealdd=` date -d "$v_dealyyyymmdd" +%d`                             #前一天取日DD


    v_lastyyyymmfirst=`date -d "1 day ago $v_thisyyyymmfirst" +%Y%m`01    #前一月1号YYYYMMDD
    v_2lastyyyymmfirst=`date -d "1 day ago $v_lastyyyymmfirst" +%Y%m`01   #前两月1号YYYYMMDD
    v_3lastyyyymmfirst=`date -d "1 day ago $v_2lastyyyymmfirst" +%Y%m`01  #前三月1号YYYYMMDD
    v_4lastyyyymmfirst=`date -d "1 day ago $v_3lastyyyymmfirst" +%Y%m`01  #前四月1号YYYYMMDD
    v_5lastyyyymmfirst=`date -d "1 day ago $v_4lastyyyymmfirst" +%Y%m`01  #前五月1号YYYYMMDD
    v_6lastyyyymmfirst=`date -d "1 day ago $v_5lastyyyymmfirst" +%Y%m`01  #前六月1号YYYYMMDD
    v_lastyyyymm=`date -d "$v_lastyyyymmfirst" +%Y%m`                     #前一月YYYYMM
    v_2lastyyyymm=`date -d "$v_2lastyyyymmfirst" +%Y%m`                   #前两月YYYYMM
    v_3lastyyyymm=`date -d "$v_3lastyyyymmfirst" +%Y%m`                   #前三月YYYYMM
    v_4lastyyyymm=`date -d "$v_4lastyyyymmfirst" +%Y%m`                   #前四月YYYYMM
    v_5lastyyyymm=`date -d "$v_5lastyyyymmfirst" +%Y%m`                   #前五月YYYYMM
    v_6lastyyyymm=`date -d "$v_6lastyyyymmfirst" +%Y%m`                   #前六月YYYYMM

    echo "${v_thisyyyymmdd}"
    #echo "${v_lastyyyymm}"
    #echo "${v_2lastyyyymm}"
    #echo "${v_3lastyyyymm}"
    #echo "${v_4lastyyyymm}"
    #echo "${v_5lastyyyymm}"
    #echo "${v_6lastyyyymm}"


###################################################################################################
#手厅活跃用户数(日)
#create table if not exists pmrt.tmrt_day_app_activeuser_data
#(
#     flag         string
#    ,stat_day     string
#    ,prov_code    string
#    ,city_code    string
#    ,actnum       int
#)
#partitioned by (deal_date string)
#row format delimited
#fields terminated by '\t'
#lines terminated by '\n'
#stored as orcfile;

###################################################################################################
#手厅活跃用户数表示统计周期内认证登录用户数、统一封装、能力调用按手机号去重的全部用户数

#hive配置参数
#set hive.exec.parallel=true;
#set mapreduce.map.memory.mb=2048;
#set mapred.map.child.java.opts=-XX:-UseGCOverheadLimit -Xms1538m -Xmx1538m -verbose:gc -Xloggc:/tmp/mr_log/@taskid@.gc;
#set mapreduce.reduce.memory.mb=2048;
#set mapred.reduce.child.java.opts=-XX:-UseGCOverheadLimit -Xms1538m -Xmx1538m -verbose:gc -Xloggc:/tmp/mr_log/@taskid@.gc;
#set mapred.max.split.size=536870912;
#set mapred.min.split.size.per.node=268435456;
#set mapred.min.split.size.per.rack=268435456;
#set hive.merge.smallfiles.avgsize=64000000;
#set hive.merge.mapfiles=true;
#set hive.merge.mapredfiles=true;


#分市活跃用户数
sql="
set hive.exec.parallel=true;
set mapreduce.map.memory.mb=2048;
set mapred.map.child.java.opts=-XX:-UseGCOverheadLimit -Xms1538m -Xmx1538m -verbose:gc -Xloggc:/tmp/mr_log/@taskid@.gc;
set mapreduce.reduce.memory.mb=2048;
set mapred.reduce.child.java.opts=-XX:-UseGCOverheadLimit -Xms1538m -Xmx1538m -verbose:gc -Xloggc:/tmp/mr_log/@taskid@.gc;
set mapred.max.split.size=536870912;
set mapred.min.split.size.per.node=268435456;
set mapred.min.split.size.per.rack=268435456;
set hive.merge.smallfiles.avgsize=64000000;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
insert overwrite table pmrt.tmrt_month_app_activeuser_data partition(deal_date=${v_thisyyyymm})
select '50'     --校验标记
,'${v_thisyyyy}-${v_thismm}'  --日期
,t1.prov_code  --省份编码
,t1.city_code  --地市编码
,t1.daysum     --活跃用户数
,t2.register_num --注册用户数
,t3.index1  --新增活跃用户数
,t4.index1  --app渠道活跃次数
,t4.index2  --app渠道活跃用户数
,t4.index1/t4.index2     --人均活跃次数
,t1.daysum/t2.register_num --用户活跃度
,t3.index1/t1.daysum  --拉新率
,t5.index2            --一级app的活跃用户数
,t5.index1            --省级app与一级app重合的活跃用户数
,t5.index1/t5.index2  --客群聚重度
,t6.index1  --上月app活跃用户在本月app活跃的用户数
,t6.index2  --上个月的活跃用户数
,t6.index1/t6.index2  --留存率
from (select d2.prov_code
      ,d2.city_code
      ,count(distinct d1.login_name ) as daysum
      from (select a1.login_name
            from (select login_name as login_name
                  from pods.tl_user_login_msg_log
                  where year=${v_thisyyyy} and month=${v_thismm}
                  and spid in (12005,12043,12012,12013,12014)
                  and login_name not like '%@%'
                  and length(login_name)=11
                  and login_name rlike '^\\\d+$'
                  group by login_name
                union all
                  select serial_number as login_name
                  from pods.BOS_LOGIN_DTL_NEW
                  where year=${v_thisyyyy} and month=${v_thismm}
                  and serial_number not like '%@%'
                  and length(serial_number)=11
                  and serial_number rlike '^\\\d+$'
                  group by serial_number
                union all
                  select mobile as login_name
                  from pmrt.tmrt_wt_group_prov_h5_active_mobile_day_orc
                  where yearstr=${v_thisyyyy} and monstr=${v_thismm}
                  and mobile not like '%@%'
                  and length(mobile)=11
                  and mobile rlike '^\\\d+$'
                  group by mobile)a1
            group by a1.login_name) d1
                left join (select section
                           ,prov_code
                           ,city_code
                           from pods.MNG_AREANUM_INFO
                           where substring(regexp_replace(expiry_date, '-', ''),1,8)>${v_thisyyyymmfirst}) d2
                           on substring(d1.login_name,1,7)=d2.section
      where d2.prov_code is not null
      and d2.city_code is not null
      group by d2.prov_code
      ,d2.city_code) t1
      left join (select t.prov_code
                 ,t.city_code
                 ,t.register_num  --注册用户数
                 from pdwd.tdwd_register_user_num_city t
                 where t.deal_date=${v_thisyyyymmdd:0:6}) t2
                 on t1.prov_code=t2.prov_code
                 and cast(t1.city_code as bigint)=t2.city_code
      left join (select t.prov_code
                 ,t.city_code
                 ,count(distinct case when t.create_date like '%${v_thisyyyy}-${v_thismm}%' then serial_number end) as index1  --新增活跃用户数
                 from pmrt.tmrt_update_mobile_user_total t
                 where t.deal_date='${v_thisyyyymmdd}'
                 group by t.prov_code
                 ,t.city_code) t3
                 on t1.prov_code=t3.prov_code
                 and cast(t1.city_code as bigint)=t3.city_code
      left join (select t2.prov_cd as prov_code
                 ,t2.ld_area_cd as city_code
                 ,count(1) as index1  --app渠道活跃次数
                 ,count(distinct serial_number)  as index2  --app活跃用户数
                 from pods.BOS_LOGIN_DTL_NEW t1
                     left join pods.TF_M_SECTION_NUMBER t2
                     on substring(t1.serial_number,1,7)=t2.msisdn_area_id
                 where t1.year=${v_thisyyyy:0:4} and t1.month=${v_thisyyyymmdd:4:2}
                 group by t2.prov_cd
                 ,t2.ld_area_cd) t4
                 on t1.prov_code=t4.prov_code
                 and cast(t1.city_code as int)=t4.city_code
      left join (select b1.prov_code
                 ,b1.city_code as city_code
                 ,count(distinct case when  b2.serial_number is not null then b1.serial_number end) as index1  --省app与一级app重合的活跃用户数
                 ,count(distinct b1.serial_number) as index2  --一级app的活跃用户数
                 from (select t2.prov_cd as prov_code
                       ,t2.ld_area_cd as city_code
                       ,t1.serial_number
                       from pods.BOS_LOGIN_DTL_NEW t1
                           left join pods.TF_M_SECTION_NUMBER t2
                           on substring(t1.serial_number,1,7)=t2.msisdn_area_id
                       where t1.year=${v_thisyyyy:0:4} and t1.month=${v_thisyyyymmdd:4:2}
                       group by t2.prov_cd
                       ,t2.ld_area_cd
                       ,t1.serial_number) b1
                           left join (select a1.phone_num as serial_number
                                      ,t2.prov_cd as prov_code
                                      ,t2.ld_area_cd  as city_code
                                      from (select phone_num
                                            from pods.app_login_province
                                            where deal_date like '%${v_thisyyyymm}%'
                                            group by phone_num) a1
                                            left join pods.TF_M_SECTION_NUMBER t2
                                            on substring(a1.phone_num,1,7)=t2.msisdn_area_id) b2
                                            on b1.serial_number=b2.serial_number
                 group by b1.prov_code
                 ,b1.city_code)t5
                 on t1.prov_code=t5.prov_code
                 and cast(t1.city_code as int)=t5.city_code
      left join (select nvl(a1.prov_cd,a2.prov_cd) as prov_code
                 ,nvl(a1.city_code,a2.city_code) as city_code
                 ,count(case when a1.serial_number is not null and a2.serial_number is not null then a1.serial_number end ) as index1 --上月app活跃用户在本月app活跃的用户数,即留存用户
                 ,count(case when a2.serial_number is not null then a2.serial_number end ) as index2 --上个月的活跃用户数
                 ,count(case when a1.serial_number is not null and a2.serial_number is not null then a1.serial_number end )/count(case when a2.serial_number is not null then a2.serial_number end ) as index3  --留存率
                 from (select t2.prov_cd as prov_cd
                       ,t2.ld_area_cd as city_code
                       ,serial_number
                       from pods.BOS_LOGIN_DTL_NEW t1
                           inner join pods.TF_M_SECTION_NUMBER t2
                           on substring(t1.serial_number,1,7)=t2.msisdn_area_id
                       where t1.year=${v_thisyyyy:0:4} and t1.month=${v_thisyyyymmdd:4:2}
                       group by serial_number
                       ,t2.prov_cd
                       ,t2.ld_area_cd) a1
                           full join (select t2.prov_cd as prov_cd
                                      ,t2.ld_area_cd as city_code
                                      ,serial_number
                                      from pods.BOS_LOGIN_DTL_NEW t1
                                          inner join pods.TF_M_SECTION_NUMBER t2
                                          on substring(t1.serial_number,1,7)=t2.msisdn_area_id
                                      where t1.year=${v_dealyyyymm:0:4} and t1.month=${v_dealyyyymm:4:2}
                                      group by serial_number
                                      ,t2.prov_cd
                                      ,t2.ld_area_cd) a2
                                      on a1.serial_number=a2.serial_number
                                      and a1.prov_cd=a2.prov_cd
                                      and a1.city_code=a2.city_code
                 group by nvl(a1.prov_cd,a2.prov_cd)
                 ,nvl(a1.city_code,a2.city_code)) t6
                 on t1.prov_code=t6.prov_code
                 and cast(t1.city_code as int)=t6.city_code
"

    v_num=1
#执行
    echo "${sql}"
    hive -e "${sql};"
#异常处理
    if [ "$?" -ne 0 ]
    then
    sed_sql=`echo $sql | sed  's/\t/ /g' `
    hive -e "insert into table pods.p_log
             select \"$pname\",\"$sed_sql\",\"$systime\",\"$v_num\"
             from pods.dual;"
    echo "always failed" 2>&1
    exit 1
    fi
####插入分省汇总数据
sql="
insert into table pmrt.tmrt_month_app_activeuser_data partition (deal_date=${v_thisyyyymm})
select
'50',
'${v_thisyyyy}-${v_thismm}',
t1.prov_code,
'0000',
sum(t1.actnum),
sum(t1.regist_num),
sum(t1.add_activityuser_num),
sum(t1.activity_num),
sum(t1.activity_user_num),
sum(t1.activity_num)/sum(t1.activity_user_num),
sum(t1.actnum)/sum(t1.regist_num),
sum(t1.add_activityuser_num)/sum(t1.actnum),
sum(jituan_user_num),
sum(cross_user_num),
sum(cross_user_num)/sum(jituan_user_num),
sum(remain_num),
sum(last_month_num),
sum(remain_num)/sum(last_month_num)
from
pmrt.tmrt_month_app_activeuser_data t1
where t1.deal_date='${v_thisyyyymm}'
and t1.prov_code!='9999' and t1.city_code!='0000'
group by t1.prov_code
"
   v_num=1
#执行
   echo "${sql}"
   hive -e "${sql};"

 if [ "$?" -ne 0 ]
    then
    sed_sql=`echo $sql | sed  's/\t/ /g' `
    hive -e "insert into table pods.p_log
             select \"$pname\",\"$sed_sql\",\"$systime\",\"$v_num\"
             from pods.dual;"
    echo "always failed" 2>&1
    exit 1
  fi
######################插入全国汇总数据
sql="
insert into table pmrt.tmrt_month_app_activeuser_data partition (deal_date=${v_thisyyyymm})
select
'50',
'${v_thisyyyy}-${v_thismm}',
'9999',
'0000',
sum(t1.actnum),
sum(t1.regist_num),
sum(t1.add_activityuser_num),
sum(t1.activity_num),
sum(t1.activity_user_num),
sum(t1.activity_num)/sum(t1.activity_user_num),
sum(t1.actnum)/sum(t1.regist_num),
sum(t1.add_activityuser_num)/sum(t1.actnum),
sum(jituan_user_num),
sum(cross_user_num),
sum(cross_user_num)/sum(jituan_user_num),
sum(remain_num),
sum(last_month_num),
sum(remain_num)/sum(last_month_num)
from
pmrt.tmrt_month_app_activeuser_data t1
where t1.deal_date='${v_thisyyyymm}'
and t1.prov_code!='9999' and t1.city_code='0000'
"
   v_num=1
#执行
   echo "${sql}"
   hive -e "${sql};"

 if [ "$?" -ne 0 ]
    then
    sed_sql=`echo $sql | sed  's/\t/ /g' `
    hive -e "insert into table pods.p_log
             select \"$pname\",\"$sed_sql\",\"$systime\",\"$v_num\"
             from pods.dual;"
    echo "always failed" 2>&1
    exit 1
  fi

#保证31省数据完整性
sql="
insert overwrite table pmrt.tmrt_month_app_activeuser_data partition (deal_date=${v_thisyyyymm})
 select
  '50'
  ,'${v_thisyyyy}-${v_thismm}'
  ,nvl(t1.prov_code,0)
  ,nvl(t1.city_code,0)
  ,nvl(actnum,0)
  ,nvl(regist_num,0)
  ,nvl(add_activityuser_num,0)
  ,nvl(activity_num,0)
  ,nvl(activity_user_num,0)
  ,nvl(avg_activity_num,0)
  ,nvl(activty_ratio,0)
  ,nvl(laxin_rate,0)
  ,nvl(jituan_user_num,0)
  ,nvl(cross_user_num,0)
  ,nvl(contract_ratio,0)
  ,nvl(remain_num,0)
  ,nvl(last_month_num,0)
  ,nvl(remain_rate,0)
  from (select prov_code,city_code from pods.PROV_AREA_INFO) t1
       left join (select
                    flag
                    ,stat_day
                    ,prov_code
                    ,city_code
                    ,actnum
                    ,regist_num
                    ,add_activityuser_num
                    ,activity_num
                    ,activity_user_num
                    ,avg_activity_num
                    ,activty_ratio
                    ,laxin_rate
                    ,jituan_user_num
                    ,cross_user_num
                    ,contract_ratio
                    ,remain_num
                    ,last_month_num
                    ,remain_rate
                    from pmrt.tmrt_month_app_activeuser_data
                    where deal_date='${v_thisyyyymm}') t2
        on  t1.prov_code=t2.prov_code
        and t1.city_code=t2.city_code


"
   v_num=1
#执行
   echo "${sql}"
   hive -e "${sql};"

 if [ "$?" -ne 0 ]
    then
    sed_sql=`echo $sql | sed  's/\t/ /g' `
    hive -e "insert into table pods.p_log
             select \"$pname\",\"$sed_sql\",\"$systime\",\"$v_num\"
             from pods.dual;"
    echo "always failed" 2>&1
    exit 1
  fi

#结束
    echo "script successful"

    endTime=`date  +%H:%M:%S`

    sT=`date +%s -d$startTime`
    eT=`date +%s -d$endTime`
    let useTime=`expr $eT - $sT`
    echo "
    Run `basename $0` ok !
        startTime = $startTime
        endTime = $endTime
        useTime = $useTime (s)!"
