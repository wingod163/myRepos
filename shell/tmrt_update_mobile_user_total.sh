#!/bin/sh
source /e3base/.bash_profile
##################################################################
# 程序名称：日新增登录客户包含更新明细
# 程序功能：
# 创建信息：2017-03-30  by huangkr
# 修改信息：2017-08-16  by huangkr
# 修改信息：2018-10-17  by zhaojl_bds  更改了sql结构，方便阅读，逻辑不变
##################################################################
if [ $# -eq 0 ];then
 echo "缺少参数YYYYMMDD"
 exit
fi

startTime=`date  +%H:%M:%S`
systime=`date  +%Y%m%d%H%M%S`
#脚本名称


#常用参数赋值
v_thisyyyymmdd=$1                                                     #当天YYYYMMDD
v_thisyyyymmdd_=`date -d "$v_thisyyyymmdd" +%Y-%m-%d`                 #当天YYYY-MM-DD
v_dealyyyymmdd=`date  -d "1 day ago $v_thisyyyymmdd" +%Y%m%d`     #前一天YYYYMMDD
v_thisyyyymmdds=`date -d "$v_thisyyyymmdd" +%s`                       #当天时间戳
v_nextyyyymmdd=`date -d "next-day $v_thisyyyymmdd" +%Y%m%d`           #明天YYYYMMDD
v_nextyyyymmdds=`date -d "$v_nextyyyymmdd" +%s`                       #明天时间戳

v_thisyyyymmfirst=`date  -d "$v_thisyyyymmdd" +%Y%m`01                #本月1号YYYYMMDD
v_last1yyyymmdd=`date -d "1 day ago $v_thisyyyymmfirst" +%Y%m%d`      #上一月末YYYYMMDD
v_last1yyyymmfirst=`date  -d "$v_last1yyyymmdd" +%Y%m`01              #上一月1号YYYYMMDD
v_last2yyyymmdd=`date -d "1 day ago $v_last1yyyymmfirst" +%Y%m%d`     #上两月末YYYYMM
v_last2yyyymmfirst=`date  -d "$v_last2yyyymmdd" +%Y%m`01              #上两月1号YYYYMMDD
v_last3yyyymmdd=`date -d "1 day ago $v_last2yyyymmfirst" +%Y%m%dd`    #上三月末YYYYMM

v_this1yyyy=`date -d "$v_last1yyyymmdd" +%Y`                          #上一月的YYYY
v_last1mm=`date -d "$v_last1yyyymmdd" +%m`                            #上一月MM
v_this2yyyy=`date -d "$v_last2yyyymmdd" +%Y`                          #上两月的YYYY
v_last2mm=`date -d "$v_last2yyyymmdd" +%m`                            #上两月MM
v_this3yyyy=`date -d "$v_last3yyyymmdd" +%Y`                          #上三月的YYYY
v_last3mm=`date -d "$v_last3yyyymmdd" +%m`                            #上三月MM

v_thisyyyymm=`date  -d "$v_thisyyyymmdd" +%Y%m`                       #本月YYYYMM
v_thisyyyy=`date -d "$v_thisyyyymmdd" +%Y`                            #本年YYYY
v_thismm=`date -d "$v_thisyyyymmdd" +%m`                              #本月MM
v_thisdd=`date -d "$v_thisyyyymmdd" +%d`                              #当天DD
v_rightyyyymmdd=`date -d " -1 day " +%Y%m%d`
echo ${v_rightyyyymmdd}

echo $v_thisyyyymmdd
year=${v_thisyyyymmdd:0:4}
echo $year
month=${v_thisyyyymmdd:4:2}
echo $month
day=${v_thisyyyymmdd:6:2}
echo $day

#if [ ${v_rightyyyymmdd} -ne ${v_thisyyyymmdd} ]
#then
#echo ‘"请手动执行 tmrt_update_mobile_user_total_hand.sh"
#exit 1
#fi
#########插入新增用户
sql="
insert overwrite table pmrt.tmrt_update_mobile_user_total partition (deal_date='${v_thisyyyymmdd}')
select c1.serial_number
,max(case when c1.imei='N' then ''
      when c1.imei='NULL' then ''
      when c1.imei is null then '' else c1.imei end)
,max(c1.channel_code)
,max(c1.sys_plat)
,max(c1.mbosvesrion)
,max(c1.mb_type_info)
,max(c1.scr_pix)
,max(c1.client_ver)
,max(case when c1.create_date is null then '' else c1.create_date end)  --这些CASE WHEN语句为了规范化格式
,max(case when c1.create_time is null then '' else c1.create_time end)
,max(case when c1.update_date is null then '' else c1.update_date end)
,max(case when c1.update_time is null then '' else c1.update_time end)
,max(case when c1.last_channel_code is null then '' else c1.last_channel_code end)
,max(c1.xk)
,max(case when c1.last_login_time is null then '' else c1.last_login_time end)
,max(c1.prov_code)
,max(c1.city_code)
,max(c1.city_name)
,''
,''
,''
,''
,''
from (select a1.serial_number  --以下这些字段就是将两表组合起来形成a1表以后列出所有的字段，外加创建了一个update_time2时间戳
      ,a1.imei
      ,a1.channel_code
      ,a1.sys_plat
      ,a1.mbosvesrion
      ,a1.mb_type_info
      ,a1.scr_pix
      ,a1.client_ver
      ,a1.create_date
      ,a1.create_time
      ,a1.update_date
      ,a1.update_time
      ,a1.last_channel_code
      ,a1.xk
      ,a1.last_login_time
      ,a1.prov_code
      ,a1.city_code
      ,a1.city_name
      ,unix_timestamp(concat(case when a1.update_date='' and a1.create_date<>'' then a1.create_date 
                                  when a1.update_date='' and a1.create_date='' then '2017-08-03' else a1.update_date end                            
                      ,' '
                      ,case when a1.update_time='' and a1.create_time<>'' then a1.create_time 
                            when a1.update_time='' and a1.create_time='' then '00:00:00' else a1.update_time end)) as update_time2  --创建一个update_time2时间戳
      from (select serial_number 
            ,imei
            ,channel_code
            ,sys_plat
            ,mbosvesrion
            ,mb_type_info
            ,scr_pix
            ,client_ver
            ,create_date
            ,create_time
            ,update_date
            ,update_time
            ,last_channel_code
            ,xk
            ,last_login_time
            ,prov_code
            ,city_code
            ,city_name
            from pmrt.tmrt_distinct_bus_user_info
            where deal_date='${v_thisyyyymmdd}'
          union all   --将两个表组合起来
            select serial_number
            ,imei
            ,channel_code
            ,sys_plat
            ,mbosvesrion
            ,mb_type_info
            ,scr_pix
            ,client_ver
            ,create_date
            ,create_time
            ,update_date
            ,update_time
            ,last_channel_code
            ,xk
            ,last_login_time
            ,prov_code
            ,city_code
            ,city_name
            from pmrt.tmrt_update_mobile_user_total t
            where t.deal_date='${v_dealyyyymmdd}') a1) c1 
                inner join (select serial_number  --a2作为副本，为了保证所有的手机号都存在
                            ,max(unix_timestamp(concat(case when t2.update_date='' and t2.create_date<>'' then t2.create_date 
                                                            when t2.update_date='' and t2.create_date='' then '2017-08-03' else t2.update_date end                            
                                 ,' '
                                 ,case when t2.update_time='' and t2.create_time<>'' then t2.create_time 
                                       when t2.update_time='' and t2.create_time='' then '00:00:00' else t2.update_time end ))) as max_time --为每个手机号，创建一个时间戳，找出最大的时间戳，也就是最新出现的手机号
                            from (select serial_number 
                                  ,update_date
                                  ,update_time
                                  ,create_date
                                  ,create_time
                                  from pmrt.tmrt_distinct_bus_user_info
                                  where deal_date='${v_thisyyyymmdd}'
                                union all  --将两表组合起来
                                  select serial_number 
                                  ,update_date
                                  ,update_time
                                  ,create_date
                                  ,create_time
                                  from pmrt.tmrt_update_mobile_user_total t
                                  where t.deal_date='${v_dealyyyymmdd}') t2
                            group by t2.serial_number) a2
                            on c1.serial_number=a2.serial_number
                            and c1.update_time2=a2.max_time --使用手机号与时间戳来关联，找出最新的那个手机号的那条记录
group  by c1.serial_number
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
echo "hive error" 2>&1
exit 1
fi
echo "script success"
