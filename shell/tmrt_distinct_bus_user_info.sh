#!/bin/sh
source /e3base/.bash_profile
##################################################################
# 程序名称：日新增登录客户包含更新明细
# 程序功能：
# 创建信息：2017-03-30  by huangkr
# 修改信息：2017-08-16  by huangkr
# 修改信息：2018-10-17  by zhaojl_bds  修改了sql结构，方便阅读，逻辑不变
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

echo $v_thisyyyymmdd
year=${v_thisyyyymmdd:0:4}
echo $year
month=${v_thisyyyymmdd:4:2}
echo $month
day=${v_thisyyyymmdd:6:2}
echo $day
sql="
insert overwrite table pmrt.tmrt_distinct_bus_user_info partition (deal_date='${v_thisyyyymmdd}')
select a1.serial_number
,max(a1.imei)
,max(a1.channel_code)
,max(a1.sys_plat)
,max(a1.mbosvesrion)
,max(a1.mb_type_info)
,max(a1.scr_pix)
,max(a1.client_ver)
,max(a1.create_date)
,max(a1.create_time)
,max(a1.update_date)
,max(a1.update_time)
,max(a1.last_channel_code)
,max(a1.xk)
,max(a1.last_login_time)
,max(nvl(a3.prov_code,'未知'))
,max(nvl(a3.city_code,'未知'))
,max(nvl(a3.city_name,'未知'))
,''
,''
,''
,''
,''
from (select serial_number
      ,imei
      ,channel_code
      ,sys_plat
      ,mbosvesrion
      ,mb_type_info
      ,scr_pix 
      ,client_ver
      ,create_date
      ,create_time  ---20170801 用户属性表更改字段 
      ,update_date
      ,update_time
      ,last_channel_code
      ,xk
      ,last_login_time
      ,unix_timestamp(concat(nvl(update_date,nvl(create_date,'${v_thisyyyy}-${v_thismm}-${v_thisdd}'))
                      ,' '
                      ,substring(nvl(update_time,nvl(create_time,'00:00:00')),1,8))) as update_time2
      from pods.bus_user_info_new
      where year=${v_thisyyyy} and month=${v_thismm} and day=${v_thisdd} ) a1
          inner join (select serial_number
                      ,max(unix_timestamp(concat(nvl(t2.update_date,nvl(t2.create_time,'${v_thisyyyy}-${v_thismm}-${v_thisdd}'))
                                          ,' '
                                          ,substring(nvl(t2.update_time,nvl(t2.create_time,'00:00:00')),1,8)))) as max_time
                      from pods.bus_user_info_new t2
                      where t2.year=${v_thisyyyy} and t2.month=${v_thismm} and t2.day=${v_thisdd}
                      group by t2.serial_number) a2
                      on a1.serial_number=a2.serial_number
                      and a1.update_time2=a2.max_time
          left join (select t.prov_code
                     ,t.city_code
                     ,t.city_name
                     ,t.section
                     from pods.MNG_AREANUM_INFO t
                     where substring(regexp_replace(t.expiry_date, '-', ''),1,8)>${v_thisyyyymmdd}) a3
                     on substring(a1.serial_number,1,7)=a3.section 
group by a1.serial_number
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
