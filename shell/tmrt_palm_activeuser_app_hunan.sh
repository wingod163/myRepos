#!/bin/sh
source /e3base/.bash_profile
##################################################################
# 程序名称：胡雯--湖南手厅渠道分地市年累计活跃用户数需求
# 程序功能：
# 创建信息：2018-08-06  by zhaojl_bds
# 修改信息：
##################################################################

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

#定义异常方法
function execption()
{
if [ "$?" -ne 0 ]
then
sed_sql=`echo $sql | sed  's/\t/ /g' `
hive -e "insert into table pods.p_log
         select \"$pname\",\"$sed_sql\",\"$systime\",\"$v_num\"
         from pods.dual;"
echo "hive error" 2>&1
exit 1
fi
}

#脚本开始
##########################################################################################################
#创建分区表，存储手厅渠道分月的活跃用户数。
#create table if not exists pmrt.tmrt_palm_activeuser_per_mon
#(
#phone string
#,prov_code string
#,city_code string
#,city_name string
#)
#partitioned by (year string,month string)
#row format delimited
#fields terminated by '\t'
#stored as textfile;
#

#将手厅表中的手机号按月循环的加入到此表中

sql1="
insert overwrite table pmrt.tmrt_palm_activeuser_per_mon partition (year=${v_thisyyyy},month=${v_thismm})
select t1.serial_number
,t2.prov_code
,t2.city_code
,t2.city_name
from (select serial_number
      from pods.BOS_LOGIN_DTL_NEW
      where year=${v_thisyyyy} and month=${v_thismm}
      and serial_number not like '%@%'
      and length(serial_number)=11
      and serial_number rlike '^\\\d+$') t1
      inner join (select section
                  ,prov_code
                  ,city_code
                  ,city_name
                  from pods.MNG_AREANUM_INFO
                  where substring(regexp_replace(expiry_date, '-', ''),1,8)>${v_thisyyyymmfirst}
                  and prov_code='731') t2
                  on substr(t1.serial_number,1,7)=t2.section
group by t1.serial_number
,t2.prov_code
,t2.city_code
,t2.city_name
"

echo "${sql1}"
hive -e "${sql1}"
execption;



#湖南移动三大入口用户规模指标-手厅
sql2="
select t.city_name
,count(distinct t.phone)
from (select phone
      ,prov_code
      ,city_code
      ,city_name
      from  pmrt.tmrt_palm_activeuser_per_mon
      where year=${v_thisyyyy} and (month>=01 and month<=${v_thismm})
      group by phone
      ,prov_code
      ,city_code
      ,city_name) t
group by t.city_name
"

echo ${sql2}
echo "----------------------------------------------------------------" >> /e3base/zjl/result/tmrt_palm_activeuser_per_mon.txt
echo "${v_thisyyyymm} 湖南移动三大入口用户规模指标-手厅" >> /e3base/zjl/result/tmrt_palm_activeuser_per_mon.txt
/e3base/hive/bin/hive -e "${sql2}"  >> /e3base/zjl/result/tmrt_palm_activeuser_per_mon.txt
execption;


#结束
endTime=`date  +%H:%M:%S`
sT=`date +%s -d$startTime`
eT=`date +%s -d$endTime`
let useTime=`expr $eT - $sT`
echo "
Run `basename $0` ok !
        startTime = $startTime
          endTime = $endTime
          useTime = $useTime (s)!"
echo "script successful"

