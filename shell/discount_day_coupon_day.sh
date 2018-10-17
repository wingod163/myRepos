#!/bin/bash
source /e3base/.bash_profile
#######################################
# title:  特惠日活动卡券数据
# author: zhaojl_bds
# date:   2018-09-26
# note:   特惠日活动日数据。
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



#所有脚本开始

#首先，将文件清空。
> /e3base/zjl/result/discount_day_coupon_day_${v_thisyyyymmdd}.txt

echo "#######################${v_thisyyyymmdd}特惠日活动卡券数据#######################" >> /e3base/zjl/result/discount_day_coupon_day_${v_thisyyyymmdd}.txt
echo "开始时间：startTime = "$startTime >> /e3base/zjl/result/discount_day_coupon_day_${v_thisyyyymmdd}.txt
echo "系统时间：systime = "$systime >> /e3base/zjl/result/discount_day_coupon_day_${v_thisyyyymmdd}.txt

echo "参数年月日：v_thisyyyymmdd = "${v_thisyyyymmdd} >> /e3base/zjl/result/discount_day_coupon_day_${v_thisyyyymmdd}.txt
echo "参数年月：v_thisyyyymm = "${v_thisyyyymm} >> /e3base/zjl/result/discount_day_coupon_day_${v_thisyyyymmdd}.txt
echo "参数年  ：v_thisyyyy = "${v_thisyyyy} >> /e3base/zjl/result/discount_day_coupon_day_${v_thisyyyymmdd}.txt
echo "参数月  ：v_thismm = "${v_thismm} >> /e3base/zjl/result/discount_day_coupon_day_${v_thisyyyymmdd}.txt
echo "参数日  ：v_thisdd = "${v_thisdd} >> /e3base/zjl/result/discount_day_coupon_day_${v_thisyyyymmdd}.txt


#*********************************************************************************************************************************
#拼手气红包数据
sql1="
select
'${v_thisyyyymmdd}',
case when t.pcard_id='1026379489971671040' and t.batch_id='1026379893727956992' then '三季度拼手气发红包-30MB流量兑换券'
     when t.pcard_id='1026378303671504896' and t.batch_id='1026379264544608256' then '三季度拼手气发红包-70MB流量兑换券' 
     when t.pcard_id='1026375404019126272' and t.batch_id='1026375872095064064' then '三季度拼手气发红包-5元话费加赠券'
     when t.pcard_id='1026373628213727232' and t.batch_id='1026374686516318208' then '三季度拼手气发红包-15元话费加赠券' 
     when t.pcard_id='1026369849854660608' and t.batch_id='1026372835116978176' then '三季度拼手气发红包-20元话费加赠券' end as category
,count(distinct case when regexp_replace(substr(t.BIND_TIME,1,10),'-','')='${v_thisyyyymmdd}' then t.PCARD_SN end) as bind_num  --发放量
,count(distinct case when regexp_replace(substr(t.USE_TIME,1,10),'-','')='${v_thisyyyymmdd}' and t.LIFECYCLE_ST='10' then t.PCARD_SN end) as use_num  --使用量
from pods.Coupons_V2_TD_PCARD_INFO t
where t.yearstr=${v_thisyyyy} and t.monthstr=${v_thismm} and t.daystr=${v_thisdd}
and t.pcard_id in ('1026379489971671040','1026378303671504896','1026375404019126272','1026373628213727232','1026369849854660608')
and t.batch_id in ('1026379893727956992','1026379264544608256','1026375872095064064','1026374686516318208','1026372835116978176')
group by
case when t.pcard_id='1026379489971671040' and t.batch_id='1026379893727956992' then '三季度拼手气发红包-30MB流量兑换券'
     when t.pcard_id='1026378303671504896' and t.batch_id='1026379264544608256' then '三季度拼手气发红包-70MB流量兑换券' 
     when t.pcard_id='1026375404019126272' and t.batch_id='1026375872095064064' then '三季度拼手气发红包-5元话费加赠券'
     when t.pcard_id='1026373628213727232' and t.batch_id='1026374686516318208' then '三季度拼手气发红包-15元话费加赠券' 
     when t.pcard_id='1026369849854660608' and t.batch_id='1026372835116978176' then '三季度拼手气发红包-20元话费加赠券' end 
"

#执行
echo "${sql1}"
echo "***********************************************" >> /e3base/zjl/result/discount_day_coupon_day_${v_thisyyyymmdd}.txt
echo "拼手气红包" >> /e3base/zjl/result/discount_day_coupon_day_${v_thisyyyymmdd}.txt
hive -e "${sql1}" >> /e3base/zjl/result/discount_day_coupon_day_${v_thisyyyymmdd}.txt


#*********************************************************************************************************************************
#券心券意券有礼数据
if  [ ${v_thisdd} -ge 23 ] && [ ${v_thisdd} -le 25 ];then
		if [ ${v_thisdd} -eq 25 ];then 
		sql3="
		select
		'${v_thisyyyymmdd}',
		case when t.pcard_id='1025220235076177920' and t.batch_id='1029256201390657536' then '三季度特惠日-15元话费加赠券'
			 when t.pcard_id='1024936379903250432' and t.batch_id='1029261076929318912' then '三季度特惠日-50元话费加赠券' end as category --分类
		,count(distinct case when regexp_replace(substr(t.BIND_TIME,1,10),'-','')='${v_thisyyyymmdd}' then t.PCARD_SN end) as bind_num  --发放量
		,count(distinct case when regexp_replace(substr(t.USE_TIME,1,10),'-','')='${v_thisyyyymmdd}' and t.LIFECYCLE_ST='10'then t.PCARD_SN end) as use_num  --使用量
		from pods.Coupons_V2_TD_PCARD_INFO t
		where t.yearstr=${v_thisyyyy} and t.monthstr=${v_thismm} and t.daystr=${v_thisdd}
		and t.pcard_id in ('1025220235076177920','1024936379903250432') 
		and t.batch_id in ('1029256201390657536','1029261076929318912')
		group by
		case when t.pcard_id='1025220235076177920' and t.batch_id='1029256201390657536' then '三季度特惠日-15元话费加赠券'
			 when t.pcard_id='1024936379903250432' and t.batch_id='1029261076929318912' then '三季度特惠日-50元话费加赠券' end 
		"

		echo "${sql3}"
		echo "***********************************************" >> /e3base/zjl/result/discount_day_coupon_day_${v_thisyyyymmdd}.txt
		echo "25日券心券意有礼" >> /e3base/zjl/result/discount_day_coupon_day_${v_thisyyyymmdd}.txt
		hive -e "${sql3}" >> /e3base/zjl/result/discount_day_coupon_day_${v_thisyyyymmdd}.txt


		else
		sql2="
		select
		'${v_thisyyyymmdd}',
		case when t.pcard_id='1025220235076177920' and t.batch_id='1025269227013148672' then '三季度特惠日-15元话费加赠券'
			 when t.pcard_id='1024936379903250432' and t.batch_id='1024944252335034368' then '三季度特惠日-50元话费加赠券' end as category --分类
		,count(distinct case when regexp_replace(substr(t.BIND_TIME,1,10),'-','')='${v_thisyyyymmdd}' then t.PCARD_SN end) as bind_num  --发放量
		,count(distinct case when regexp_replace(substr(t.USE_TIME,1,10),'-','')='${v_thisyyyymmdd}' and t.LIFECYCLE_ST='10'then t.PCARD_SN end) as use_num  --使用量
		from pods.Coupons_V2_TD_PCARD_INFO t
		where t.yearstr=${v_thisyyyy} and t.monthstr=${v_thismm} and t.daystr=${v_thisdd}
		and t.pcard_id in ('1025220235076177920','1024936379903250432') 
		and t.batch_id in ('1025269227013148672','1024944252335034368')
		group by
		case when t.pcard_id='1025220235076177920' and t.batch_id='1025269227013148672' then '三季度特惠日-15元话费加赠券'
			 when t.pcard_id='1024936379903250432' and t.batch_id='1024944252335034368' then '三季度特惠日-50元话费加赠券' end 
		"

		echo "${sql2}"
		echo "***********************************************" >> /e3base/zjl/result/discount_day_coupon_day_${v_thisyyyymmdd}.txt
		echo "23-24日券心券意有礼" >> /e3base/zjl/result/discount_day_coupon_day_${v_thisyyyymmdd}.txt
		hive -e "${sql2}" >> /e3base/zjl/result/discount_day_coupon_day_${v_thisyyyymmdd}.txt

		fi
fi


echo "***********************************************" >> /e3base/zjl/result/discount_day_coupon_day_${v_thisyyyymmdd}.txt
echo "script successful" >> /e3base/zjl/result/discount_day_coupon_day_${v_thisyyyymmdd}.txt
echo "script successful"

