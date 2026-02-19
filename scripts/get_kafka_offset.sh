#!/bin/bash
# ./kafka-cat.sh core.hlsc.tls_take_course "2024-05-08 00:00:00"
if [[ -z $1 ]]; then
    echo "topic name is none"
    exit 1
fi

if [[ -n $2 ]]; then
    DATE_BEGIN=$(date -d "$2 1 day ago" -u "+%F %T")
    DATE_END=$(date -d "$2" -u "+%F %T")

    TIMESTAMP_BEGIN=$(date -d "$2 1 day ago" -u "+%s")
    TIMESTAMP_END=$(date -d "$2" -u "+%s")

    OFFSET_BEGIN=$(kcat -Q -b 172.30.1.99 -t $1:0:$TIMESTAMP_BEGIN"000" | awk '{print $4}')
    if [[ $TIMESTAMP_END -ge $(date -d now -u "+%s") ]]; then
        OFFSET_END=$(kcat -Q -b 172.30.1.99 -t $1:0:-1 | awk '{print $4}')
    else
        OFFSET_END=$(kcat -Q -b 172.30.1.99 -t $1:0:$TIMESTAMP_END"000" | awk '{print $4}')
    fi

else
    DATE_BEGIN=$(date -d "1 day ago" -u "+%F %T")
    DATE_END=$(date -d now -u "+%F %T")

    TIMESTAMP_BEGIN=$(date -d "1 day ago" -u "+%s")
    TIMESTAMP_END=$(date -d now -u +%s)

    OFFSET_BEGIN=$(kcat -Q -b 172.30.1.99 -t $1:0:$TIMESTAMP_BEGIN"000" | awk '{print $4}')
    OFFSET_END=$(kcat -Q -b 172.30.1.99 -t $1:0:-1 | awk '{print $4}')

fi
echo "BEGIN=$TIMESTAMP_BEGIN, $DATE_BEGIN"
echo "END  =$TIMESTAMP_END, $DATE_END"
COUNT=$(expr $OFFSET_END - $OFFSET_BEGIN)
echo "count = $(printf "%'.f" $COUNT)"
