#!/bin/bash
INSTALL_PATH=/opt/module

check_process(){
    pid=$(ps -ef 2>/dev/null | grep -v grep | grep -i $1 | awk '{print $2}')
    ppid=$(netstat -nltp 2>/dev/null | grep $2 | awk '{print $7}' | cut -d '/' -f 1)
    echo $pid
    [[ "$pid" =~ "$ppid" ]] && [ "$ppid" ] && return 0 || return 1
}

main(){
    usage="Usage: $0 (uuid|url|time|key|rank|behavior|userkey)"

    if [ $# -lt 1 ]; then
        echo $usage
        exit 1
    fi
    case $1 in
        uuid)
            spark-submit --master spark://hadoop000:7077 --class cn.un.hanlp.LogUuid /root/jars/spark.jar
            ;;
        url)
            spark-submit --master spark://hadoop000:7077 --class cn.un.hanlp.LogUrl /root/jars/spark.jar
            ;;
        time)
            spark-submit --master spark://hadoop000:7077 --class cn.un.hanlp.LogTime /root/jars/spark.jar
            ;;
        key)
            spark-submit --master spark://hadoop000:7077 --class cn.un.hanlp.LogKey /root/jars/spark.jar
            ;;
        rank)
            spark-submit --master spark://hadoop000:7077 --class cn.un.hanlp.LogRank /root/jars/spark.jar
            ;;
        behavior)
            spark-submit --master spark://hadoop000:7077 --class cn.un.hanlp.LogBehavior /root/jars/spark.jar
            ;;
        userkey)
            spark-submit --master spark://hadoop000:7077 --class cn.un.hanlp.LogUserKey /root/jars/spark.jar
            ;;
        all)
            spark-submit --master spark://hadoop000:7077 --class cn.un.hanlp.LogUUID /root/jars/spark.jar
            spark-submit --master spark://hadoop000:7077 --class cn.un.hanlp.LogUrl /root/jars/spark.jar
            spark-submit --master spark://hadoop000:7077 --class cn.un.hanlp.LogTime /root/jars/spark.jar
            spark-submit --master spark://hadoop000:7077 --class cn.un.hanlp.LogKey /root/jars/spark.jar
            spark-submit --master spark://hadoop000:7077 --class cn.un.hanlp.LogRank /root/jars/spark.jar
            spark-submit --master spark://hadoop000:7077 --class cn.un.hanlp.LogBehavior /root/jars/spark.jar
            spark-submit --master spark://hadoop000:7077 --class cn.un.hanlp.LogUserKey /root/jars/spark.jar
            ;;

        *)
            echo $usage
            ;;
    esac
}
main $1
