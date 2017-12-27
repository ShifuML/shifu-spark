#!/usr/bin/env bash

#compile the project
MAVEN=`command -v mvn`
if [ ${MAVEN} != "" ]; then 
    ${MAVEN} -DskipTests clean install
fi

SHIFU_PACKAGE_PATH=`find . -iname shifu*-hdp-yarn.tar.gz`

echo "${SHIFU_PACKAGE_PATH}"

SHIFU_PACKAGE_NAME=`basename "${SHIFU_PACKAGE_PATH}"`


if [ -f "${SHIFU_PACKAGE_PATH}" ]; then
   TARGET_PATH=`dirname "${SHIFU_PACKAGE_PATH}"`
   cd "${TARGET_PATH}"
   SHIFU_SPARK_JAR=`find . -iname shifu-spark*.jar | grep -v "source" | grep -v docs`
   if [ -f ${SHIF_SPARK_JAR} ]; then
        rm -rf tmp
        mkdir tmp
        tar -zxvf "${SHIFU_PACKAGE_NAME}" -C tmp
        SHIFU_TMP_DIR=`ls tmp`
        cp "${SHIFU_SPARK_JAR}" "tmp/${SHIFU_TMP_DIR}/lib/"
        tar -zcvf "${SHIFU_PACKAGE_NAME}" -C tmp .
   else 
        echo "Could not find shifu spark eval jar. Please build shifu spark before run this script."
   fi
else 
    echo "Could not find shifu package in target. Please build shifu spark before run this script"
fi
