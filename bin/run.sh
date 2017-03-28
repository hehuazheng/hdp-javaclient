#!/bin/bash
if [ $# != 2 ]; then
  echo "usage localPath remotePath"
  exit 1
fi
workDir=`dirname $0`
libPath=""
for i in `ls $workDir/lib`; do
  libPath="$libPath:$workDir/lib/$i"
done
libPath=${libPath:1}
echo "process $*"
/usr/local/java8/bin/java -cp $libPath com.secoo.bigdata.UploadFileToHdfs $workDir/nn $*