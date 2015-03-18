export JAVA_HOME=/ORA/dbs01/oracle/home/jdk1.7.0/bin

export OGGDIR=/ORA/dbs01/oracle/product/OGG12
export CLASSPATH=$OGGDIR/ggjava/ggjava.jar:ojdbc7.jar

echo $CLASSPATH

$JAVA_HOME/javac -cp $CLASSPATH com/oracle/gg/datapump/* 

$JAVA_HOME/jar cvf testhandler.jar com
