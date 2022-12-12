FROM chenfengwei/jdk:openjdk1.8.0.01
VOLUME /tmp
ADD ./start/build/libs/app.jar app.jar
CMD hostname | xargs -I {} echo 127.0.0.1 {} >> /etc/hosts \
    && java -Djava.security.egd=file:/dev/./urandom \
    -Xms2000m -Xmx2000m -Xmn350m -Xss512k -XX:+UseConcMarkSweepGC -XX:MaxTenuringThreshold=5 -XX:+UseConcMarkSweepGC -jar app.jar