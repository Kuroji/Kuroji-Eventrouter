FROM gradle:jdk8 as builder

WORKDIR /home/gradle/project

COPY . .

WORKDIR ./Kuroji-Eventrouter-Server

RUN gradle shadowJar

FROM openjdk:8-jre-alpine

WORKDIR /app

COPY --from=builder /home/gradle/project/Kuroji-Eventrouter-Server/build/libs/kuroji-eventrouter-server-*-all.jar kuroji-eventrouter-server.jar

ENTRYPOINT ["java", "-jar", "kuroji-eventrouter-server.jar"]
