 ./mvnw -T8 clean install -DskipTests  -Dair.check.skip-checkstyle=true
cd ./core/docker
./build.sh -a amd64
