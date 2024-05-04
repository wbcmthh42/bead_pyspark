cd C:\Users\qsfun\Desktop\New_folder
set DOCKER_BUILDKIT=1
docker build -t pyspark-ge .
docker build --output . .