docker build -t clp-docker-image .
docker run --name postgresclp -p 5432:5432 clp-docker-image
