# Part 1: Creating and filling in the database

You need `Docker` to run the following scripts. Other requirements are seen in `requirements.txt`. cd to assignment2_2024 and run the following commands to create the database and populate its tables:

```
docker-compose up -d
docker-compose exec app python main.py
```

# Part 2: Querying the database

Stay in assignment2_2024 and use the following command, which also prints the result for each query:

```
docker-compose up -d
docker-compose exec app python part2.py
```

To access the mysql environment, in a separate terminal, run the following commands:
```
docker-compose up -d
docker-compose exec app mysql -p
```

Enter the mysql password, which is `group20`. Next, run the following SQL command to access the database:

```sql
USE geolife
```

# Connecting to Docker Container Shells

This guide provides instructions on how to connect to the shell of your Docker containers for both the `app` and `mysql` services.

## Prerequisites

- Ensure Docker and Docker Compose are installed on your machine.
- Ensure your Docker Compose services are running.

## Starting Docker Compose Services

First, start your Docker Compose services in detached mode:

```sh
docker-compose up -d

Connecting to the app Container Shell
To open a shell inside the app container, use the following command:

docker-compose exec app /bin/bash

This will open a Bash shell inside the app container, allowing you to run commands interactively.

Connecting to the mysql Container Shell
To open a shell inside the mysql container, use the following command:

docker-compose exec mysql /bin/bash

This will open a Bash shell inside the mysql container.

Accessing the MySQL Shell
Once you are inside the mysql container shell, you can access the MySQL shell by running:

mysql -u root -p

When prompted, enter the MySQL root password (e.g., group20)
```

