- Run the docker compose
```shell
docker compose up -d
```

- Run pipenv
```shell
pipenv install
```

- Run the migration
```shell
pipenv run python run_migration.py
```

- Airflow interface is in [localhost](http://localhost:8080/)
    - user and psswd is `airflow`

- PGAdmin interface as well if preferred [localhost](http://localhost:5050/)

    - user `admin@example.com` psswd `admin` 


# on the approach

- I dediced to go with postgres for convenience. 
- Upsides
    - it's easy to create local env that would be similar to what you would have in the cloud
    - it's easy to automate 
    - with appropriate indexing and modeling, performance can be improved

- Downsides
    - for analytics, postgres is a bit more tricky.
    - it has cumbersome partitioning (a lot of manual stuff)
    - no support for keeping data ordered without rewriting whole table

- Data lake was organized as bronze and silver

![image](https://github.com/user-attachments/assets/a9ecd68a-0531-42a7-a33a-920ac5083cb2)

