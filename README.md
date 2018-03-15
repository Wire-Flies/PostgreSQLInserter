# How to run

Remember to run `yarn` in the "root" of the project to install dependencies.

In order to run you type `node user=POSTGRES\_USER password=POSTGRES\_PASSWORD host=POSTGRES\_HOST port=POSTGRES\_PORT database=POSTGRES\_DATABASE flightPath=FOLDER\_WHERE\_FLIGHTRADAR\_DATA\_IS`.

If postgres is newly installed you are going to have to create a postgres user. This is done inside the postgres cli/shell (psql). A user is called Role in postgres. Google how to create postgres role for documentation of how to do it (you will create the password at the same time that you create the role). `POSTGRES\_USER` and `POSTGRES\_PASSWORD` are going to be the created role and its corresponding password.

The `POSTGRES\_HOST` is probably going to be localhost
The `POSTGRES\_PORT` is probably 5432 (unless someone changed the default port that postgres listens at).

Exampe of how I would run the scripts on my computer:
`node app.js user=wireflies password=wireflies host=localhost port=5432 database=wireflies flightPath=~/school/tmp/secure.flightradar24.com/stockholm_1000km`
