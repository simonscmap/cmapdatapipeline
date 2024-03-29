# CMAP ETL Pipeline

## Environment Setup
Make sure unixodbc driver is already installed on your machine. On linux use the following command:

```console
$ apt-get install -y unixodbc unixodbc-dev freetds-dev freetds-bin tdsodbc
```

Then create the `etl` conda environment using the `etl.yml` file:

```console
$ conda env create --file etl.yml
```

Verify that the environment was installed correctly:

```console
$ conda env list
```

Finally, activate the environment:

```console
$ conda activate etl
```


