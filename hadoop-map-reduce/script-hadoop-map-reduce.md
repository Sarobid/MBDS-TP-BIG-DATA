# SCRIPT PROGRAMME HADOOP MAPREDUCE

## Intégration du fichier `city_temperature.csv` en local (Vagrant)
Téléchargez le dataset : [Kaggle - Daily Temperature of Major Cities](https://www.kaggle.com/datasets/sudalairajkumar/daily-temperature-of-major-cities)

1. Extraire le fichier `city_temperature.csv`.
2. Copiez-le dans un dossier `data-temperature` dans le dossier Vagrant.

## Intégration du fichier `city_temperature.csv` dans HDFS

### Démarrer Hadoop HDFS :
```bash
start-dfs.sh
start-yarn.sh
```

### Créer un dossier `temperature` dans HDFS :
```bash
hdfs dfs -mkdir /temperature
```

### Ajouter le fichier `city_temperature.csv` dans HDFS :
```bash
hdfs dfs -put /vagrant/data-temperature/city_temperature.csv /temperature
```

## Compilation et création du programme MapReduce

Placez-vous dans le dossier contenant `Temperature.java`.

### Compilation :
```bash
javac -classpath $(hadoop classpath) -d . Temperature.java
```

### Création du fichier JAR :
```bash
jar cf temperature.jar hadoop/Temperature*.class
```

## Exécution du programme MapReduce

Pour lancer le programme MapReduce, utilisez la commande suivante :
```bash
hadoop jar temperature.jar hadoop.Temperature /temperature/city_temperature.csv resultat
```

## Vérification du résultat du programme MapReduce
```bash
hadoop fs -cat resultat/*
```

## Chargement du résultat dans Hive

### Lancer Hive :
```bash
beeline
```

### Se connecter à Hive :
```bash
!connect jdbc:hive2://localhost:10000
```

### Création d’une table externe dans Hive

Cette table pointe vers le résultat de notre programme Hadoop MapReduce :
```sql
CREATE EXTERNAL TABLE TEMPERATURE_H_EXT (
    region STRING,
    country STRING,
    year INT,
    avgTemperature DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'resultat'
TBLPROPERTIES ('skip.header.line.count'='1');
```

### Vérification des données
```sql
SELECT * FROM TEMPERATURE_H_EXT LIMIT 10;
```

---
