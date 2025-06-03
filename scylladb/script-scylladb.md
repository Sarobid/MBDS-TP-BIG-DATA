# MBDS-TP-BIG-DATA

## 1. Démarrage de Hadoop

```sh
start-dfs.sh
```

## 2. Lancement de Hive

```sh
nohup hive --service metastore > /dev/null &
nohup hiveserver2 > /dev/null &
```
Vérification :
```sh
ps aux | grep hive
```

## 3. Installation et lancement de ScyllaDB
- Installer docker
  ```sh
  sudo dnf install -y dnf-utils device-mapper-persistent-data lvm2

  sudo dnf config-manager --add-repo=https://download.docker.com/linux/centos/docker-ce.repo

  sudo dnf install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

  sudo systemctl start docker

  sudo systemctl enable docker

  sudo usermod -aG docker vagrant
  ```
- Quitter et refaire vagrant ssh
- Installer ScyllaDB via Docker :
  ```sh
  docker pull scylladb/scylla:6.0.3
  ```
  ```sh
  docker run --name nodeX -p 9042:9042 -d scylladb/scylla:6.0.3 --smp 1
  ```
- Accéder au terminal ScyllaDB :
  ```sh
  docker exec -it nodeX cqlsh
  ```

## 4. Création de la table dans ScyllaDB

```sql
CREATE KEYSPACE climate_policies WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
CREATE TABLE climate_policies.policies (
    "Index" int,
    Policy_raw text,
    Year int,
    IPCC_Region text,
    Policy_Type text,
    sector text,
    bm25_score_first float,
    PRIMARY KEY (IPCC_Region, Year, "Index")
);
```

## 5. Importation des données dans ScyllaDB

- Installer Maven :
  ```sh
  sudo dnf install maven
  ```
- Compiler et exécuter le projet Java :
  ```sh
  cd scylladb-import
  ```
-  mettre le policy.csv dans le même répertoire que App.java

  ```url
  https://drive.google.com/file/d/1_8rjwqOTjnLRcz-vinVM-wUO7C-AxiOf/view?usp=sharing
  ```
  ```sh
  mvn clean install
  mvn clean compile
  mvn exec:java -Dexec.mainClass="com.example.App"
  ```
- Vérifier l'import :
  ```sql
  select IPCC_Region from climate_policies.policies limit 10;
  ```

## 6. Transfert de ScyllaDB vers Hive via Spark

- Aller dans le dossier :
  ```sh
  cd '.\script scylla to hive\'
  ```
- Lancer le script Spark :
  ```sh
  spark-submit \
    --packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 \
    scylla_to_hive.py
  ```

## 7. Connexion à Hive

- Se connecter avec Beeline :
  ```sh
  beeline
  !connect jdbc:hive2://localhost:10000
  ```
  Utilisateur : `oracle`  
  Mot de passe : `welcome1`

## 8. Création de la table externe Hive

```sql
CREATE EXTERNAL TABLE climate_policies_hive (
  ipcc_region STRING,
  year INT,
  Index INT,
  bm25_score_first DOUBLE,
  policy_raw STRING,
  policy_type STRING,
  sector STRING
)
STORED AS PARQUET
LOCATION 'hdfs:/user/vagrant/climate_policies';
```

- Vérifier l'import :
  ```sql
  SELECT * FROM climate_policies_hive LIMIT 10;
  ```
