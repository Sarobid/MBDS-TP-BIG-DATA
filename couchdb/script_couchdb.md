# Installation et Intégration de CouchDB avec Hive

## 1. Préparation du serveur

### Installer les outils nécessaires
```bash
sudo yum install -y yum-utils
```

### Ajouter le dépôt CouchDB
```bash
sudo yum-config-manager --add-repo https://couchdb.apache.org/repo/couchdb.repo
```

### Ajouter le dépôt EPEL (Extra Packages for Enterprise Linux)
```bash
sudo yum install epel-release
```

---

## 2. Installation de CouchDB

### Installer CouchDB
```bash
sudo yum install -y couchdb
```

### Créer un compte administrateur
Éditer le fichier :
```bash
sudo nano /opt/couchdb/etc/local.ini
```
Dans la section `[admins]`, ajouter :
```
admin = root
```

### Lancer le service
```bash
sudo systemctl start couchdb
sudo systemctl restart couchdb
```

### Vérifier le bon fonctionnement
```bash
curl http://127.0.0.1:5984/
```

---

## 3. Création de la base de données CouchDB

### Créer la base
```bash
curl -X PUT http://admin:root@127.0.0.1:5984/energy_consumption
```

### Vérifier la création
```bash
curl -X GET http://admin:root@127.0.0.1:5984/energy_consumption
```

### Insérer des données
Assurez-vous que le fichier `data_energy.json` est placé dans le dossier `/vagrant/CouchDBData`.

```bash
curl -X POST http://admin:root@127.0.0.1:5984/energy_consumption/_bulk_docs \
  -H "Content-Type: application/json" \
  -d @/vagrant/CouchDBData/data_energy.json
```

---

## 4. Création d’un index pour les requêtes

```bash
curl -X POST http://localhost:5984/energy_consumption/_index \
  -u admin:root \
  -H "Content-Type: application/json" \
  -d '{
    "index": {
      "fields": ["country", "year"]
    },
    "name": "country-year-index",
    "type": "json"
}'
```

### Requête d'exemple
```bash
curl -X POST http://admin:root@127.0.0.1:5984/energy_consumption/_find \
  -H "Content-Type: application/json" \
  -d '{
    "selector": {
      "country": "Madagascar",
      "year": 2020
    }
}'
```

---

## 5. Intégration dans Hive avec Spark

### Pré-requis
- Apache Spark doit être installé dans l’environnement (déjà fait via Vagrant).
- Hive doit être installé et configuré.

### Création de la base et table Hive

```sql
CREATE DATABASE global_warming_db;
USE global_warming_db;

CREATE TABLE energy_consumptions (
  country STRING,
  region STRING,
  hydro_twh DOUBLE,
  solar_twh DOUBLE,
  wind_twh DOUBLE,
  coal_ton DOUBLE,
  gas_m3 DOUBLE,
  oil_m3 DOUBLE,
  nuclear_twh DOUBLE,
  gas_emissions_ton DOUBLE,
  population INT
)
PARTITIONED BY (year INT)
STORED AS PARQUET;
```

### Installer le module Python CouchDB
```bash
pip install couchdb
```

### Lancer le script ELT initial

Lancez le script `spark_batch_initial.py` pour charger tous les données déjà présentes dans CouchDB vers Hive :
```bash
spark-submit \
  --jars /usr/share/java/mysql-connector-java.jar \
  --driver-class-path /usr/share/java/mysql-connector-java.jar \
  /vagrant/couchDBData/spark_batch.py
```

Le script lit toutes les données de CouchDB, les nettoie, les transforme, puis les écrit dans la table Hive `energy_consumptions` partitionnée par année.

### Lancer l'ETL Spark Streaming

Lancez le script `spark_streaming_couchdb.py` pour charger les données en temps réel vers vers Hive :
```bash
spark-submit \
  --jars /usr/share/java/mysql-connector-java.jar \
  --driver-class-path /usr/share/java/mysql-connector-java.jar \
  /vagrant/couchDBData/spark_streaming_couchdb.py
```

---

## 6. Vérification dans Hive

Dans Hive CLI :
```sql
USE global_warming_db;
SELECT * FROM energy_consumptions LIMIT 10;
```
