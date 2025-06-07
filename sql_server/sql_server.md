# Installation et Intégration de Sql Server avec Hive

## 1. Préparation du serveur
### Setup des fichiers nécessaires
- Copier le fichier **Tables.sql**  de ce dans un dossier nommé sql_server que créerez dans COURSBIDATA 

- Copier les drivers **mssql-jdbc-12.4.2.jre8.jar** et  **mysql-connector-j-8.0.33.jar** dans le dossier sql_server que vous avez créer précédemment

- Copier le fichier **sql_server_to_hive.py** dans le dossier sql_server

> Pour la suite des instructions se connecter sur vagrant

### Ajouter le dépôt Microsoft Sql server

```bash
sudo curl -o /etc/yum.repos.d/mssql-server.repo https://packages.microsoft.com/config/rhel/8/mssql-server-2022.repo
```

---

## 2. Installation de Sql Server

### Installer Sql Server
```bash
sudo dnf install -y mssql-server
```

### Configuration de Sql Server
```bash
sudo /opt/mssql/bin/mssql-conf setup
```
- Choisir l'option 2 pour option développeur

- Choisissez le mot de passe pour l'admin
> Vous devez choisir Admin_1_Admin 

### Activer le service Sql Server
```bash
sudo systemctl enable --now mssql-server
```

### Pour accéder via ligne de commande à la base de données
Il faut installer la commande sqlcml

- Ajouter le repository de Microsoft
```bash
sudo curl -o /etc/yum.repos.d/msprod.repo https://packages.microsoft.com/config/rhel/8/prod.repo
```

- Installer l'outil sqlcmd
```bash
sudo dnf install -y mssql-tools unixODBC-devel
```

- Ajouter sqlcmd au profil bash
```bash
echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bash_profile
source ~/.bash_profile
```

- Finalement se connecter sur la base de données
```bash
sqlcmd -S localhost -U SA -P 'Votre_mot_de_passe'
```

## 3. Création de la base de données, des tables et insetion des données sur Sql Server
- Connecter vous sur la base de données

- Créer la base de données
```bash
CREATE DATABASE NaturalCatastrophe;
GO
```

- Utiliser la base de données
```bash
USE NaturalCatastrophe;
GO
```

- Créer les tables et insérer les données
```bash
sqlcmd -S localhost -U SA -P 'Votre_mot_de_passe' -d NaturalCatastrophe -i /vagrant/sql_server/Tables.sql
```

## 3. Création de la base de données, des tables et insetion des données sur Sql Server
Avant de lancer le script python, s'assurer que Hive est lancé et opérationnel
- Créer la base de données **global_warming_db** sur Hive si elle n'existe pas encore

- Lancer le script python
```bash
spark-submit \
  --jars /vagrant/sql_server/mssql-jdbc-12.4.2.jre8.jar \
  --driver-class-path /vagrant/sql_server/mysql-connector-j-8.0.33.jar \
  /vagrant/sql_server/sql_server_to_hive.py
  ```