Ce projet vise à explorer et analyser les données d'Uber en utilisant les fonctionnalités puissantes de Google Cloud Platform (GCP) en conjonction avec MAGE, un outil robuste de pipeline. L'objectif principal était de créer un processus efficace pour extraire, transformer et charger (ETL) les données d'Uber depuis un bucket GCP, puis d'effectuer des analyses exploitables.
    Étapes clés du projet :
       1. Extraction des données Uber sur GCP

            Les données provenant de Uber ont été exportées et stockées sur un bucket dans Google Cloud Platform. Cette étape a permis de centraliser les informations à analyser.
       
       2. Mise en place d'une pipeline avec MAGE

            L'outil MAGE a été utilisé pour créer une pipeline efficace permettant de récupérer les données depuis le bucket GCP, de les transformer selon les besoins spécifiques du projet, et enfin de les charger dans le service BigQuery de GCP. Cette pipeline a été conçue pour automatiser le flux de données et garantir sa cohérence.
        
       3.Chargement des données dans BigQuery

            Les données transformées ont été chargées avec succès dans BigQuery, offrant ainsi une base de données prête à être explorée et analysée à l'aide de requêtes SQL et de diverses fonctions d'analyse de données.
       
       4. Création d'un vaste dataframe pour les analyses

            À partir des données stockées dans BigQuery, des requêtes ont été élaborées pour créer un grand dataframe en utilisant Python. Ce dataframe a été la fondation pour mener différentes analyses et tirer des insights significatifs.
Comment exécuter le projet :

    Prérequis :
        Accès à Google Cloud Platform.
        Installation de Python et des bibliothèques requises (pandas, etc.) pour l'analyse des données.

    Configurer l'environnement :
        Cloner ce dépôt.
        Assurez-vous d'avoir les autorisations d'accès nécessaires à GCP.

    Exécution du pipeline :
        Suivez les instructions dans le dossier 'pipeline' pour configurer et lancer la pipeline MAGE.

    Analyse des données :
        Explorez le script Python fourni dans le dossier 'analyses' pour accéder au dataframe créé et mener vos propres analyses.



