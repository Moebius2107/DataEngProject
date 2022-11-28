# DataEngProject

Pour lancer le projet :
> Se placer dans le dossier DataEngProject
> Taper dans le terminal (PowerShell ou autre) :
```
docker compose up
```
> Aller sur http://localhost:8080/home

## Subject : Hackaton (2018)

Presentation of the subject
For this project, we have retrieved datasets about the hackatons that took place in the US in may 2018, provided by Prof Alex Nolte. We will use 3 datasets containing information about: the [participants](https://www.dropbox.com/sh/4i4tp6y0kl2lk24/AACnkkHEropuFClu7XgbhPuja/participants?dl=0&subfolder_nav_tracking=1), the [projects](https://www.dropbox.com/sh/4i4tp6y0kl2lk24/AABMXKB4WetwcT_f1YoNtpbDa/projects?dl=0&subfolder_nav_tracking=1), and about the [hackaton](https://www.dropbox.com/sh/4i4tp6y0kl2lk24/AACsy_Ll8IgUjXujQSVR4KUIa/hackathons?dl=0&subfolder_nav_tracking=1) itself. With the recovered elements we will try to answer the following two questions in a user-friendly format:

  - Which hackaton has the highest average skill level of the participants?
               
  - Which are the top 10 states (in order) with the most participants in hackatons? 

To complete this data flow we will go through the following stages:

## 0. Coordonner les différentes tâches
Airflow is a dataflow orchestrator used to define data engineering workflows (DAGs describe how to run a data pipeline). It allows parallelize jobs, schedule them appropriately with dependencies. Nous allons donc l'utiliser pour notre projet. Comme nous souhaitons séparer clairement les différentes étapes dans le traitement de nos données, nous avons décidé d'avoir trois fichiers, un par DAG. Nous avons aussi implémenté un Master Dag qui va trigger le prochain DAG une fois que le précédent se termine.
![Master Dag](/img/master_dag.PNG)

## 1. Data selection and cleaning with MongoDB (ingeston data)
Nous souhaitons avoir un environnement propre pour commencer, c'est pourquoi la tâche "clean_folders" apparaît en premier : nous supprimons toutes les données qui auraient pu être téléchargé suite à des exécutions précédentes du DAG. 
Une fois les dossiers supprimés, nous allons les recréer avec les tâches "download_XX_url_content". Les données sont stockées sur DropBox. Pour pouvoir y accéder nous avons le lien décrit dans la présentation mais ce n'est pas suffisant pour télécharger les différents fichiers : il faut créer une application Dropbox et obtenir un access token. Une fois le token obtenu, nous pouvons l'utiliser pour se conneceter à l'api de DropBox dans nos tâches. Nous récupérons dans notre dossier dag/data nos fichiers sous format JSON. Nous pouvons voir qu'il y a un "dummy_node" après ces tâches, en effet, nous aurions pu juste l'enlever et mettre à la suite par exemple "download_hackaton_url_content" et "ingest_hackaton" mais nous avons choisi d'attendre que toutes les tâches de téléchargement soient finies avant de passer à la suite. Une fois les tâches de téléchargement finies, nous envoyons les données sur MongoDB. Les données sont stockés in JSON-like documents, ce qui permet de ne pas avoir trop de changement par rapport à leur état initial. De plus, comme nous ne procédons à aucun traitement durant cette étape, le fait que MongoDB ne provide pas de schéma est useful/handy.
![Ingestion Dag](/img/ingestion_dag.PNG)

## 2. Staging area
Avant de commmence le traitement des données, nous vérifions si la base de données et les collections ont été créées avec la tâche "check_db_existence".
Une fois les données bien chargées, nous utilisons Pandas pour les nettoyer et les transformer. Pandas est spécifiquement conçue pour la manipulation et l’analyse de données en langage Python. One of the best advantages of Pandas is it needs less writing for more work done. What would have taken multiple lines in Python without any support libraries, can simply be achieved through 1-2 lines with the use of Pandas. Thus, using Pandas helps to shorten the procedure of handling data. Also Pandas can import large amounts of data very fast ce qui permet de faire des économies de temps.
Nous nous aidons aussi de Jupyter Notebook qui fait office de debugger dans notre projet. Nous l'utilisons pour visualiser les traitements que nous faisons sur les données, ce qui permet de voir les commandes qui ne donnent pas les résultats voulues.
Nous commençons par transformer les documents en DataFrame pour les manipuler plus facilement. Puis les données ont subi plusieurs transformations afin de devenir exploitable.
![Staging Dag](/img/staging_dag.PNG)
### A. Suppression des doublons
Avec la fonction describe(), nous pouvons voir combien de lignes comptent le DataFrame et combien de valeurs sont uniques pour chaque colonne dedans. Par exemple, nous avons 2000 participants mais seulement 500 participants-id uniques, ce qui implique que trois-quart de ces valeurs sont identiques et donc inutiles.
![Duplicate information](/img/duplicate_information.PNG)
Nous allons donc les supprimer.
### B. Suppression des colonnes
Nous allons ensuite supprimer les colonnes qui contiennent des informations qui ne seront pas utiles dans notre analyse avec la fonction drop().
### C. Gestion des null/vide
Il y a de nombreux vides dans les données que nous récupérons, essentiellement dans les colonnes que nous avons supprimées dans l'étape précédente. Celles qui restent en contiennent aussi et nous devons les gérer. Les lignes où il manque l'id, une référence aux autres tables ou la localisation sont supprimées car nous faisons nos analyses dessus et nous ne pouvons pas déduire leur valeur depuis leur voisin. Nous gardons les lignes ayant un vide sur les informations non essentielles mais qui peuvent être intéressantes à étudier dans un autre contexte par exemple.



## 3. Production Data and answer to the questions
La dernière étape consiste à exploiter les données.


## 4. What can be improved
Comme nous avons eu du mal à avancer tout au long du projet, certains parties auraient pu être améliorer si nous avions eu plus de temps. Par exemple, nous pouvons varier les sources pour obtenir des données plus récentes, celles que nous utilisons datant de 2018. 