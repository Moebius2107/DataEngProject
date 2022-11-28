# DataEngProject
Subject : Hackaton (2018)

Presentation of the subject
For this project, we have retrieved datasets about the hackatons that took place in the US in may 2018, provided by Prof Alex Nolte. We will use 3 datasets containing information about: the [participants](https://www.dropbox.com/sh/4i4tp6y0kl2lk24/AACnkkHEropuFClu7XgbhPuja/participants?dl=0&subfolder_nav_tracking=1), the [projects](https://www.dropbox.com/sh/4i4tp6y0kl2lk24/AABMXKB4WetwcT_f1YoNtpbDa/projects?dl=0&subfolder_nav_tracking=1), and about the [hackaton](https://www.dropbox.com/sh/4i4tp6y0kl2lk24/AACsy_Ll8IgUjXujQSVR4KUIa/hackathons?dl=0&subfolder_nav_tracking=1) itself. With the recovered elements we will try to answer the following two questions in a user-friendly format:

  - Which hackaton has the highest average skill level of the participants?
               
  - Which are the top 10 states (in order) with the most participants in hackatons? 

To complete this data flow we will go through the following stages:

## 0. Coordonner les différentes tâches
Airflow is a dataflow orchestrator used to define data engineering workflows (DAGs describe how to run a data pipeline). It allows parallelize jobs, schedule them appropriately with dependencies. Nous allons donc l'utiliser pour notre projet.

## 1. Data selection and cleaning with MongoDB (ingeston data)
Nous souhaitons avoir un environnement propre pour commencer, c'est pourquoi la tâche "clean_folders" apparaît en premier : nous supprimons toutes les données qui auraient pu être téléchargé suite à des exécutions précédentes du DAG. 
Une fois les dossiers supprimés, nous allons les recréer avec les tâches "download_XX_url_content". Les données sont stockées sur DropBox. Pour pouvoir y accéder nous avons le lien décrit dans la présentation mais ce n'est pas suffisant pour télécharger les différents fichiers : il faut créer une application Dropbox et obtenir un access token. Une fois le token obtenu, nous pouvons l'utiliser pour se conneceter à l'api de DropBox dans nos tâches. Nous récupérons dans notre dossier dag/data nos fichiers sous format JSON. Nous pouvons voir qu'il y a un "dummy_node" après ces tâches, en effet, nous aurions pu juste l'enlever et mettre à la suite par exemple "download_hackaton_url_content" et "ingest_hackaton" mais nous avons choisi d'attendre que toutes les tâches de téléchargement soient finies avant de passer à la suite. Une fois les tâches de téléchargement finies, nous envoyons les données sur MongoDB. Les données sont stockés in JSON-like documents, ce qui permet de ne pas avoir trop de changement par rapport à leur état initial. De plus, comme nous ne procédons à aucun traitement durant cette étape, le fait que MongoDB ne provide pas de schéma est useful/handy.
![Ingestion Dag](/img/ingestion_dag.PNG)

## 2. Staging area
Avant de commmence le traitement des données
Une fois les données chargées, nous utilisons Pandas pour les nettoyer et les transformer. Pandas est spécifiquement conçue pour la manipulation et l’analyse de données en langage Python. Nous nous aidons de Jupyter Notebook pour visualiser les traitements que nous faisons dessus, ce qui 
Nous commençons par transformer les documents en DataFrame pour les manipuler plus facilement. Puis

![Staging Dag](./img/staging_dag.png)
## 3. Production Data and answer to the questions
        (To be completed)
