# DataEngProject

To launch the project :
> Go to the DataEngProject folder
> Type in the terminal (PowerShell or other) :
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

## Coordinate the different tasks

Airflow is a dataflow orchestrator used to define data engineering workflows (DAGs describe how to run a data pipeline). It allows parallelize jobs, schedule them appropriately with dependencies. So we will use it for our project. As we want to clearly separate the different steps in the processing of our data, we have decided to have three files, one per DAG. We have also implemented a Master Dag that will trigger the next DAG once the previous one ends.

![Master Dag](/img/master_dag.PNG)

## 1. Data selection and cleaning with MongoDB (ingeston data)
We want to have a clean environment to start with, that's why the "clean_folders" task appears first: we delete all the data that could have been downloaded following previous executions of the DAG. 
Once the folders are deleted, we will recreate them with the "download_XX_url_content" tasks. The data is stored on DropBox. To be able to access them we have the link described in the presentation but it is not enough to download the different files: we have to create a Dropbox application and obtain an access token. Once the token is obtained, we can use it to connect to the DropBox api in our tasks. We get in our dag/data folder our files in JSON format. We can see that there is a "dummy_node" after these tasks, in fact, we could have just removed it and put after it for example "download_hackaton_url_content" and "ingest_hackaton" but we chose to wait until all the download tasks are finished before moving on. Once the download tasks are finished, we send the data to MongoDB. The data is stored in JSON-like documents, which allows us not to have too much change from its initial state. Moreover, since we do not perform any processing during this step, the fact that MongoDB does not provide a schema is useful/handy.

![Ingestion Dag](/img/ingestion_dag.PNG)

## 2. Staging area

Before we start processing the data, we check if the database and the collections have been created with the "check_db_existence" task.
Once the data is loaded properly, we use Pandas to clean and transform it. Pandas is specifically designed for data manipulation and analysis in Python. One of the best advantages of Pandas is it needs less writing for more work done. What would have taken multiple lines in Python without any support libraries, can simply be achieved through 1-2 lines with the use of Pandas. Thus, using Pandas helps to shorten the procedure of handling data. Also Pandas can import large amounts of data very fast which saves time.
We also use Jupyter Notebook as a debugger in our project. We use it to visualize the processing we do on the data, which allows us to see the commands that do not give the desired results.
We start by transforming the documents into DataFrame to manipulate them more easily. Then the data underwent several transformations in order to become usable.

![Staging Dag](/img/staging_dag.PNG)

### A. Removing duplicates
With the describe() function, we can see how many rows the DataFrame has and how many values are unique for each column in it. For example, we have 2000 participants but only 500 unique participant-ids, which implies that three-quarters of these values are identical and therefore useless.
Duplicate information](/img/duplicate_information.PNG)
So we will delete them.
### B. Deleting the unuseful columns
We will then delete the columns that contain information that will not be useful in our analysis with the drop() function.
### C. Null/empty management
There are many voids in the data we are retrieving, mainly in the columns we deleted in the previous step. The remaining ones also contain them and we have to manage them. The rows where the id, a reference to other tables or the location is missing are deleted because we do our analysis on them and we can't deduce their value from their neighbor. We keep the rows with a void on non-essential information but which may be interesting to study in another context for example.
### D. Split columns
We have a column with an adress, to analyse the country separately we splited the column to have that field separated.
### E. Unpivot columns
We had a column per participant in a project and we unpivot them to be able to join participants and projects afterwards.

## 3. Production Data and answer to the questions
We could not include the Production Dag because we had an Docker/Airflow issue where we could not see and execute dags in the last 36 hours before the project deadline.
We answered to the questions with our production data .csv file and Power BI tiles.
Which hackaton has the highest average skill level of the participants?
![Average skills](/img/skill_average.png)
Which are the top 10 states (in order) with the most participants in hackatons?
![Top 10 States](/img/top_10_states.png)


## 4. What can be improved

We had some technical and lack of communication issues. With more time we could improve for example the dimentions of our production data SCD type 2. Also we could have a better documentation of the work we did.  
