# DataEngProject
To see our presentation :
> [go to canva](https://www.canva.com/design/DAFSm2cQkeI/NIA07S4kV_CDY5WR32taIg/view?utm_content=DAFSm2cQkeI&utm_campaign=designshare&utm_medium=link2&utm_source=sharebutton)

To download the project :
> https://github.com/Moebius2107/DataEngProject

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
Once the folders are deleted, we will recreate them with the "download_XX_url_content" tasks. The data is stored on DropBox. To be able to access them we have the link described in the presentation but it is not enough to download the different files: we have to create a Dropbox application and obtain an access token. Once the token is obtained, we can use it to connect to the DropBox api in our tasks. 

After having inconsistencies in our data we realized that we where taking only the first part of a paginated answer in the dropbox API call used to get the links, that is why we used the .continue functionality.

We get in our dag/data folder our files in JSON format. Once the download tasks are finished, we send the data to MongoDB.

The data is stored in JSON-like documents, which allows us not to have too much change from its initial state. Moreover, since we do not perform any processing during this step, the fact that MongoDB does not provide a schema isn't a problem for us.

![Ingestion Dag](/img/ingestion_dag.PNG)

## 2. Staging area

Before we start processing the data, we check if the database and the collections have been created with the "check_db_existence" task.

Once the data is loaded properly, we use Pandas to clean and transform it. Pandas is specifically designed for data manipulation and analysis in Python. One of the best advantages of Pandas is it needs less writing for more work done. What would have taken multiple lines in Python without any support libraries, can simply be achieved through 1-2 lines with the use of Pandas. Thus, using Pandas helps to shorten the procedure of handling data. Also Pandas can import large amounts of data very fast which saves time.

We also use Jupyter Notebook as a debugger in our project. We use it to visualize the processing we do on the data, which allows us to see the commands that do not give the desired results. The 'Staging_notebook' has all the applied steps with all the prints we used to realize which transformations to make to each of our datasets.

We start by transforming the documents into DataFrame to manipulate them more easily. Then the data underwent several transformations in order to become usable.

![Staging Dag](/img/staging_dag.PNG)

### A. Removing duplicates
With the describe() function, we can see how many rows the DataFrame has and how many values are unique for each column in it. Identifiers where the columns we used to deleted the duplicates.
### B. Deleting the unuseful columns
We will then delete the columns that contain information that will not be useful in our analysis with the drop() function.
### C. Null/empty management
There are many null values in the data we are retrieving. We keep the rows with a null on non-essential columns to the correctness of our data.
### D. Split columns
We have a column with a full adress (Street, city, Postal code, Country), we want in our case to study the country separately, so we splited the column to have that field separated thanks to pandas rsplit function.
### E. Unpivot columns
We had a column per participant in a team project. As we want to work with participants separately, we have to unpivot them. This change allow to be able to to join participants and projects afterwards.

## 3. Production Data and answer to the questions
We could not test the Production Dag because we had an Docker/Airflow issue where we could not see and execute dags in the last 36 hours before the project deadline.
That is why we also include an .sql file that we tested and we know it works to create and populate all the tables described below. It is located in dags/sql folder.

We stored our production data in a PostgreSQL Database with a Star Schema.

We took the csv file produced after the staging data and we imported it to PostgreSQL. With that imported table we could create the 3 dimensions we are going to use in our case:

-Hackatons Dimension:

![Dim Hackatons](/img/dim_hackatons.png)

-Participants Dimension:

![Dim Participants](/img/dim_participants.PNG)

Projects Dimension:

![Dim Projects](/img/dim_projects.png)

And then with the identifiers of our dimension tables we created our facts table:

![Fact Table](/img/facts.png)


We answered to the questions thanks to our production data linked to a Power BI report (we took screenshots of the results obtained).

Which hackaton has the highest average skill level of the participants?

![Average skills](/img/skill_average.png)

Which are the top 10 states (in order) with the most participants in hackatons?

![Top 10 States](/img/top_10_states.png)


## 4. What can be improved

We had some technical issues with Docker/Airflow (on the last 2 days airflow UI did not work for us and we could not resolve it) and lack of communication issues. 

With more time we could improve for example the dimensions of our production data to be SCD type 2.
Also we could have written a better documentation, with more explanations of the decisions and the applied steps. 
