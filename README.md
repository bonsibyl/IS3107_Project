# IS3107 Project: Spotified

# Background

Spotify is one of the top players in music streaming services. With over 500 million users on its platform listening to music, discovering new music, and curating playlists, it has become something of a necessity in the modern day (Spotify, n.d.). Spotify prides itself on its hyper-personalisation of services, which recommends users songs that align with their listening preferences. Another more recent campaign, Spotify Wrapped, summarises users’ listening data with fun and interactive visualisations.

## Problem Statement

Although Spotify has developed features aimed at providing a more personalised and engaging listening experience, users have expressed their dissatisfaction regarding its usefulness, which leaves much room for improvement.

At present, there is no feature within Spotify that allows users to view their past listening statistics in real-time. Users are only able to view this data via the annually-recurring Spotify Wrapped. Additionally, the granularity of the data is low as it only covers basic statistics such as listening duration and the number of plays.

Another problem our project hopes to address is to provide more relevant and fresh music recommendations to users. Spotify’s recommendations come in many forms: Radio, Discover Weekly, Daily Mixes; but some common user complaints concerning them include (Reddit, ):
Repeatedly receiving song recommendations that a user has listened to before, making it difficult to discover new music.
Receiving irrelevant recommendations that do not reflect a user’s music preferences.
Receiving recommendations that were heavily skewed towards songs or artists that were already popular, making it difficult to discover underrated artists.

Overall, our project aims to address these issues and provide a user experience that is more personalised and engaging than what is currently available on Spotify.

# Table of Contents

1. [Technologies](#technologies)
2. [Setup](#setup)
3. [Contributors](#contributors)

# Technologies

- Stack

  - Machine Learning - Python
  - Workflow Orchestration - Apache Airflow
  - Data Lake/ Databse - AWS EC2, S3, RDS
  - User Interface - React JS

- Language

  - Python
  - SQL
  - React

- APIs
  - Spotify Developer API

# Setup

**If the links are not working, make sure that it is http and not https**

You may try out our dags through our AWS Hosted Airflow Instance at:
http://ec2-13-213-48-227.ap-southeast-1.compute.amazonaws.com:8080/

username: airflow
<br>
password: airflow

You may also try out our user GUI through our AWS hosted instance at:
http://ec2-18-139-116-71.ap-southeast-1.compute.amazonaws.com:3000/

username: test
<br>
password: test

You may paste your own **PUBLIC** spotify URL in the input box. Make sure it looks like this: https://open.spotify.com/playlist/7MFbySBZbklUth1B6MBCmF?si=a32e7513200e4ca0

**Do note that recommendations do not update immediately as the pipeline takes a few minutes to run but you can still watch the dag trigger in the airflow UI**

# Hosting Locally

In order to deploy the GUI on your localhost, cd into GUI/server and `npm install` followed by `node.index.js`
<br>
Afterwards, go into GUI/is3107gui and run `npm install` again followed by `npm start`

Airflow can be hosted locally as per the airflow installation instructions here: https://airflow.apache.org/docs/apache-airflow/stable/installation/index.html

Afterwards, do install the requirements in the `requirements.txt` folder using `pip install -r requirements.txt`. As packages update, this list is not exhaustive.

Move the dags in our repo to the dags folder in your airflow folder. Then, create a `data` folder in your airflow folder and place in data.csv files.

Also, you need to insert some .json files that contain required playlist data. Place A FEW, NOT ALL of the .json files that you can find in our google drive here: https://drive.google.com/drive/u/1/folders/1Ob0AE-J04RnXN0wMGRIVH_-srJTLBCd0

Lastly, make a `downloaded` folder in your airflow folder and leave it empty.

**Do note that due to differences in $AIRFLOW_HOME configurations for different machines, you may have to change some file paths within the airflow dags. The dags are written assuming $AIRFLOW_HOME=/home/airflow**

# Contributors

- Chelsea: https://github.com/chelseayap02
- Chris: https://github.com/chrischanwz
- Nathaniel: https://github.com/Nat-Han1999
- Qian Yi: https://github.com/qianyiloh
- Wei Yang: https://github.com/bonsibyl
