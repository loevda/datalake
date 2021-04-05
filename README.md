# DATA LAKE with Spark
***
### How to run the Project
Create <b><i>dl.cfg</i></b> file into the project root path wih the following values:

<b>[AWS]</b><br />
AWS_ACCESS_KEY_ID=\<YOUR AWS ACCESS KEY WITHOUT QUOTES>
<br />AWS_SECRET_ACCESS_KEY=\<YOUR AWS SECRET EY WIHTOUT QUOTES>
<br />AWS_S3_OUTPUT_BUCKET=\<YOUR AWS S3 OUTPUT BUCKET WITHOUT QUOTES> 

Run the <b><i>etl.py</i></b> with the following command:

``python etl.py``