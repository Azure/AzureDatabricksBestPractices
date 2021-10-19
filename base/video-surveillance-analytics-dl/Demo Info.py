# Databricks notebook source
# MAGIC %md # CAVIAR Project Videos
# MAGIC 
# MAGIC Videos for the demo were obtained from http://groups.inf.ed.ac.uk/vision/CAVIAR/CAVIARDATA1/
# MAGIC   - Clips from INRIA (1st Set)

# COMMAND ----------

# MAGIC %md 
# MAGIC Videos were available in mpg format.
# MAGIC 
# MAGIC It's difficult to embed mpg videos in displayHTML() so I converted all the videos to mp4 with `ffmpeg`

# COMMAND ----------

# MAGIC %md `brew install ffmpeg`
# MAGIC 
# MAGIC ```
# MAGIC for x in *.MPG; do
# MAGIC     ffmpeg -i $x -strict experimental -f mp4 \
# MAGIC            -vcodec libx264 -acodec aac \
# MAGIC            -ab 160000 -ac 2 -preset slow \
# MAGIC            -crf 22 ${x/.MPG/.mp4};
# MAGIC done
# MAGIC ```

# COMMAND ----------

# MAGIC %md ## Library setup
# MAGIC 
# MAGIC Necessary libraries are already set up in the `/demos/Identifying Suspicious Behavior/` folder with the versions that were used originally.
# MAGIC 
# MAGIC 
# MAGIC - h5py
# MAGIC - imgscalr_lib.jar
# MAGIC - keras
# MAGIC - opencv-python
# MAGIC - spark-deep-learning
# MAGIC - tensorflow (needs to be version 1.4.1 with Spark DL 0.2.0)
# MAGIC - tensorframes (don't download the jar from spark packages directly, use the Library UI and pick tensorframes from there)

# COMMAND ----------

# MAGIC %md ## Demo Instructions
# MAGIC 
# MAGIC The main demo is in 2 notebooks:
# MAGIC - 01 - Data Ingest and Pre-Processing
# MAGIC - 02 - Model and Evaluation
# MAGIC 
# MAGIC There is a hidden cell in both notebooks at cmd #2. Before a demo, you will need to click on `show code` in the cell's drop down menu, run those cells, and then clear notebook results, and re-hide the cell.
# MAGIC 
# MAGIC These hidden cells are basically helper functions for displaying video or images in a notebook.
# MAGIC 
# MAGIC *Note*:
# MAGIC The videos I used for the model training are in mpg format, but it's hard to display mpg video files with HTML5 so I converted all the videos to mp4s just to display them in a notebook cell.
# MAGIC 
# MAGIC These mp4 videos are saved in the FileStore, and are used only with `displayVid()` to play the video in a notebook.  
# MAGIC 
# MAGIC The actual mpg files I used for training are in my S3 bucket at `/mnt/raela/cctvVideos/train/`. Please don't delete them :)  
# MAGIC The mp4 files I used for displaying are in the FileStore at `/FileStore/mnt/raela/cctvVideos/train/mp4/` and `/FileStore/mnt/raela/cctvVideos/test/mp4/`
# MAGIC 
# MAGIC Since I kept the original video names in every extracted jpg image, you can copy the string before frame123.jpg and paste that into displayVid.  
# MAGIC For eg:  
# MAGIC 
# MAGIC `dbfs:/mnt/raela/cctvFrames_test/LeftBag_AtChairframe0023.jpg` was predicted to be suspicious.  
# MAGIC To display the relevant video, copy "LeftBag_AtChair" and paste it into the displayVid() command.  
# MAGIC `displayVid()` should contain a helper string like `displayVid("/mnt/raela/cctvVideos/test/mp4/.mp4")`
# MAGIC 
# MAGIC 
# MAGIC The 2 Spark DL cells in notebook 01 does take some time to run (about 30 seconds each with 2 i3.xlarges). I usually clear notebook results and pre-run those 2 cells before a demo so I don't have to run those and wait for them to finish during a demo.

# COMMAND ----------

# MAGIC %fs ls /mnt/raela/cctvVideos/train/

# COMMAND ----------

# MAGIC %fs ls /FileStore/mnt/raela/cctvVideos/train/mp4/