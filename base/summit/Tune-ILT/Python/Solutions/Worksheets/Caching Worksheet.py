# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC <style>
# MAGIC   input { width: 10em }
# MAGIC </style>
# MAGIC 
# MAGIC <table>
# MAGIC   <tr style="background-color:#F0F0F0"><th colspan="2">What</th><th colspan="3">Value</th></tr>
# MAGIC   <tr>
# MAGIC     <td colspan="2">Storage Memory:</td>
# MAGIC     <td colspan="2"><input type="text"></td> 
# MAGIC   </tr>
# MAGIC   
# MAGIC   <tr style="background-color:#F0F0F0"><th colspan="2">Experiment</th><th>Min</th><th>Median</th><th>Max</th></tr>
# MAGIC 
# MAGIC   <tr>
# MAGIC     <td rowspan="2">#1: Not Cached</td>
# MAGIC     <td>Duration:</td>
# MAGIC     <td><input type="text"></td>
# MAGIC     <td><input type="text"></td>
# MAGIC     <td><input type="text"></td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>GC Time:</td>
# MAGIC     <td><input type="text"></td>
# MAGIC     <td><input type="text"></td>
# MAGIC     <td><input type="text"></td>
# MAGIC   </tr>
# MAGIC 
# MAGIC   
# MAGIC   <tr>
# MAGIC     <td rowspan="2">#2: MEMORY_AND_DISK</td>
# MAGIC     <td>Duration:</td>
# MAGIC     <td><input type="text"></td>
# MAGIC     <td><input type="text"></td>
# MAGIC     <td><input type="text"></td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>GC Time:</td>
# MAGIC     <td><input type="text"></td>
# MAGIC     <td><input type="text"></td>
# MAGIC     <td><input type="text"></td>
# MAGIC   </tr>
# MAGIC 
# MAGIC   <tr>
# MAGIC     <td rowspan="2">#3: DISK_ONLY</td>
# MAGIC     <td>Duration:</td>
# MAGIC     <td><input type="text"></td>
# MAGIC     <td><input type="text"></td>
# MAGIC     <td><input type="text"></td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>GC Time:</td>
# MAGIC     <td><input type="text"></td>
# MAGIC     <td><input type="text"></td>
# MAGIC     <td><input type="text"></td>
# MAGIC   </tr>
# MAGIC 
# MAGIC   <tr>
# MAGIC     <td rowspan="2">#4: MEMORY_ONLY</td>
# MAGIC     <td>Duration:</td>
# MAGIC     <td><input type="text"></td>
# MAGIC     <td><input type="text"></td>
# MAGIC     <td><input type="text"></td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>GC Time:</td>
# MAGIC     <td><input type="text"></td>
# MAGIC     <td><input type="text"></td>
# MAGIC     <td><input type="text"></td>
# MAGIC   </tr>
# MAGIC 
# MAGIC   <tr>
# MAGIC     <td rowspan="2">#5: MEMORY_ONLY_SER</td>
# MAGIC     <td>Duration:</td>
# MAGIC     <td><input type="text"></td>
# MAGIC     <td><input type="text"></td>
# MAGIC     <td><input type="text"></td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>GC Time:</td>
# MAGIC     <td><input type="text"></td>
# MAGIC     <td><input type="text"></td>
# MAGIC     <td><input type="text"></td>
# MAGIC   </tr>
# MAGIC 
# MAGIC   <tr>
# MAGIC     <td rowspan="2">#6: MEMORY_AND_DISK_SER</td>
# MAGIC     <td>Duration:</td>
# MAGIC     <td><input type="text"></td>
# MAGIC     <td><input type="text"></td>
# MAGIC     <td><input type="text"></td>
# MAGIC   </tr>
# MAGIC   <tr>
# MAGIC     <td>GC Time:</td>
# MAGIC     <td><input type="text"></td>
# MAGIC     <td><input type="text"></td>
# MAGIC     <td><input type="text"></td>
# MAGIC   </tr>
# MAGIC 
# MAGIC </table>