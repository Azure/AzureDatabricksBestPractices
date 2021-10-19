// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC <style>
// MAGIC   input { width: 10em }
// MAGIC </style>
// MAGIC 
// MAGIC <table>
// MAGIC   <tr style="background-color:#F0F0F0"><th colspan="2">What</th><th>2014</th><th>2018</th><th>Description / Help</th></tr>
// MAGIC   <tr>
// MAGIC     <td colspan="2">Number of Jobs:</td>
// MAGIC     <td><input type="text"></td> 
// MAGIC     <td><input type="text"></td> 
// MAGIC     <td>How many jobs did the call to **count()** trigger?</td>
// MAGIC   </tr>
// MAGIC   <tr>
// MAGIC     <td colspan="2">Number of Stages:</td>
// MAGIC     <td><input type="text"></td> 
// MAGIC     <td><input type="text"></td> 
// MAGIC     <td>How many stages did the call to **count()** trigger?</td>
// MAGIC   </tr>
// MAGIC   <tr>
// MAGIC     <td colspan="2">Number of Tasks:</td>
// MAGIC     <td><input type="text"></td> 
// MAGIC     <td><input type="text"></td>
// MAGIC     <td>How many tasks did the call to **count()** trigger?</td>
// MAGIC   </tr>
// MAGIC 
// MAGIC   
// MAGIC   
// MAGIC   <tr style="background-color:#F0F0F0"><th>What/Value</th><th style="width:1em">&nbsp;</th><th>2014</th><th>2018</th><th>Description / Help</th></tr>
// MAGIC   <tr>
// MAGIC     <td colspan="2">GC Time:</td>
// MAGIC     <td><input type="text"></td> 
// MAGIC     <td><input type="text"></td>
// MAGIC     <td>View the stage details, **GC Time** row, **Median** column</td>
// MAGIC   </tr>
// MAGIC 
// MAGIC   
// MAGIC   <tr>
// MAGIC     <td rowspan="3">Input Size:</td>
// MAGIC     <td>Min</td>
// MAGIC     <td><input type="text"></td> 
// MAGIC     <td><input type="text"></td> 
// MAGIC     <td>View the stage details, **Input Size / Records** row, **Min** column, 1st value</td>
// MAGIC   </tr>
// MAGIC   <tr>
// MAGIC     <td>Median</td>
// MAGIC     <td><input type="text"></td> 
// MAGIC     <td><input type="text"></td> 
// MAGIC     <td>View the stage details, **Input Size / Records** row, **Median** column, 1st value</td>
// MAGIC   </tr>
// MAGIC   <tr>
// MAGIC     <td>Max</td>
// MAGIC     <td><input type="text"></td> 
// MAGIC     <td><input type="text"></td> 
// MAGIC     <td>View the stage details, **Input Size / Records** row, **Max** column, 1st value</td>
// MAGIC   </tr>
// MAGIC   
// MAGIC   
// MAGIC   
// MAGIC   <tr>
// MAGIC     <td rowspan="3">Records:</td>
// MAGIC     <td>Min</td>
// MAGIC     <td><input type="text"></td> 
// MAGIC     <td><input type="text"></td> 
// MAGIC     <td>View the stage details, **Input Size / Records** row, **Min** column, 2nd value</td>
// MAGIC   </tr>
// MAGIC   <tr>
// MAGIC     <td>Median</td>
// MAGIC     <td><input type="text"></td> 
// MAGIC     <td><input type="text"></td> 
// MAGIC     <td>View the stage details, **Input Size / Records** row, **Median** column, 2nd value</td>
// MAGIC   </tr>
// MAGIC   <tr>
// MAGIC     <td>Max</td>
// MAGIC     <td><input type="text"></td> 
// MAGIC     <td><input type="text"></td> 
// MAGIC     <td>View the stage details, **Input Size / Records** row, **Max** column, 2nd value</td>
// MAGIC   </tr>
// MAGIC 
// MAGIC   
// MAGIC   <tr><td colspan="5">Other metrics may also provide clues as to a cause such as **Scheduler Delay**, **Task Deserialization Time**, **Result Serialization Time** and **Peak Execution Memory**</td></tr>
// MAGIC   
// MAGIC   
// MAGIC   <tr style="background-color:#F0F0F0"><th>What/Value</th><th style="width:1em">&nbsp;</th><th>2014</th><th>2018</th><th>Description / Help</th></tr>
// MAGIC   <tr>
// MAGIC     <td colspan="2"># of Parquet Files:</td>
// MAGIC     <td><input type="text"></td> 
// MAGIC     <td><input type="text"></td>
// MAGIC     <td>
// MAGIC       Using our utility function **computeFileStats(..)**, determine the number of files for the entire dataset<br/>
// MAGIC       If there is a significant difference, continue investigating the individual partitions.
// MAGIC     </td>
// MAGIC   </tr>
// MAGIC   <tr>
// MAGIC     <td rowspan="2">Partition File Counts:</td>
// MAGIC     <td>2014</td> 
// MAGIC     <td colspan="2"><input type="text" style="width:23em" value="n, n, n, n, n, n, n, n, n, n, n, n"></td>
// MAGIC     <td rowspan="2">Using our utility function **computeFileStats(..)**, determine<br/>if any of the 24 partitions have more files than any other</td>
// MAGIC   </tr>
// MAGIC   <tr>
// MAGIC     <td>2018</td> 
// MAGIC     <td colspan="2"><input type="text" style="width:23em" value="n, n, n, n, n, n, n, n, n, n, n, n"></td>
// MAGIC   </tr>
// MAGIC  
// MAGIC </table>