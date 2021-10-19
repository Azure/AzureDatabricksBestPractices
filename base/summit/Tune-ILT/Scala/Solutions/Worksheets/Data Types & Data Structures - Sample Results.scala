// Databricks notebook source
// MAGIC 
// MAGIC %md-sandbox
// MAGIC <html>
// MAGIC   <body>
// MAGIC     <style>
// MAGIC       input { width: 10em; text-align:right }
// MAGIC     </style>
// MAGIC 
// MAGIC     <table style="margin:0">
// MAGIC       <tr style="background-color:#F0F0F0"><th colspan="2">&nbsp;</th><th>Sample A<br/>(Parquet/String)</th><th>Sample B<br/>(CSV/String)</th><th>Sample C<br/>(Parquet/Mixed)</th></tr>
// MAGIC       <tr>
// MAGIC         <td colspan="2">Number of Records:</td>
// MAGIC         <td><input type="text" value="41,309,277"></td> 
// MAGIC         <td><input type="text" value="41,309,277"></td> 
// MAGIC         <td><input type="text" value="41,309,277"></td> 
// MAGIC       </tr>
// MAGIC       <tr>
// MAGIC         <td colspan="2">Query Duration:</td>
// MAGIC         <td><input type="text" value="452 ms"></td> 
// MAGIC         <td><input type="text" value="22,222 ms"></td> 
// MAGIC         <td><input type="text" value="835 ms"></td> 
// MAGIC       </tr>
// MAGIC       <tr>
// MAGIC         <td colspan="2">Records per MS:</td>
// MAGIC         <td><input type="text" value="91,392"></td> 
// MAGIC         <td><input type="text" value="1,858"></td> 
// MAGIC         <td><input type="text" value="49,472"></td> 
// MAGIC       </tr>
// MAGIC 
// MAGIC 
// MAGIC       <tr style="background-color:#F0F0F0"><th colspan="5">&nbsp;</th></tr>
// MAGIC 
// MAGIC 
// MAGIC       <tr>
// MAGIC         <td colspan="2">Size on Disk:</td>
// MAGIC         <td><input type="text" value="1.38 GB"></td> 
// MAGIC         <td><input type="text" value="4.21 GB"></td> 
// MAGIC         <td><input type="text" value="1.25 GB"></td> 
// MAGIC       </tr>
// MAGIC       <tr>
// MAGIC         <td colspan="2">Bytes per Record:</td>
// MAGIC         <td><input type="text" value="35"></td> 
// MAGIC         <td><input type="text" value="109"></td> 
// MAGIC         <td><input type="text" value="32"></td> 
// MAGIC       </tr>
// MAGIC 
// MAGIC 
// MAGIC       <tr style="background-color:#F0F0F0"><th colspan="5">&nbsp;</th></tr>
// MAGIC 
// MAGIC 
// MAGIC       <tr>
// MAGIC         <td colspan="2">Sized Cached in RAM:</td>
// MAGIC         <td><input type="text" value="3.44 GB"></td> 
// MAGIC         <td><input type="text" value="3.51 GB"></td> 
// MAGIC         <td><input type="text" value="2.29 GB"></td> 
// MAGIC       </tr>
// MAGIC       <tr>
// MAGIC         <td colspan="2">Bytes per Record:</td>
// MAGIC         <td><input type="text" value="89"></td> 
// MAGIC         <td><input type="text" value="91"></td> 
// MAGIC         <td><input type="text" value="59"></td> 
// MAGIC       </tr>
// MAGIC       <tr>
// MAGIC         <td colspan="2">Cache Duration:</td>
// MAGIC         <td><input type="text" value="73.80 sec"></td> 
// MAGIC         <td><input type="text" value="92.52 sec"></td> 
// MAGIC         <td><input type="text" value="56.59 sec"></td> 
// MAGIC       </tr>
// MAGIC       <tr>
// MAGIC         <td colspan="2">Query Duration:</td>
// MAGIC         <td><input type="text" value="96 ms"></td> 
// MAGIC         <td><input type="text" value="94 ms"></td> 
// MAGIC         <td><input type="text" value="75 ms"></td> 
// MAGIC       </tr>
// MAGIC       <tr>
// MAGIC         <td colspan="2">Records per MS:</td>
// MAGIC         <td><input type="text" value="430,304"></td> 
// MAGIC         <td><input type="text" value="439,460"></td> 
// MAGIC         <td><input type="text" value="550,790"></td> 
// MAGIC       </tr>
// MAGIC 
// MAGIC 
// MAGIC       <tr style="background-color:#F0F0F0"><th colspan="5">&nbsp;</th></tr>
// MAGIC 
// MAGIC 
// MAGIC       <tr>
// MAGIC         <td colspan="2">Sized Cached to Disk:</td>
// MAGIC         <td><input type="text" value="1.69 GB"></td> 
// MAGIC         <td><input type="text" value="1.70 GB"></td> 
// MAGIC         <td><input type="text" value="1.51 GB"></td> 
// MAGIC       </tr>
// MAGIC       <tr>
// MAGIC         <td colspan="2">Bytes per Record:</td>
// MAGIC         <td><input type="text" value="43"></td> 
// MAGIC         <td><input type="text" value="44"></td> 
// MAGIC         <td><input type="text" value="39"></td> 
// MAGIC       </tr>
// MAGIC       <tr>
// MAGIC         <td colspan="2">Cache Duration:</td>
// MAGIC         <td><input type="text" value="79.03 sec"></td> 
// MAGIC         <td><input type="text" value="94.73 sec"></td> 
// MAGIC         <td><input type="text" value="55.80 sec"></td> 
// MAGIC       </tr>
// MAGIC       <tr>
// MAGIC         <td colspan="2">Query Duration:</td>
// MAGIC         <td><input type="text" value="1311 ms"></td> 
// MAGIC         <td><input type="text" value="1359 ms"></td> 
// MAGIC         <td><input type="text" value="832 ms"></td> 
// MAGIC       </tr>
// MAGIC       <tr>
// MAGIC         <td colspan="2">Records per MS:</td>
// MAGIC         <td><input type="text" value="31,509"></td> 
// MAGIC         <td><input type="text" value="30,396"></td> 
// MAGIC         <td><input type="text" value="49,650"></td> 
// MAGIC       </tr>
// MAGIC 
// MAGIC 
// MAGIC     </table>
// MAGIC   </body>
// MAGIC </html>