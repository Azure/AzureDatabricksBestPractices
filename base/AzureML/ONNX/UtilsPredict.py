# Databricks notebook source
# MAGIC %run ./Utils

# COMMAND ----------

TAG_PARENT_RUN_ID = "mlflow.parentRunId"

def get_best_run(experiment_id, metric, ascending=False, ignore_nested_runs=False):
    """
    Current search syntax does not allow to check for existence of a tag key so if there are nested runs we have to
    bring all runs back to the client making this costly and unscalable.
    """
    order_by = "ASC" if ascending else "DESC"
    column = "metrics.{} {}".format(metric, order_by)
    if ignore_nested_runs:
        runs = mlflow_client.search_runs(experiment_id, "", order_by=[column])
        runs = [ run for run in runs if TAG_PARENT_RUN_ID not in run.data.tags ]
    else:
        runs = mlflow_client.search_runs(experiment_id, "", order_by=[column], max_results=1)
    return runs[0].info.run_id,runs[0].data.metrics[metric]

# COMMAND ----------

def create_train_notebook_path():
  npath = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
  return npath.replace("Predict_","Train_")

# COMMAND ----------

def get_last_run(experiment_id):
    runs = mlflow_client.search_runs(experiment_id, order_by=["attributes.start_time desc"], max_results=1)
    return runs[0]

# COMMAND ----------

# TODO: use search_runs

TAG_PARENT_RUN_ID = "mlflow.parentRunId"

def get_last_parent_run(experiment_id):
    runs = [mlflow_client.get_run(x.run_id) for x in mlflow_client.list_run_infos(experiment_id)]
    runs = [x for x in runs if not TAG_PARENT_RUN_ID in x.data.tags]
    runs = sorted(runs, key=lambda x: x.info.start_time, reverse=True)
    return runs[0]

# COMMAND ----------

#WIDGET_PREDICT_EXPERIMENT = "Experiment ID or Name"
WIDGET_PREDICT_EXPERIMENT = "Experiment"
default_run_id = ""
RUN_MODE, BEST_RUN, LAST_RUN, RUN_ID = " Run Mode", "Best Run", "Last Run", "Run ID"

def create_predict_widgets(exp_name, metrics):
    print("exp_name:",exp_name)
    dbutils.widgets.text(WIDGET_PREDICT_EXPERIMENT,exp_name)
    dbutils.widgets.dropdown("Metric Name",metrics[0], metrics)
    dbutils.widgets.dropdown("Metric Sort","min",["min","max"])
    dbutils.widgets.text("Run ID",default_run_id)

    dbutils.widgets.dropdown(RUN_MODE, LAST_RUN,[RUN_ID,BEST_RUN,LAST_RUN])
    exp_id_or_name = dbutils.widgets.get(WIDGET_PREDICT_EXPERIMENT)
    print("exp_id_or_name:",exp_id_or_name)
    exp_id,exp_name = get_experiment_info(exp_id_or_name)
    print("exp_id:",exp_id)
    print("exp_name:",exp_name)
    
    run_mode = dbutils.widgets.get(RUN_MODE)
    metric = ""
    if run_mode == RUN_ID:
        run_id = dbutils.widgets.get("Run ID")
    elif run_mode == LAST_RUN:
        run = get_last_parent_run(exp_id)
        run_id = run.info.run_uuid
    elif run_mode == BEST_RUN:
        metric = dbutils.widgets.get("Metric Name")
        ascending = dbutils.widgets.get("Metric Sort")
        best = get_best_run_fast(exp_id,metric,ascending=="min")
        print("best:",best,"metric:",metric)
        run_id = best[0]
    else:
        run_id = dbutils.widgets.get("Run ID") # TODO: Error

    return run_id,exp_id,exp_name,run_id,metric,run_mode

# COMMAND ----------

def get_experiment_id(exp_id_or_name):
  if exp_id_or_name.isdigit(): 
      return exp_id_or_name
  exp = mlflow_client.get_experiment_by_name(exp_id_or_name)
  return exp.experiment_id

# COMMAND ----------

def get_experiment_info(exp_id_or_name):
  if exp_id_or_name.isdigit(): 
      exp = mlflow_client.get_experiment(exp_id_or_name)
      exp_name = None if exp is None else exp.name
      return (exp_id_or_name,exp_name)
  else:
      exp = mlflow_client.get_experiment_by_name(exp_id_or_name)
      exp_id = None if exp is None else exp.experiment_id
      return (exp_id,exp_id_or_name)