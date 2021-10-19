# Databricks notebook source

from pyspark.sql.types import *
# It's very important to put this import before keras,
# as explained here: Loading tensorflow before scipy.misc seems to cause imread to fail #1541
# https://github.com/tensorflow/tensorflow/issues/1541
import scipy.misc

import tensorflow as tf
from collections import namedtuple
from keras import backend as K
import tempfile
import os
from keras.models import load_model
from pyspark.sql.types import *


# COMMAND ----------

# MAGIC %run ./sql_udf_lib_3

# COMMAND ----------


def stripAndFreezeGraph(tf_graph_def, sess, output_names):
    """
    A typical usage would look like:
      tf_graph_def = tf_graph.as_graph_def(add_shapes=True)
      sess = tf.Session(graph=tf_graph)
    """
    # The following doesn't seem to be needed... for graphs ready for prediction usage.
    #if len(tf.global_variables()) > 0:
    #    sess.run(tf.global_variables_initializer())
    output_graph_def = tf.graph_util.convert_variables_to_constants(
        sess,
        tf_graph_def,
        output_names,
        variable_names_blacklist=[])
    g2 = tf.Graph()
    with g2.as_default():
        tf.import_graph_def(output_graph_def, name='')
    return g2


# A super simple representation of a function as a TF blob
# The graph def is a frozen chunk of a TF graph
# Input names is the list of op names of inputs (not tensor names). They must be placeholders.
# Ouput names is the list of op names of outputs (not tensor names). They can be whatever.
GraphFunction = namedtuple("GraphFunction", ["graph_def", "input_names", "output_names"])

def _op_name(tensor):
    if type(tensor) is tf.Tensor:
        return _op_name(tensor.name)
    assert (type(tensor) is str) or (type(tensor) is unicode), (tensor, type(tensor))
    return tensor.split(":")[0]

def _tensor_name(op_name):
    return op_name + ":0"

def _check_output(output, graph):
    if type(output) is tf.Tensor:
        return _check_output(output.name, graph)
    assert (type(output) is str) or (type(output) is unicode), (output, type(output))
    op_name = _op_name(output)
    assert graph.get_operation_by_name(op_name), output
    return op_name

def _check_input(input_, graph):
    if type(input_) is tf.Tensor:
        assert input_.op.type == u'Placeholder', (input_, input_.op)
        return _check_input(input_.name, graph)
    assert (type(input_) is str) or (type(input_) is unicode), (input_, type(input_))
    op_name = _op_name(input_)
    assert graph.get_operation_by_name(op_name), output
    return op_name

def makeGFunction(graph, inputs, outputs):
    gdef = graph.as_graph_def(add_shapes=True)
    return GraphFunction(
        graph_def = gdef,
        input_names = [_check_input(i, graph) for i in inputs],
        output_names = [_check_output(o, graph) for o in outputs])

def makeGUDF(name, gf, vectorized = False):
    g = tf.Graph()
    with g.as_default():
        tf.import_graph_def(gf.graph_def, name='')
        fetches = [g.get_tensor_by_name(_tensor_name(o)) for o in gf.output_names]
        return makeUDF(fetches, name, blocked=vectorized)


def mergeFunctions(functions):
    """ Takes multiple graph functions and merges them into a single graph function.

    Arguments:
      functions: a list of (name, GraphFunction). The name can be null, but you risk collisions between elements.

    It is assumed that there is only one input and one output in the intermediate layers.
    """
    assert len(functions) >= 1, functions
    (scope_1, gf_1) = functions[0]
    (scope_last, gf_last) = functions[-1]
    (last_output_name,) = gf_last.output_names
    def scoped_names(l, scope_name):
        if scope_name == '':
            return l
        else:
            return [scope_name + "/" + i for i in l]
    # Only one output allowed for now.
    assert len(gf_1.output_names) == 1, gf_1.output_names
    # Get the name and the shapes of all the inputs:
    # Import the first layer, no scoping.
    g0 = tf.Graph()
    with g0.as_default():
        tf.import_graph_def(gf_1.graph_def, return_elements=gf_1.output_names, name='')
        input_tensors0 = [g0.get_tensor_by_name(_tensor_name(x)) for x in gf_1.input_names]
        input_info0 = [(_op_name(t.name), t.dtype, t.shape) for t in input_tensors0]
    g = tf.Graph()
    with g.as_default():
        # Add the placeholders that correspond to the first layer.
        inputs = [tf.placeholder(dtype, shape, name) for (name, dtype, shape) in input_info0]
        input_map1 = dict(zip(gf_1.input_names, inputs))
        print "input_map1", input_map1
        # The first layer:
        #returns_1 = [_tensor_name(x) for x in scoped_names(gf_1.output_names, scope_1)]
        (output_1,) = tf.import_graph_def(gf_1.graph_def, return_elements=gf_1.output_names, name=scope_1, input_map=input_map1)
        last_output = output_1
        last_output_t = g.get_tensor_by_name(_tensor_name(last_output.name))
        # The other layers
        for (scope, gf) in functions[1:]:
            #(current_return,) = [_tensor_name(x) for x in scoped_names(gf.output_names, scope)]
            (current_input,) = gf.input_names
            (current_output,) = tf.import_graph_def(
                gf.graph_def,
                input_map={_tensor_name(current_input): last_output_t},
                return_elements=gf.output_names,
                name=scope)
            last_output = current_output
            last_output_t = g.get_tensor_by_name(_tensor_name(last_output.name))
        # Add an identity input to make the current output, if the current one is hidden in a scope:
        if scope_last != '':
            final_output = tf.identity(last_output_t, name=last_output_name)
        # It seems some placeholders are still dangling. Remove them:
        gdef = g.as_graph_def(add_shapes=True)
    session = tf.Session()
    # TODO: connect final_output
    gdef2 = stripAndFreezeGraph(gdef, session, gf_last.output_names)
    return makeGFunction(gdef2, gf_1.input_names, gf_last.output_names)


class KSessionWrap():

    def __init__(self, graph = None, learning_phase=0):
        # Phase 0 -> testing
        # Phase 1 -> training
        self.requested_graph = graph
        self.requested_learning_phase = learning_phase

    def __enter__(self):
        self.old_session = K.get_session()
        self.g = self.requested_graph or tf.Graph()
        self.current_session = tf.Session(graph = self.g)
        K.set_session(self.current_session)
        K.set_learning_phase(self.requested_learning_phase)
        return (self.current_session, self.g)

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Restore the previous session
        K.set_session(self.old_session)

def modelToFunction(model):
    d = tempfile.mkdtemp(prefix='keras-')
    f = os.path.join(d, "model.h5")
    # Save and restore in a new session
    model.save(f)
    with KSessionWrap() as (s, g):
        with g.as_default():
            K.set_learning_phase(0) # Testing phase
            model2 = load_model(f)
            out_op_name = _op_name(model2.output)
            # Full clean of the model
            g2 = stripAndFreezeGraph(g.as_graph_def(add_shapes=True), s, [out_op_name])
            #return g2
    # Reimport in a clean session:
    with KSessionWrap(g2) as (s2, _):
        with g2.as_default():
            input_2 = g2.get_tensor_by_name(model2.input.name)
            output_2 = g2.get_tensor_by_name(model2.output.name)
            return makeGFunction(g2, [input_2], [output_2])

# COMMAND ----------

# MAGIC %md Functions for wrapping keras models

# COMMAND ----------

def _build_loader_snippet():
  g = tf.Graph()
  with g.as_default():
      # Flat image data -> image dimensions
      height = tf.placeholder(tf.int32, [], name="height")
      width = tf.placeholder(tf.int32, [], name="width")
      num_channels = tf.placeholder(tf.int32, [], name="nChannels")
      image_buffer = tf.placeholder(tf.string, [], name="data")
      # Note: the shape argument is required for tensorframes as it uses a 
      # slightly older version of tensorflow.
      shape = tf.reshape(
          tf.stack([height, width, num_channels], axis=0), 
          shape=(3,), 
          name='shape')
      # TODO: support non-RGB8 by checking image mode
      image_uint8 = tf.decode_raw(image_buffer, tf.uint8, name="decode_raw")
      image_float = tf.to_float(image_uint8)
      image_reshaped = tf.reshape(image_float, shape, name="reshaped")
      image_reshaped_expanded = tf.expand_dims(image_reshaped, 0, name="expanded")
      snip1 = makeGFunction(g, [height, width, image_buffer, num_channels], [image_reshaped_expanded])
      return snip1

# COMMAND ----------

def _build_preprocessor_snippet(input_shape, preprocessor_function):
  assert len(input_shape) >= 2, input_shape
  if len(input_shape) == 3:
    (w, h, _) = input_shape
    input_shape = (w, h)
  g = tf.Graph()
  with g.as_default():
    image_input = tf.placeholder(
          tf.float32,
          [None, None, None, 3], 
          name="image_input")
    resized_images = tf.image.resize_images(image_input, input_shape)
    processed_images = preprocess_input(resized_images)
    snip2 = makeGFunction(g, [image_input], [processed_images])
    return snip2

# COMMAND ----------

def _build_final_flattener_snippet():
  g = tf.Graph()
  with g.as_default():
    mat_input = tf.placeholder(
          tf.float32,
          [None, None],
          name="mat_input")
    mat_output = tf.identity(tf.reshape(mat_input, shape=[-1])[0], name='output')
    snip4 = makeGFunction(g, [mat_input], [mat_output])
    return snip4

# COMMAND ----------

def registerKerasUDF(udf_name, keras_model, preprocessor, input_shape, flatten_output=True):
  assert flatten_output
  snip1 = _build_loader_snippet()
  snip2 = _build_preprocessor_snippet(input_shape, preprocessor)
  snip3 = modelToFunction(model)
  snip4 = _build_final_flattener_snippet()
  gf = mergeFunctions([('ingest', snip1), ('preprocessing', snip2), ('model', snip3), ('reshaping', snip4)])
  udf3 = makeGUDF(udf_name, gf)
  registerCompositeUDF(udf_name, [udf3])