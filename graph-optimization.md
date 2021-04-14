# Advanced Collections: _Graph Optimizations_

Performance can be significantly improved in different contexts by making
small optimizations on the Dask graph before calling the scheduler.

The ``dask.optimization`` module contains several functions to transform graphs
in a variety of useful ways. In most cases, users won't need to interact with
these functions directly, as specialized subsets of these transforms are done
automatically in the Dask collections (``dask.array``, ``dask.bag``, and
``dask.dataframe``). However, users working with custom graphs or computations
may find that applying these methods results in substantial speedups.

In general, there are two goals when doing graph optimizations:

1. Simplify computation
2. Improve parallelism

Simplifying computation can be done on a graph level by removing unnecessary
tasks (``cull``), or on a task level by replacing expensive operations with
cheaper ones (``RewriteRule``).

Parallelism can be improved by reducing
inter-task communication, whether by fusing many tasks into one (``fuse``), or
by inlining cheap operations (``inline``, ``inline_functions``).

Below, we show an example walking through the use of some of these to optimize
a task graph.

Example
-------

Suppose you had a custom Dask graph for doing a word counting task:

```python
import dask

def print_and_return(string):
    print(string)
    return string

def format_str(count, val, nwords):
    return (f'word list has {count} occurrences of '
            f'{val}, out of {nwords} words')

dsk = {'words': 'apple orange apple pear orange pear pear',
       'nwords': (len, (str.split, 'words')),
       'val1': 'orange',
       'val2': 'apple',
       'val3': 'pear',
       'count1': (str.count, 'words', 'val1'),
       'count2': (str.count, 'words', 'val2'),
       'count3': (str.count, 'words', 'val3'),
       'format1': (format_str, 'count1', 'val1', 'nwords'),
       'format2': (format_str, 'count2', 'val2', 'nwords'),
       'format3': (format_str, 'count3', 'val3', 'nwords'),
       'print1': (print_and_return, 'format1'),
       'print2': (print_and_return, 'format2'),
       'print3': (print_and_return, 'format3')}

dask.visualize(dsk, verbose=True, collapse_outputs=True)
```

Here we are counting the occurrence of the words ``'orange``, ``'apple'``, and
``'pear'`` in the list of words, formatting an output string reporting the
results, printing the output, and then returning the output string.

To perform the computation, we first remove unnecessary components from the
graph using the ``cull`` function and then pass the Dask graph and the desired
output keys to a scheduler ``get`` function:

```python
from dask.threaded import get
from dask.optimization import cull

outputs = ['print1', 'print2']
dsk1, dependencies = cull(dsk, outputs)  # remove unnecessary tasks from the graph

results = get(dsk1, outputs)
dask.visualize(dsk1, verbose=True, collapse_outputs=True)
```

As can be seen above, the scheduler computed only the requested outputs
(``'print3'`` was never computed). This is because we called the
``dask.optimization.cull`` function, which removes the unnecessary tasks from
the graph.

Culling is part of the default optimization pass of almost all collections.
Often you want to call it somewhat early to reduce the amount of work done in
later steps.

Looking at the task graph above, there are multiple accesses to constants such
as ``'val1'`` or ``'val2'`` in the Dask graph. These can be inlined into the
tasks to improve efficiency using the ``inline`` function. For example:


```python
from dask.optimization import inline

dsk2 = inline(dsk1, dependencies=dependencies)
results = get(dsk2, outputs)
dask.visualize(dsk2, verbose=True, collapse_outputs=True)
```

Now we have two sets of *almost* linear task chains. The only link between them
is the word counting function. For cheap operations like this, the
serialization cost may be larger than the actual computation, so it may be
faster to do the computation more than once, rather than passing the results to
all nodes. To perform this function inlining, the ``inline_functions`` function
can be used:

```python
from dask.optimization import inline_functions

dsk3 = inline_functions(dsk2, outputs, [len, str.split],
                        dependencies=dependencies)
results = get(dsk3, outputs)
dask.visualize(dsk3, verbose=True, collapse_outputs=True)
```

Now we have a set of purely linear tasks. We'd like to have the scheduler run
all of these on the same worker to reduce data serialization between workers.
One option is just to merge these linear chains into one big task using the
``fuse`` function:

```python
from dask.optimization import fuse

dsk4, dependencies = fuse(dsk3)
results = get(dsk4, outputs)
dask.visualize(dsk4, verbose=True, collapse_outputs=True)
```

Putting it all together:

```python
def optimize_and_get(dsk, keys):
    dsk1, deps = cull(dsk, keys)
    dsk2 = inline(dsk1, dependencies=deps)
    dsk3 = inline_functions(dsk2, keys, [len, str.split],
                            dependencies=deps)
    dsk4, deps = fuse(dsk3)
    return get(dsk4, keys)

optimize_and_get(dsk, outputs)
```

In summary, the above operations accomplish the following:

1. Removed tasks unnecessary for the desired output using ``cull``
2. Inlined constants using ``inline``
3. Inlined cheap computations using ``inline_functions``, improving parallelism
4. Fused linear tasks together to ensure they run on the same worker using ``fuse``

As stated previously, these optimizations are already performed automatically
in the Dask collections. Users not working with custom graphs or computations
should rarely need to directly interact with them.

### Keyword Arguments

Some optimizations take optional keyword arguments.  To pass keywords from the
compute call down to the right optimization, prepend the keyword with the name
of the optimization.  For example, to send a ``keys=`` keyword argument to the
``fuse`` optimization from a compute call, use the ``fuse_keys=`` keyword:

```python
def fuse(dsk, keys=None):
    ...

x.compute(fuse_keys=['x', 'y', 'z'])
```

## Customizing Optimization

Dask defines a default optimization strategy for each collection type (Array,
Bag, DataFrame, Delayed).  However, different applications may have different
needs.  To address this variability of needs, you can construct your own custom
optimization function and use it instead of the default.  An optimization
function takes in a task graph and list of desired keys and returns a new
task graph:

```python
def my_optimize_function(dsk, keys):
    new_dsk = {...}
    return new_dsk
```

You can then register this optimization class against whichever collection type
you prefer and it will be used instead of the default scheme:

```python
with dask.config.set(array_optimize=my_optimize_function):
    x, y = dask.compute(x, y)
```

You can register separate optimization functions for different collections, or
you can register ``None`` if you do not want particular types of collections to
be optimized:

```python
with dask.config.set(array_optimize=my_optimize_function,
                    dataframe_optimize=None,
                    delayed_optimize=my_other_optimize_function):
       ...
```
You do not need to specify all collections.  Collections will default to their
standard optimization scheme (which is usually a good choice).

When creating your own collection, you can specify the optimization function
by implementing a ``_dask_optimize__`` method.
