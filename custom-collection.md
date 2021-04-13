# Advanced Collections: _Custom Collections_

[Docs](https://docs.dask.org/en/latest/custom-collections.html)

For many problems, the built-in Dask collections (``dask.array``,
``dask.dataframe``, ``dask.bag``, and ``dask.delayed``) are sufficient. For
cases where they aren't, it's possible to create your own Dask collections. Here
we describe the required methods to fulfill the Dask collection interface.

## The Dask Collection Interface

To create your own Dask collection, you need to fulfill the following
interface. Note that there is no required base class.

```python
class CollectionInterface():
    def __dask_graph__(self):
        """The Dask graph.

        **Returns**
        dsk : Mapping, None
            The Dask graph.  If ``None``, this instance will not be interpreted as a
            Dask collection, and none of the remaining interface methods will be
            called.

        If the collection also specifies :meth:`__dask_layers__`, then ``dsk`` must be a
        :class:`~dask.highlevelgraph.HighLevelGraph` or ``None``.
        """

    def __dask_keys__(self):
        """The output keys for the Dask graph.

        Note that there are additional constraints on keys for a Dask collection
        than those described in the :doc:`task graph specification documentation <spec>`.
        These additional constraints are described below.

        **Returns**

        keys : list
            A possibly nested list of keys that represent the outputs of the graph.
            After computation, the results will be returned in the same layout,
            with the keys replaced with their corresponding outputs.

        All keys must either be non-empty strings or tuples where the first element is a
        non-empty string, followed by zero or more arbitrary hashables.
        The non-empty string is commonly known as the *collection name*. All collections
        embedded in the dask package have exactly one name, but this is not a requirement.

        These are all valid outputs:

        - ``[]``
        - ``["x", "y"]``
        - ``[[("y", "a", 0), ("y", "a", 1)], [("y", "b", 0), ("y", "b", 1)]``
        """

    def __dask_layers__(self):
        """ This method should only be implemented if the collection uses
        ``~dask.highlevelgraph.HighLevelGraph`` to implement its dask graph.

        **Returns**

        names : tuple
            Tuple of names of the HighLevelGraph layers which contain all keys returned by
            ``__dask_keys__``.
        """

    @staticmethod
    def __dask_optimize__(dsk, keys, **kwargs):
        """
        Given a graph and keys, return a new optimized graph.

        This method can be either a ``staticmethod`` or a ``classmethod``, but not
        an ``instancemethod``.

        Note that graphs and keys are merged before calling ``__dask_optimize__``;
        as such, the graph and keys passed to this method may represent more than
        one collection sharing the same optimize method.

        If not implemented, defaults to returning the graph unchanged.

        **Parameters**

        dsk : MutableMapping
            The merged graphs from all collections sharing the same
            ``__dask_optimize__`` method.
        keys : list
            A list of the outputs from ``__dask_keys__`` from all collections
            sharing the same ``__dask_optimize__`` method.
        **kwargs
            Extra keyword arguments forwarded from the call to ``compute`` or
            ``persist``. Can be used or ignored as needed.

        **Returns**

        optimized_dsk : MutableMapping
            The optimized Dask graph.
        """

    @staticmethod
    def __dask_scheduler__(dsk, keys, **kwargs):
        """The default scheduler ``get`` to use for this object.

        Usually attached to the class as a staticmethod, e.g.:

        >>> import dask.threaded
        >>> class MyCollection:
        ...     # Use the threaded scheduler by default
        ...     __dask_scheduler__ = staticmethod(dask.threaded.get)
        """

    def __dask_postcompute__(self):
        """Return the finalizer and (optional) extra arguments to convert the computed
        results into their in-memory representation.

        Used to implement ``dask.compute``.

        **Returns**

        finalize : callable
            A function with the signature ``finalize(results, *extra_args)``.
            Called with the computed results in the same structure as the
            corresponding keys from ``__dask_keys__``, as well as any extra
            arguments as specified in ``extra_args``. Should perform any necessary
            finalization before returning the corresponding in-memory collection
            from ``compute``. For example, the ``finalize`` function for
            ``dask.array.Array`` concatenates all the individual array chunks into
            one large numpy array, which is then the result of ``compute``.
        extra_args : tuple
            Any extra arguments to pass to ``finalize`` after ``results``. If no
            extra arguments should be an empty tuple.
        """

    def __dask_postpersist__(self):
        """Return the rebuilder and (optional) extra arguments to rebuild an equivalent
        Dask collection from a persisted or rebuilt graph.

        Used to implement :func:`dask.persist`.

        **Returns**

        rebuild : callable
            A function with the signature
            ``rebuild(dsk, *extra_args, rename : Mapping[str, str] = None)``.
            ``dsk`` is a Mapping which contains at least the output keys returned by
            :meth:`__dask_keys__`. The callable should return an equivalent Dask collection
            with the same keys as ``self``, but with the results that are computed through a
            different graph. In the case of :func:`dask.persist`, the new graph will have
            just the output keys and the values already computed.

            If the optional parameter ``rename`` is specified, it indicates that output
            keys may be changing too; e.g. if the previous output of :meth:`__dask_keys__`
            was ``[("a", 0), ("a", 1)]``, after calling
            ``rebuild(dsk, *extra_args, rename={"a": "b"})`` it must become
            ``[("b", 0), ("b", 1)]``.
            The ``rename`` mapping may not contain the collection name(s); in such case the
            associated keys do not change. It may contain replacements for unexpected names,
            which must be ignored.

        extra_args : tuple
            Any extra arguments to pass to ``rebuild`` after ``dsk``. If no extra
            arguments are necessary, it must be an empty tuple.
        """

    def __dask_tokenize__(self):
        """Optional, but recommended for implementing deterministic hashing.

        Where possible, it is recommended to define the ``__dask_tokenize__`` method.
        This method takes no arguments and should return a value fully
        representative of the object.

        **Returns**

        token : string
            A unique identifier for this collection. Used as the key in dask graph.
        """

```

## Internals of the Core Dask Methods

Dask has a few *core* functions (and corresponding methods) that implement
common operations:

- [``compute``](#compute): Convert one or more Dask collections into their in-memory
  counterparts
- [``persist``](#persist): Convert one or more Dask collections into equivalent Dask
  collections with their results already computed and cached in memory
- [``optimize``](#optimize): Convert one or more Dask collections into equivalent Dask
  collections sharing one large optimized graph
- [``visualize``](#visualize): Given one or more Dask collections, draw out the graph that
  would be passed to the scheduler during a call to ``compute`` or ``persist``
- [``tokenize``]: Generate key for collection using Dask's own deterministic hash function.

Here we briefly describe the internals of these functions to illustrate how
they relate to the above interface.

### Compute

The operation of ``compute`` can be broken into three stages:

1. **Graph Merging & Optimization**

   First, the individual collections are converted to a single large graph and
   nested list of keys. How this happens depends on the value of the
   ``optimize_graph`` keyword, which each function takes:

   - If ``optimize_graph`` is ``True`` (default), then the collections are first
     grouped by their ``__dask_optimize__`` methods.  All collections with the
     same ``__dask_optimize__`` method have their graphs merged and keys
     concatenated, and then a single call to each respective
     ``__dask_optimize__`` is made with the merged graphs and keys.  The
     resulting graphs are then merged.

   - If ``optimize_graph`` is ``False``, then all the graphs are merged and all
     the keys concatenated.

   After this stage there is a single large graph and nested list of keys which
   represents all the collections.

2. **Computation**

   After the graphs are merged and any optimizations performed, the resulting
   large graph and nested list of keys are passed on to the scheduler.  The
   scheduler to use is chosen as follows:

   - If a ``get`` function is specified directly as a keyword, use that
   - Otherwise, if a global scheduler is set, use that
   - Otherwise fall back to the default scheduler for the given collections.
     Note that if all collections don't share the same ``__dask_scheduler__``
     then an error will be raised.

   Once the appropriate scheduler ``get`` function is determined, it is called
   with the merged graph, keys, and extra keyword arguments.  After this stage,
   ``results`` is a nested list of values. The structure of this list mirrors
   that of ``keys``, with each key substituted with its corresponding result.

3. **Postcompute**

   After the results are generated, the output values of ``compute`` need to be
   built. This is what the ``__dask_postcompute__`` method is for.
   ``__dask_postcompute__`` returns two things:

   - A ``finalize`` function, which takes in the results for the corresponding
     keys
   - A tuple of extra arguments to pass to ``finalize`` after the results

   To build the outputs, the list of collections and results is iterated over,
   and the finalizer for each collection is called on its respective results.

In pseudocode, this process looks like the following:

```python
def compute(*collections, **kwargs):
    # 1. Graph Merging & Optimization
    # -------------------------------
    if kwargs.pop('optimize_graph', True):
        # If optimization is turned on, group the collections by
        # optimization method, and apply each method only once to the merged
        # sub-graphs.
        optimization_groups = groupby_optimization_methods(collections)
        graphs = []
        for optimize_method, cols in optimization_groups:
            # Merge the graphs and keys for the subset of collections that
            # share this optimization method
            sub_graph = merge_graphs([x.__dask_graph__() for x in cols])
            sub_keys = [x.__dask_keys__() for x in cols]
            # kwargs are forwarded to ``__dask_optimize__`` from compute
            optimized_graph = optimize_method(sub_graph, sub_keys, **kwargs)
            graphs.append(optimized_graph)
        graph = merge_graphs(graphs)
    else:
        graph = merge_graphs([x.__dask_graph__() for x in collections])

    # Keys are always the same
    keys = [x.__dask_keys__() for x in collections]

    # 2. Computation
    # --------------
    # Determine appropriate get function based on collections, global
    # settings, and keyword arguments
    get = determine_get_function(collections, **kwargs)
    # Pass the merged graph, keys, and kwargs to ``get``
    results = get(graph, keys, **kwargs)

    # 3. Postcompute
    # --------------
    output = []
    # Iterate over the results and collections
    for res, collection in zip(results, collections):
        finalize, extra_args = collection.__dask_postcompute__()
        out = finalize(res, **extra_args)
        output.append(out)

    # `dask.compute` always returns tuples
    return tuple(output)
```

### Persist

Persist is very similar to ``compute``, except for how the return values are
created. It too has three stages:

1. **Graph Merging & Optimization**

   Same as in ``compute``.

2. **Computation**

   Same as in ``compute``, except in the case of the distributed scheduler,
   where the values in ``results`` are futures instead of values.

3. **Postpersist**

   Similar to ``__dask_postcompute__``, ``__dask_postpersist__`` is used to
   rebuild values in a call to ``persist``. ``__dask_postpersist__`` returns
   two things:

   - A ``rebuild`` function, which takes in a persisted graph.  The keys of
     this graph are the same as ``__dask_keys__`` for the corresponding
     collection, and the values are computed results (for the single machine
     scheduler) or futures (for the distributed scheduler).
   - A tuple of extra arguments to pass to ``rebuild`` after the graph

   To build the outputs of ``persist``, the list of collections and results is
   iterated over, and the rebuilder for each collection is called on the graph
   for its respective results.

In pseudocode, this looks like the following:

```python
def persist(*collections, **kwargs):
    # 1. Graph Merging & Optimization
    # -------------------------------
    # **Same as in compute**
    graph = ...
    keys = ...

    # 2. Computation
    # --------------
    # **Same as in compute**
    results = ...

    # 3. Postpersist
    # --------------
    output = []
    # Iterate over the results and collections
    for res, collection in zip(results, collections):
        # res has the same structure as keys
        keys = collection.__dask_keys__()
        # Get the computed graph for this collection.
        # Here flatten converts a nested list into a single list
        subgraph = {k: r for (k, r) in zip(flatten(keys), flatten(res))}

        # Rebuild the output dask collection with the computed graph
        rebuild, extra_args = collection.__dask_postpersist__()
        out = rebuild(subgraph, *extra_args)

        output.append(out)

    # dask.persist always returns tuples
    return tuple(output)
```

### Optimize

The operation of ``optimize`` can be broken into two stages:

1. **Graph Merging & Optimization**

   Same as in ``compute``.

2. **Rebuilding**

   Similar to ``persist``, the ``rebuild`` function and arguments from
   ``__dask_postpersist__`` are used to reconstruct equivalent collections from
   the optimized graph.

In pseudocode, this looks like the following:

```python
def optimize(*collections, **kwargs):
    # 1. Graph Merging & Optimization
    # -------------------------------
    # **Same as in compute**
    graph = ...

    # 2. Rebuilding
    # -------------
    # Rebuild each dask collection using the same large optimized graph
    output = []
    for collection in collections:
        rebuild, extra_args = collection.__dask_postpersist__()
        out = rebuild(graph, *extra_args)
        output.append(out)

    # dask.optimize always returns tuples
    return tuple(output)
```

### Visualize

Visualize is the simplest of the 4 core functions. It only has two stages:

1. **Graph Merging & Optimization**

   Same as in ``compute``.

2. **Graph Drawing**

   The resulting merged graph is drawn using ``graphviz`` and outputs to the
   specified file.

In pseudocode, this looks like the following:

```python
    def visualize(*collections, **kwargs):
    # 1. Graph Merging & Optimization
    # -------------------------------
    # **Same as in compute**
    graph = ...

    # 2. Graph Drawing
    # ----------------
    # Draw the graph with graphviz's `dot` tool and return the result.
    return dot_graph(graph, **kwargs)
```

## Adding the Core Dask Methods to Your Class

Defining the above interface will allow your object to used by the core Dask
functions (``dask.compute``, ``dask.persist``, ``dask.visualize``, etc.). To
add corresponding method versions of these, you can subclass from
``dask.base.DaskMethodsMixin`` which adds implementations of ``compute``,
``persist``, and ``visualize`` based on the interface above.

---------

## Example Dask Collection

Here we create a Dask collection representing a tuple.  Every element in the
tuple is represented as a task in the graph.  Note that this is for illustration
purposes only - the same user experience could be done using normal tuples with
elements of ``dask.delayed``:

```python
import dask
from dask.base import DaskMethodsMixin, replace_name_in_key
from dask.optimization import cull

# We subclass from DaskMethodsMixin to add common dask methods to our
# class. This is nice but not necessary for creating a dask collection.
class Tuple(DaskMethodsMixin):
    def __init__(self, dsk, keys):
        # The init method takes in a dask graph and a set of keys to use
        # as outputs.
        self._dsk = dsk
        self._keys = keys

    def __dask_graph__(self):
        return self._dsk

    def __dask_keys__(self):
        return self._keys

    @staticmethod
    def __dask_optimize__(dsk, keys, **kwargs):
        # We cull unnecessary tasks here. Note that this isn't necessary,
        # dask will do this automatically, this just shows one optimization
        # you could do.
        dsk2, _ = cull(dsk, keys)
        return dsk2

    # Use the threaded scheduler by default.
    __dask_scheduler__ = staticmethod(dask.threaded.get)

    def __dask_postcompute__(self):
        # We want to return the results as a tuple, so our finalize
        # function is `tuple`. There are no extra arguments, so we also
        # return an empty tuple.
        return tuple, ()

    def __dask_postpersist__(self):
        # We need to return a callable with the signature
        # rebuild(dsk, *extra_args, rename: Mapping[str, str] = None)
        return Tuple._rebuild, (self._keys,)

    @staticmethod
    def _rebuild(dsk, keys, *, rename=None):
        if rename is not None:
            keys = [replace_name_in_key(key, rename) for key in keys]
        return Tuple(dsk, keys)

    def __dask_tokenize__(self):
        # For tokenize to work we want to return a value that fully
        # represents this object. In this case it's the list of keys
        # to be computed.
        return self._keys
```

Demonstrating this class:

```python
from operator import add, mul

# Define a dask graph
dsk = {"k0": 1,
       ("x", "k1"): 2,
       ("x", 1): (add, "k0", ("x", "k1")),
       ("x", 2): (mul, ("x", "k1"), 2),
       ("x", 3): (add, ("x", "k1"), ("x", 1))}

# The output keys for this graph.
# The first element of each tuple must be the same across the whole collection;
# the remainder are arbitrary, unique hashables
keys = [("x", "k1"), ("x", 1), ("x", 2), ("x", 3)]
x = Tuple(dsk, keys)
x
```

### Compute
Turns Tuple into a tuple

```python
x.compute()
```

### Persist
Turns Tuple into a Tuple

```python
x2 = x.persist()
isinstance(x2, Tuple)
```

with each task already computed:

```python
x2.__dask_graph__()
```

## Checking if an object is a Dask collection

To check if an object is a Dask collection, use
``dask.base.is_dask_collection``:

```python
from dask.base import is_dask_collection
from dask import delayed

x = delayed(sum)([1, 2, 3])
assert is_dask_collection(x) is True
assert is_dask_collection(1) is False
```

## Implementing Deterministic Hashing

Dask implements its own deterministic hash function to generate keys based on
the value of arguments.  This function is available as ``dask.base.tokenize``.
Many common types already have implementations of ``tokenize``, which can be
found in ``dask/base.py``.

When creating your own custom classes, you may need to register a ``tokenize``
implementation. There are two ways to do this:

1. The ``__dask_tokenize__`` method

   Where possible, it is recommended to define the ``__dask_tokenize__`` method.
   This method takes no arguments and should return a value fully
   representative of the object.

2. Register a function with ``dask.base.normalize_token``

   If defining a method on the class isn't possible or you need to
   customize the tokenize function for a class whose super-class is
   already registered (for example if you need to sub-class built-ins),
   you can register a tokenize function with the ``normalize_token``
   dispatch.  The function should have the same signature as described
   above.

In both cases the implementation should be the same, where only the location of the
definition is different.

> Both Dask collections and normal Python objects can have implementations of
> ``tokenize`` using either of the methods described above.

### Example

Define a tokenize implementation using a method on the class

```python
from dask.base import tokenize

class Foo:
    def __init__(self, a, b):
        self.a = a
        self.b = b

    def __dask_tokenize__(self):
        # This tuple fully represents self
        return (Foo, self.a, self.b)

x = Foo(1, 2)
assert tokenize(x) == tokenize(x)  # token is deterministic
tokenize(x)
```

Register an implementation with normalize_token

```python
from dask.base import tokenize, normalize_token

class Bar:
    def __init__(self, x, y):
        self.x = x
        self.y = y

@normalize_token.register(Bar)
def tokenize_bar(x):
    return (Bar, x.x, x.y)

y = Bar(1, 2)
assert tokenize(y) == tokenize(y)
assert tokenize(y) != tokenize(x)  # tokens for different objects aren't equal
tokenize(y)
```

For more examples, see ``dask/base.py`` or any of the built-in Dask collections.
