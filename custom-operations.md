# Advanced Collections: _Custom Operations_

There are many algorithms that are pre-defined for different types of Dask collections
(such as Arrays, DataFrames and Bags). Sometimes for more niche use-cases, these predefined
algorithms are not sufficient.

We have seen that you can wrap arbitrary functions in ``dask.delayed`` to parallelize them,
but when you have a Dask collection already, ``dask.delayed`` doesn't know how to operate on
the blocks. In these cases there are several different ways in which you can set up
custom functions.

-   All collections have a ``map_partitions`` or ``map_blocks`` function, that
    applies a user provided function across every Pandas dataframe or NumPy array
    in the collection.  Because Dask collections are made up of normal Python
    objects, it's often straightforward to map custom functions across partitions of a
    dataset without much modification.

    ```python
    df.map_partitions(my_custom_func)
    ```

-   More complex ``map_*`` functions.  Sometimes your custom behavior isn't
    embarrassingly parallel, but requires more advanced communication.  For
    example maybe you need to communicate a little bit of information from one
    partition to the next, or maybe you want to build a custom aggregation.

    Dask collections include methods for these as well.

-   For even more complex workloads you can convert your collections into
    individual blocks, and arrange those blocks as you like using Dask Delayed.
    There is usually a ``to_delayed`` method on every collection. Note that this
    is often the slowest approach.

**Related Documentation**

  - [Array Tutorial](https://tutorial.dask.org/03_array.html)
  - [Best Practices](https://docs.dask.org/en/latest/best-practices.html#learn-techniques-for-customization)

## Block Computations


**Related Documentation**

   - [dask.array.map_blocks](https://docs.dask.org/en/latest/array-api.html?highlight=map_blocks#dask.array.Array.map_blocks)
   - [`dask.dataframe.map_partitions`](http://dask.pydata.org/en/latest/dataframe-api.html#dask.dataframe.DataFrame.map_partitions)


### Array
```python
import dask.array as da
a = da.arange(6, chunks=3)

result = a.map_blocks(lambda x: x * 2)
result.compute()
```



### DataFrame

```python
import pandas as pd
import dask.dataframe as dd
df = pd.DataFrame({'x': [1, 2, 3, 4, 5],
                   'y': [1., 2., 3., 4., 5.]})
ddf = dd.from_pandas(df, npartitions=2)

result = ddf.map_partitions(lambda df, threshold: (df.x + df.y) > threshold, threshold=4)
result.compute()
```

In practice ``map_partitions`` is used to implement many of the helper dataframe methods
that let Dask dataframe mimic Pandas. Here is the implementation of `ddf.index` for instance:

```python
@property
def index(self):
    """Return dask Index instance"""
    return self.map_partitions(
        getattr,
        "index",
        token=self._name + "-index",
        meta=self._meta.index,
        enforce_metadata=False,
    )
```
[source](https://github.com/dask/dask/blob/09862ed99a02bf3a617ac53b116f9ecf81eea338/dask/dataframe/core.py#L458-L467)



### When not to use ``map_blocks`` ``map_partitions``

Both ``map_blocks`` and ``map_partitions`` operate on each block in isolation. This
makes them ill-suited for operations that depend on outcomes in other chunks.
It also means that there will always be at least one result per block.

When you need the edges of one block in the next block you can use Overlapping Computations

## Overlapping Computations

**Related Documentation**
   - [Array Overlap](https://docs.dask.org/en/latest/array-overlap.html)

## Reduction

**Related Documentation
   - [`dask.array.reduction`](http://dask.pydata.org/en/latest/array-api.html#dask.dataframe.Array.reduction)
   - [`dask.dataframe.reduction`](http://dask.pydata.org/en/latest/dataframe-api.html#dask.dataframe.DataFrame.reduction)

## Blockwise Computations

Blockwise computations provide the infrastructure for implementing

**Related Documentation**

   - [API Documentation](https://docs.dask.org/en/latest/array-api.html#dask.array.blockwise)

```python
import dask.array as da
import operator, numpy as np, dask.array as da
x = da.from_array([[1, 2],
                   [3, 4]], chunks=(1, 2))
y = da.from_array([[10, 20],
                   [0, 0]])
z = da.blockwise(operator.add, 'ij', x, 'ji', y, 'ij')
z.compute()
```


## Groupby Aggregation
