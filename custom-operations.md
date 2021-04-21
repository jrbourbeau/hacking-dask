# Advanced Collections: _Custom Operations_

There are many algorithms that are pre-defined for different types of Dask collections
(such as Arrays, DataFrames and Bags). Sometimes for more niche use-cases, these predefined
algorithms are not sufficient.

You can wrap arbitrary functions in ``dask.delayed`` to parallelize them,
but when you are operating on a a Dask collection or several Dask collections,
``dask.delayed`` won't understand the organization of your blocks.

In these cases there are several different ways in which you can set up
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
Block computations operate on a per-block basis. They are simple to think about, but can be tricky to get right.
We'll explore some best practices for setting them up.

**Related Documentation**

   - [dask.array.map_blocks](https://docs.dask.org/en/latest/array-api.html?highlight=map_blocks#dask.array.Array.map_blocks)
   - [`dask.dataframe.map_partitions`](http://dask.pydata.org/en/latest/dataframe-api.html#dask.dataframe.DataFrame.map_partitions)


### Array

Here is a straightforward case of `map_blocks` on a Dask array.

```python
import dask.array as da

a = da.arange(0, 6, chunks=3)

result = a.map_blocks(lambda x: x * 2)
result.compute()
```

When we compute the result, we see that every item in ``a`` is squared.

This is equivalent to:

```python
same = a**2
same.compute()
```

We can use ``.visualize`` to convince ourselves that the task graph for both these
operations has the same structure. Two entirely independent "towers":

```python
result.visualize()
```

```python
same.visualize()
```

When the operation is more convoluted it can help to start by writing a function that works as expected
on one block before passing it to ``map_blocks``.

#### Exercise
Write a function that takes the first element of the block and subtracts it from all the following items.

<details>

```python
def func(block):
    return block - block[0]
```

</details>

Test the function out on any block of ``a``: ``a.blocks[1]``.

<details>

```python
func(a.blocks[1]).compute()
# array([0, 1, 2])
```

</details>

Now pass that function to ``map_blocks``

<details>

```python
da.map_blocks(func, a).compute()
array([0, 1, 2, 0, 1, 2])
```

</details>


#### Multiple arrays

Map_blocks can be used to combine several Dask arrays. When multiple arrays are passed, ``map_blocks``
aligns blocks by block positions without regard to shape.

In the following example we have two arrays with the same number of blocks
but with different shape and chunk sizes.

```python
import numpy as np
import dask.array as da

x = da.arange(1000, chunks=(100,))
y = da.arange(100, chunks=(10,))

def func(a, b):
    return np.array([a.max(), b.max()])

da.map_blocks(func, x, y, chunks=(2,))
```

In the example above we explicitly declare what the size of the output chunks will be ``chunks=(2,)`` this
is short-hand for ``chunks=((2, 2, 2, 2, 2, 2, 2, 2, 2, 2,),)`` meaning 10 blocks each with shape ``(2,)``.

Specifying the output chunks is very useful when doing more involved operations with ``map_blocks``

#### Exercise

See what happens when you don't specify ``chunks`` in the operation above.

#### Special arguments

There are special arguments (``block_info`` and ``block_id``) that you can use within ``map_blocks`` functions.
Let's use the case from above and print ``block_info`` so that we can get a sense of what's going on:

```python
import numpy as np
import dask.array as da

x = da.arange(1000, chunks=(100,))
y = da.arange(100, chunks=(10,))

def func(a, b, block_info=None):
    print(block_info)
    return np.array([a.max(), b.max()])

da.map_blocks(func, x, y, chunks=(2,)).compute()
```

One of the use cases for the ``block_info`` and ``block_id`` arguments is to create an array from scratch.

In the following example we first create an empty array with the desired number of chunks: 10x10. Then we
fill each chunk with

```python
import numpy as np
import dask.array as da

x = da.empty(100, shape=(10, 10), chunks=(1, 1))

def generate_data(a, block_id=None):
    ii, jj = block_id
    return np.arange(ii*200, (ii+1)*200).reshape((10, 20))

da.map_blocks(generate_data, x, chunks=(10, 20), dtype=int)
```

This example might feel contrived, but it can be useful when creating custom IO operations
especially in a distributed context.

#### Exercise

Say you have a set of images that each represent a particular portion of a scene. How can you use the
technique we just learned to patch them together?


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

#### Internal uses
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

**Related Documentation**
   - [`dask.array.reduction`](http://dask.pydata.org/en/latest/array-api.html#dask.dataframe.Array.reduction)
   - [`dask.dataframe.reduction`](http://dask.pydata.org/en/latest/dataframe-api.html#dask.dataframe.DataFrame.reduction)

## Blockwise Computations

Blockwise computations provide the infrastructure for implementing ``map_blocks`` and many
of the elementwise methods that make up the Array API.

They present a really powerful way of implementing matrix operations.

**Related Documentation**

   - [API Documentation](https://docs.dask.org/en/latest/array-api.html#dask.array.blockwise)

```python
import dask.array as da
import operator, numpy as np, dask.array as da
x = da.from_array([[1, 2],
                   [3, 4]], chunks=(1, 2))
y = da.from_array([[10, 20],
                   [0, 0]])
z = da.blockwise(operator.add, 'ij', x, 'ji', y, 'ji')
z.compute()
```

Now try switching the block pattern of the output:

```python
z = da.blockwise(operator.add, 'ji', x, 'ji', y, 'ij')
z.compute()
```

In each of these case the outcome for each block is the same. But in the first case
the blocks are placed side-by-side (shape=(4, 2)) and in the second they are stacked vertically
(shape=(2, 4))

### Internal uses

This is the internal definition of transpose on dask.Array. In it you can see that there is a
regular ``np.transpose`` applied within each block and then the blocks are themselves transposed.

```python
def transpose(a, axes=None):
    if axes:
        if len(axes) != a.ndim:
            raise ValueError("axes don't match array")
    else:
        axes = tuple(range(a.ndim))[::-1]
    axes = tuple(d + a.ndim if d < 0 else d for d in axes)
    return blockwise(
        np.transpose, axes, a, tuple(range(a.ndim)), dtype=a.dtype, axes=axes
    )
```
[source](https://github.com/dask/dask/blob/4569b150db36af0aa9d9a8d318b4239a78e2eaec/dask/array/routines.py#L161:L170)

## Groupby Aggregation
