# Advanced Collections: _Custom Operations_

There are many algorithms that are pre-defined for different types of Dask collections
(such as Arrays, DataFrames and Bags). Sometimes for more niche use-cases, these predefined
algorithms are not sufficient.

You can wrap arbitrary functions in ``dask.delayed`` to parallelize them,
but when you are operating on a Dask collection or several Dask collections,
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
technique we just learned to patch them together? There's a puzzle in the puzzle directory:

```python
from imageio import imread
import matplotlib.pyplot as plt

image = imread("puzzle/bicycle.png")
plt.imshow(image)
```

Now use ``map_blocks`` to read in the puzzle pieces from "bicycle_ii_jj.png"

<details>

```python
from imageio import imread
import matplotlib.pyplot as plt

a = da.empty(shape=(2, 2, 4), chunks=((1, 1), (1, 1), (4,)))

def reader(block, block_id=None):
    ii, jj, _ = block_id
    return imread(f"puzzle/bicycle_{ii}_{jj}.png")

result = da.map_blocks(reader, a, dtype=int, chunks=((24, 24), (24, 24), (4)))
plt.imshow(result)
```

</details>

### DataFrame

There is a similar notion in Dask dataframe called ``map_partitions``.

Here is an example of using it to check if the sum of two columns is greater than some
arbitrary ``threshold``.

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

When you need the edges of one block in the next block you can use overlapping computations.

## Overlapping Computations
Sometimes you want to operate on a per-block basis, but you need some information from neighboring blocks. Example operations include the following:

- Convolve a filter across an image
- Rolling sum/mean/max, …
- Search for image motifs like a Gaussian blob that might span the border of a block
- Evaluate a partial derivative

Dask Array supports these operations by creating a new array where each block is slightly expanded by the borders of its neighbors. This costs an excess copy and the communication of many small chunks, but allows localized functions to evaluate in an embarrassingly parallel manner.

**Related Documentation**
   - [Array Overlap](https://docs.dask.org/en/latest/array-overlap.html)

The main API for these computations is the ``map_overlap`` method. The ``map_overlap`` method is very similar to ``map_blocks`` but has the additional arguments: ``depth``, ``boundary``, and ``trim``.

Here is an example of calculating the derivative:

```python
import numpy as np
import dask.array as da

x = np.array([1, 1, 2, 3, 3, 3, 2, 1, 1])
x = da.from_array(x, chunks=5)

def derivative(x):
    return x - np.roll(x, 1)

y = x.map_overlap(derivative, depth=1, boundary=0)
y.compute()
```

In this case each block shares 1 value from its neighboring block - ``depth``. And since we set ``boundary=0``on the outer edges of the array, the first and last block are padded with the integer 0.

Since we haven't specified ``trim`` it is true by default meaning that the overlap is removed before returning the results.

![](https://docs.dask.org/en/latest/_images/overlapping-neighbors.png)

If you inspect the task graph (`y.visualize()`) you'll see two mostly independent towers of tasks, with just some value sharing at the edges.

### Exercise
Lets apply a gaussian filter to an image following the example from the [scipy docs](https://docs.scipy.org/doc/scipy/reference/generated/scipy.ndimage.gaussian_filter.html)

First create a dask array from the numpy array:

```python
from scipy import misc
import dask.array as da

a = da.from_array(misc.ascent(), chunks=(128, 128))
```

Now use ``map_overlap`` to apply ``gausian_filter`` to each block.

<details>

```python
from scipy.ndimage import gaussian_filter

b = a.map_overlap(gaussian_filter, depth=10, sigma=5, boundary="periodic")
```

</details>

Now we can plot the results:

```python
import matplotlib.pyplot as plt

fig, (ax1, ax2) = plt.subplots(1, 2)
ax1.imshow(a)
ax2.imshow(b)
plt.show()
```

Notice that if you set the depth to a smaller value, you can see the edges of the blocks in the output image.

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
z = da.blockwise(operator.add, 'ji', x, 'ji', y, 'ji')
z.compute()
```

Try switching the block pattern of the output:

```python
z = da.blockwise(operator.add, 'ij', x, 'ji', y, 'jj')
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

## Reduction
All of the methods that we have covered so far assume that the output array will be similar to the input array. But where dask really shines is with aggregations. Each dask collection has a `reduction` method that is the generalized method that supports operations that reduce the dimensionality of the inputs.

**Related Documentation**
   - [`dask.array.reduction`](http://dask.pydata.org/en/latest/array-api.html#dask.dataframe.Array.reduction)
   - [`dask.dataframe.reduction`](http://dask.pydata.org/en/latest/dataframe-api.html#dask.dataframe.DataFrame.reduction)


### Internal uses

This is the internal definition of sum on dask.Array. In it you can see that there is a
regular ``np.sum`` applied across each block and then tree-reduced with ``np.sum`` again.

```python
def sum(a, axis=None, dtype=None, keepdims=False, split_every=None, out=None):
    if dtype is None:
        dtype = getattr(np.zeros(1, dtype=a.dtype).sum(), "dtype", object)
    result = reduction(
        a,
        chunk.sum,  # this is just `np.sum`
        chunk.sum,  # this is just `np.sum`
        axis=axis,
        keepdims=keepdims,
        dtype=dtype,
        split_every=split_every,
        out=out,
    )
    return result
```

In this case the  ``chunk``, ``combine`` and ``aggregate`` functions are all the same.

```python
import numpy as np
import dask.array as da

a = da.from_array(np.arange(1000000).reshape(1000, 1000), chunks=(500, 200))
b = a.sum()
```

By visualizing `b` we can see how the tree reduction works. First the ``chunk`` function is applied to each block, then every 4 chunks are combined using the ``combine`` function. This keeps going until there are only 2 chunks left, then the ``aggregate`` function is used to finish up.

### Exercise

See how the graph changes when you set the chunks to `(500, 500)` or `(500, 300)`

### Controlling reduction with kwargs

There are a few handy keyword arguments that you can use to control the shape of the task graph.

The most useful of these are ``split_every`` which controls the number of chunk outputs that are used as input to each ``combine`` call. Try setting split_every on ``a.sum(split_every={0: 2, 1: 5})`` and visualizing the task graph to see the impact.

> **Side note**
> You can use reductions to calculate aggregations per-block reduction even if you don't want to combine and aggregate the results of those blocks:
>
> ```python
> da.reduction(a, np.sum, lambda x, **kwargs: x, dtype=int).compute()
> ```

## Groupby Aggregation

There are many standard reductions supported by default on dataframe groupbys. These include methods like `mean, max, min, sum, nunique`. These are easily scaled and parallelized.

**Related Documentation**

   - [DataFrame Groupby](https://docs.dask.org/en/latest/dataframe-groupby.html#aggregate)
   - [Examples](https://examples.dask.org/dataframes/02-groupby.html)

If you are trying to run a custom function on the groups in a groupby it can be tempting to use `.apply` but this is often a poor choice because it requires that the data be shuffled. Instead you should try writing a custom aggregation.

In order to do that you need to write three functions:

- `chunk`: operates on the series groupby on each individual partition (`ddf.partitions[0].groupby("name")["x"]`)
- `aggregate`: operates on the concatenated output from calling chunk on every partition
- `finalize`: operates on the output from calling aggregate - returns one column. This one is actually optional.

Here's an example of a custom aggregation for calculating the mean.

```python
import dask
import dask.dataframe as dd

ddf = dask.datasets.timeseries()

custom_mean = dd.Aggregation(
    'custom_mean',
    lambda s: print(type(s)); (s.count(), s.sum()),
    lambda count, sum: (count.sum(), sum.sum()),
    lambda count, sum: sum / count,
)
ddf.groupby('name').agg(custom_mean)
```

Here is how it works:

- every partition (one per day) group by ``name``
- on each of those pandas series groupby objects calculate the `count` and the `sum`
- concatenate every 8 (this is configurable) outputs together
- sum of each of these
- finally: divide the `sum` by the `count`

This is equivalent to:

```python
ddf.groupby('name').mean()
```

> NOTE: If you look at the task graph you'll see that the structure of the computation is actually pretty different. That's because `.mean` computes the `sum` and the `count` independently and only combines the values at the end.

Similarly you could use apply (DON'T DO THIS)

```python
ddf.groupby("name").apply(lambda x: x.mean())
```

This will shuffle the data so that all the data for a particular name is in the same partition. If you call `.compute()` on it you'll notice that it's much slower (about 50x on my computer).

### Exercise

Write a custom aggregation to calculate `value_counts`.

<details>

```python
import dask
import dask.dataframe as dd

ddf = dask.datasets.timeseries()

custom_value_counts = dd.Aggregation(
    'custom_value_counts',
    lambda s: s.value_counts(),
    lambda counts: counts.sum(),
)
ddf.groupby('name').agg(custom_value_counts)
```

</details>
