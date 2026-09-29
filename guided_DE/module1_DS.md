---
title: "Python for Data Science"
subtitle: "Module 01 — Idioms, Types, Records, Paths, Profiling"
date: "2026-04-11"
author: "Claude Opus / Sonnet (writing) - W. Fauriat (supervision)"
toc: true
toc-depth: 3
numbersections: true
colorlinks: true
geometry: "margin=1in"
fontsize: "11pt"
---

# Module 01 — Python for Data Science

> **Core Question**: What does a data scientist need to know about *Python the language* — not the libraries — to write code that is fast enough, correct, reproducible, and pleasant to read a year later?

**In this module:**

- [Why this module exists](#why-this-module-exists)
- [Pythonic idioms for data code](#pythonic-idioms-for-data-code)
- [Type hints without tears](#type-hints-without-tears)
- [Records: dataclass, NamedTuple, TypedDict](#records-dataclass-namedtuple-typeddict)
- [pathlib: filesystem paths done right](#pathlib-filesystem-paths-done-right)
- [Environments and reproducibility](#environments-and-reproducibility)
- [Profiling Python code](#profiling-python-code)
- [Logging instead of `print`](#logging-instead-of-print)
- [What to remember & cheatsheet](#what-to-remember--cheatsheet)

---

## Why this module exists

If you are reading this you probably already *know* Python — you can write a loop, a function, a class, you have opened a notebook, imported `pandas`, and produced a plot. This module is not about teaching Python from scratch. It is about the *delta* between "I can make Python run" and "I can write Python code that a teammate will gladly inherit, that runs fast enough not to be the bottleneck of my analysis, and that will reproduce the same result next quarter when I come back to it."

Most data science Python code that causes grief falls into a small number of recognizable categories. It is slow for reasons the author does not understand, because they never profiled it. It breaks in production because a `None` slipped through a function that silently assumed a float. It cannot be re-run because the dependencies drifted and nobody pinned them. Its file paths are cobbled together with `os.path.join` and string concatenation and only work on Linux. Its custom "record" types are nested dicts that nobody can validate. These are not library problems — they are language-and-tooling problems, and they all have standard solutions that live in the Python standard library or a thin layer on top of it.

The rest of this module walks through those solutions in the order they matter: idioms first (because they change how every line you write reads), types and records (because they structure your data), paths (because data lives on disk), environments (because reproducibility is table stakes), profiling (because you cannot optimize what you cannot measure), and logging (because `print` does not scale). The NumPy, pandas, and visualization modules that follow assume you are comfortable with everything here.

### The scientific Python ecosystem, one paragraph

It is worth having a mental map of where the ecosystem sits, because nearly everything you will touch in later modules is either part of it or built on top of it. At the bottom is **NumPy**, which provides the n-dimensional array and the linear algebra primitives; essentially every other numerical library either wraps NumPy arrays directly or uses the same memory layout. On top of NumPy sits **pandas** for labeled tabular data, **SciPy** for scientific algorithms (optimization, signal processing, sparse matrices, statistics), **scikit-learn** for classical machine learning, **matplotlib** (and its descendants `seaborn`, `plotly`) for visualization, and **statsmodels** for statistical modeling with a heavier inferential emphasis. Slightly off to the side, **PyTorch**, **JAX**, and **TensorFlow** provide tensors that look almost exactly like NumPy arrays but add GPU support and automatic differentiation. And above all of them are the storage and orchestration layers — `pyarrow`, `fsspec`, `dask`, `polars`, `duckdb` — which handle data that does not fit in memory or lives in cloud storage. You do not need all of these at once, but you do need to understand that they all ultimately produce and consume arrays of numbers in a handful of standard shapes.

The version of Python you should assume throughout this course is **Python 3.11 or newer**. A lot of the syntax that makes modern Python pleasant — `list[int]` instead of `List[int]`, pattern matching, `Self` type, `ExceptionGroup`, the performance improvements in 3.11 and 3.12 — is only available in recent versions, and sticking to 3.8 for compatibility reasons is increasingly not worth the cost.

---

## Pythonic idioms for data code

Python has a strong aesthetic. Code that reads naturally in Python tends to be short, declarative, and built out of a small vocabulary of iteration primitives. Code that reads awkwardly in Python usually does so because it is written in the dialect of another language — C-style index loops, Java-style boilerplate classes, shell-style string bashing. The first skill to develop is recognizing when you are writing the awkward dialect and reaching for the idiomatic alternative.

None of what follows is about style for its own sake. Idiomatic Python is usually faster (because it dispatches to C-level loops inside the interpreter), shorter (less space for bugs), and more composable (the standard library is built to chain together), so the payoff is practical.

### Iteration: enumerate, zip, and friends

The first sign of un-Pythonic data code is an index loop that only exists to index into a list:

```python
# un-Pythonic
for i in range(len(rows)):
    row = rows[i]
    process(i, row)

# Pythonic
for i, row in enumerate(rows):
    process(i, row)
```

`enumerate` yields `(index, item)` pairs. If you need to start counting from a value other than zero, pass `start=`: `enumerate(rows, start=1)`. This is the right pattern for producing one-indexed output, for example.

When you need to iterate over two (or more) sequences in lockstep, `zip` is the tool:

```python
for name, age in zip(names, ages):
    ...
```

A long-standing trap with `zip` is that, prior to Python 3.10, it would silently truncate to the shortest input if the sequences had different lengths. If your lists are supposed to be parallel — say, one list of features and one list of labels — silent truncation can turn a real bug (a dropped row) into a result that looks plausible but is wrong. As of Python 3.10 you can pass `strict=True` to raise a `ValueError` when lengths disagree:

```python
for x, y in zip(features, labels, strict=True):
    ...
```

Make a habit of using `strict=True` whenever you believe the inputs should be the same length. It catches exactly the class of data-alignment bug that is painful to find later.

`reversed(xs)` iterates a sequence backwards without copying it. `sorted(xs, key=fn, reverse=True)` returns a new sorted list and takes a key function — if you find yourself writing a `cmp` function, stop and express it as a key instead. For grouping purposes, `sorted` followed by `itertools.groupby` is the canonical recipe (see below).

### Comprehensions and generator expressions

A **list comprehension** is a single expression that builds a list by iterating and optionally filtering:

```python
squares = [x * x for x in numbers]
positives = [x for x in numbers if x > 0]
pairs = [(i, x) for i, x in enumerate(rows) if x.is_valid]
```

The equivalent `for` loop with `.append` works, but it is longer, it mutates a variable you just declared, and the interpreter will not optimize it as aggressively. Use the comprehension when the body is a single expression.

There are also **dict comprehensions** (`{k: v for k, v in pairs}`) and **set comprehensions** (`{x.category for x in items}`). Both deserve to be in active vocabulary. A common use of the dict comprehension in data code is inverting a mapping: `{v: k for k, v in mapping.items()}`.

The catch with list comprehensions is that they materialize the entire list in memory. If you are going to consume the result once and immediately throw it away — for example, passing it to `sum` or `max` — use a **generator expression** instead, which produces values lazily and does not build the intermediate list:

```python
total = sum(row.amount for row in rows)            # lazy, no list built
worst = max((row.loss for row in rows), default=0) # lazy
```

The syntactic difference is just `(...)` instead of `[...]`, and when the generator expression is the sole argument to a function you can even drop the outer parentheses: `sum(x * x for x in numbers)`. For large datasets the memory savings can be dramatic. A rule of thumb: if the comprehension result is immediately fed to an aggregator or another iterator, make it a generator; if you are going to index into it or iterate it more than once, make it a list.

Nested comprehensions are legal but usually a smell. A two-level comprehension that fits on one line is fine; anything more should be written as a function with explicit loops, because the reader cannot easily match the `for` clauses to the dimensions they vary.

### Unpacking and the walrus

Python has rich **unpacking** syntax that removes a lot of index-juggling:

```python
a, b, c = triple                    # basic
head, *middle, tail = seq           # starred assignment, captures a list
first, *rest = seq
(x, y), z = point_and_label          # nested
for k, v in d.items(): ...
```

When you receive a tuple from a function, unpack it in the assignment rather than indexing `result[0]`, `result[1]` — the unpacking declares the shape you expect, and if the callee starts returning four values instead of three, you get a clean `ValueError` rather than a silent bug.

The **walrus operator** `:=` (assignment expression) is the one Python 3.8-era addition that is worth internalizing. It lets you bind a name inside an expression, which is useful when you want to use a value in a condition *and* reuse it in the body:

```python
# without walrus — recomputes or duplicates
line = f.readline()
while line:
    process(line)
    line = f.readline()

# with walrus
while line := f.readline():
    process(line)
```

In comprehensions the walrus lets you compute a value once, filter on it, and emit it:

```python
cleaned = [y for x in items if (y := normalize(x)) is not None]
```

Use it where it actually saves a line or a redundant call. Do not sprinkle it over code that was already fine — it is a tool, not an aesthetic.

### The `collections` module: specialized containers

The built-in types (`list`, `dict`, `set`, `tuple`) cover 90% of needs. The `collections` module covers most of the rest, and reaching for it is faster than reinventing it.

**`defaultdict`** creates dict values on demand. It removes the "does this key already exist?" dance that plagues any grouping loop:

```python
from collections import defaultdict

groups = defaultdict(list)
for row in rows:
    groups[row.category].append(row)
# groups is now {category: [rows...]}
```

The factory can be any zero-argument callable: `defaultdict(int)` for counting, `defaultdict(set)` for collecting unique values, `defaultdict(lambda: [0, 0])` for parallel accumulators. A subtle point is that accessing a missing key *creates* it, which is usually what you want but can surprise you when iterating or checking `in` — use `dict.get` if you want a read that does not materialize.

**`Counter`** is a dict subclass specialized for counting:

```python
from collections import Counter

counts = Counter(row.status for row in rows)
counts.most_common(5)               # top 5 as (key, count) pairs
counts["succeeded"] / counts.total() # fraction (Python 3.10+)
```

`Counter` supports addition and subtraction (`counts_jan + counts_feb`), intersection (`&`, taking the minimum per key), and union (`|`, taking the maximum). It is the right tool for any frequency computation on a non-huge iterable; for very large data you would move to pandas `value_counts` or a groupby, which we cover later.

**`deque`** is a double-ended queue with O(1) append and pop on both ends. Lists have O(n) `pop(0)`, so any time you see a sliding-window or FIFO queue being built out of a list, reach for `deque`:

```python
from collections import deque

window = deque(maxlen=10)  # automatically drops old items
for x in stream:
    window.append(x)
    yield sum(window) / len(window)  # moving average
```

The `maxlen` parameter is the killer feature for streaming data: it turns the deque into a fixed-size ring buffer. No rolling-average loop ever needs more than these three lines again.

### `itertools`: composable lazy iteration

`itertools` is the standard library's answer to "how do I compose iterators without writing loops?". The whole module is worth reading through once; a handful of functions cover most data tasks.

**`chain`** flattens several iterables into one:

```python
from itertools import chain

all_rows = chain(train_rows, valid_rows, test_rows)
for row in all_rows: ...
```

`chain.from_iterable(iter_of_iters)` flattens one level of nesting, which is the cleanest way to turn a list-of-lists into a single stream without building an intermediate list.

**`islice`** is `seq[start:stop:step]` for iterators, which do not support slicing directly:

```python
from itertools import islice

first_five = list(islice(stream, 5))
every_tenth = islice(stream, 0, None, 10)
```

This is useful for previewing a generator or reading a file's first N lines without slurping it all.

**`groupby`** collapses *consecutive* equal elements into groups. The "consecutive" is the gotcha: if you want to group by a key function over an unsorted iterable, you must sort first:

```python
from itertools import groupby
from operator import attrgetter

rows.sort(key=attrgetter("category"))
for category, grouped in groupby(rows, key=attrgetter("category")):
    rows_in_cat = list(grouped)   # materialize inside the loop body
    ...
```

Two warnings. First, the `grouped` iterator is exhausted by the next iteration of the outer loop, so if you need to keep the group around, materialize it with `list(grouped)` inside the body. Second, for anything bigger than a toy analysis, pandas `groupby` is more ergonomic and handles the sort-or-hash question for you — use `itertools.groupby` for clean Python pipelines and quick scripts, not as a replacement for DataFrame aggregation.

**`accumulate`** produces running aggregates:

```python
from itertools import accumulate
import operator

running_total = list(accumulate(amounts))                        # cumulative sum
running_max   = list(accumulate(values, operator.max))            # running max
```

**`product`, `combinations`, `permutations`** generate Cartesian products and combinatorial structures, which come up constantly in hyperparameter grids and pairwise comparisons:

```python
from itertools import product

for lr, batch_size, opt in product([1e-3, 1e-4], [32, 64, 128], ["adam", "sgd"]):
    run_experiment(lr, batch_size, opt)
```

`takewhile(pred, it)` and `dropwhile(pred, it)` emit elements as long as (or until) a predicate holds — useful for parsing headers out of a file stream.

A note on `itertools.tee`, which lets you "fork" an iterator into several independent iterators: it works by buffering elements that the slower consumer has not yet consumed, which means if your consumers read at very different rates, memory usage grows unboundedly. For data work you are usually better off either materializing to a list or reading the source twice.

### `functools`: higher-order utilities

**`functools.lru_cache`** (or its alias `functools.cache` for unbounded caching in 3.9+) memoizes a pure function:

```python
from functools import lru_cache

@lru_cache(maxsize=1024)
def load_lookup_table(name: str) -> dict[str, float]:
    ... # expensive, deterministic
```

The requirements are that arguments are hashable and that the function is deterministic — if the underlying data file can change between calls, caching is a bug factory, not an optimization. The cache is per-process and per-interpreter, so it disappears when your script exits.

**`functools.partial`** pre-fills some arguments of a function, producing a new callable:

```python
from functools import partial

logger_info = partial(logger.log, level=logging.INFO)
map_to_floats = partial(map, float)
```

In data code this is most useful when you need to pass a function as a callback (for example to `map`, `pandas.DataFrame.apply`, or an optimizer) but the function expects more parameters than the caller supplies. It is cleaner than a one-line lambda and it pickles, which matters when you want to parallelize.

**`functools.reduce`** folds an iterable with a binary operation. It exists, but in Python it is almost always clearer to write the loop explicitly or use a specialized aggregator (`sum`, `max`, `math.prod`, `operator.or_` with `set.update`). Reserve `reduce` for cases where the accumulator has a non-trivial structure and the fold really is the clearest expression.

**`functools.singledispatch`** gives you ad-hoc polymorphism on the first argument's type. It is useful when you have a processing function that should behave differently for `DataFrame`, `ndarray`, and `dict` inputs, and you want to avoid a chain of `isinstance` checks.

### EAFP vs LBYL

Python has a cultural preference for **EAFP** — *easier to ask forgiveness than permission* — over **LBYL** — *look before you leap*. In practice this means that `try/except` is often more idiomatic than a guard clause, especially for operations that almost always succeed:

```python
# LBYL — racy for I/O, verbose for dicts
if key in d:
    value = d[key]
else:
    value = default

# EAFP
try:
    value = d[key]
except KeyError:
    value = default

# Or, best, use the API that was built for this
value = d.get(key, default)
```

For dictionary access, `dict.get` exists and is the right answer. But for things like parsing — "try to interpret this string as an int, fall back to float, fall back to the original string" — the EAFP chain reads better than a stack of regex probes. The rule of thumb is that if the "happy path" is overwhelmingly likely, you should write the happy path and catch the exception; if the error case is a real possibility and the check is cheap, LBYL is fine.

A corollary is that you should catch *specific* exceptions. `except Exception:` swallows bugs; `except KeyError:` states exactly which case you are handling. Bare `except:` (with no exception class) also catches `KeyboardInterrupt` and `SystemExit`, which almost never is what you want.

---

## Type hints without tears

Python is a dynamically typed language and will happily run functions whose signatures are inconsistent with the values they actually receive. Type hints do not change that — at runtime, `def score(x: float) -> float` accepts any object for `x` and returns any object. What type hints *do* provide is a machine-readable contract that static analyzers (`mypy`, `pyright`, `pyrefly`), IDEs, and humans can use to catch mistakes before they run, to autocomplete attributes, and to document intent without separate docstrings.

The reason type hints matter for data science specifically is that data code is glue code. You are constantly moving between DataFrames, NumPy arrays, Python scalars, and external APIs. Functions get complicated, "a list of dicts returned by the API" becomes a load-bearing assumption, and the bug that passes `None` to a function that assumed a number manifests three frames deep in a vectorized operation with a cryptic message. Types shrink that surface area.

### The basic syntax

Parameter and return annotations use colon syntax, with return types after `->`:

```python
def softmax(x: list[float]) -> list[float]:
    e = [math.exp(xi - max(x)) for xi in x]
    s = sum(e)
    return [ei / s for ei in e]
```

From Python 3.9 onwards, `list[int]`, `dict[str, float]`, `tuple[int, ...]` are valid directly. You do not need to import `List`, `Dict`, `Tuple` from `typing` unless you are targeting older Pythons.

For optional values (anything that can also be `None`), the modern syntax is `int | None`, replacing the older `Optional[int]`:

```python
def parse_age(s: str) -> int | None:
    try:
        return int(s)
    except ValueError:
        return None
```

Union of several types uses the same bar syntax: `int | float | str`. This requires Python 3.10 or newer; on 3.9 you still need `Union[int, float, str]` from `typing`.

`Any` is the escape hatch — it means "anything goes, stop checking". Use it sparingly; a function with `Any` parameters provides no contract.

`Literal["train", "valid", "test"]` says the value must be one of a fixed set of strings. This is the right way to annotate a "mode" parameter and is much more informative than `str`:

```python
from typing import Literal

def load_split(name: Literal["train", "valid", "test"]) -> Dataset: ...
```

`Callable[[int, str], float]` describes a callable taking an int and a str and returning a float. In practice, for anything more complex than a single-argument callback, consider defining a `Protocol` (see below) rather than a deeply nested `Callable`.

### Type aliases

When a type name gets long or appears repeatedly, give it an alias. Python 3.12 introduced the `type` statement for this:

```python
type FeatureMatrix = list[list[float]]
type LabelVector = list[int]

def fit(X: FeatureMatrix, y: LabelVector) -> Model: ...
```

On older Pythons, plain assignment works the same way at runtime: `FeatureMatrix = list[list[float]]`. Static checkers infer that it is a type alias.

### Typing at boundaries, not everywhere

A common mistake is to reach for typing immediately in every notebook cell and every throwaway script, discover it is fiddly, and abandon it. The more useful heuristic is to type at **boundaries**: the public signatures of functions that other code imports, the entry points of library modules, the data classes that travel between pipeline stages. Inside a function, type inference from the checker is usually enough, and over-annotating local variables clutters the code.

For a Jupyter notebook doing exploratory analysis, type hints are often optional; for a module in `src/` that a teammate will import, they are close to mandatory. For the glue that ETLs a DataFrame into a scikit-learn model, types on the ETL function signatures catch a startling fraction of real bugs.

### Protocols: structural typing

Sometimes you care not about the *class* of an argument but its *shape* — "anything that has a `.fit(X, y)` and `.predict(X)` method". That is **structural typing**, and Python expresses it with `typing.Protocol`:

```python
from typing import Protocol

class Estimator(Protocol):
    def fit(self, X, y) -> None: ...
    def predict(self, X): ...

def cross_validate(model: Estimator, X, y) -> float:
    ...
```

Any object with those methods satisfies the protocol; no inheritance is required. This matches Python's duck-typing culture and is especially nice for ML code where scikit-learn, XGBoost, and LightGBM models all expose the same informal interface without sharing a base class.

### Type checking at the command line

Annotations are only useful if something actually checks them. The dominant checkers are **mypy** (the reference implementation, oldest), **pyright** (Microsoft, fast, strict, the engine inside VS Code's Pylance), and **pyrefly** (newer, optimized for very large codebases). For most projects, pick one and add it to your pre-commit hooks. A minimal `mypy` run on a source tree looks like:

```bash
mypy src/
```

With a `pyproject.toml` entry to configure strictness:

```toml
[tool.mypy]
strict = true
files = ["src"]
```

`strict = true` turns on everything: it requires annotations on every function, disallows implicit `Any`, flags unused ignores, and so on. Some codebases adopt it gradually by starting with a lighter configuration and tightening over time; others, especially greenfield projects, find strict mode is worth it from day one.

### Runtime validation: Pydantic and friends

Type hints are a *static* check. They do not guarantee that a dict you parsed from a YAML config file actually has the fields your type says it does. If the data is crossing a trust boundary — coming from a config file, an HTTP API, a user upload — you want a **runtime** validator. **Pydantic** is the standard answer; it reads the same type hints, builds a validator from them, and raises a structured error if the input does not match:

```python
from pydantic import BaseModel

class TrainConfig(BaseModel):
    learning_rate: float
    batch_size: int
    optimizer: Literal["adam", "sgd"]
    warmup_steps: int = 0

config = TrainConfig(**yaml.safe_load(open("train.yaml")))
# raises ValidationError if the file is missing or malformed
```

For ETL and data ingestion, Pydantic is a good default for config objects and small structured records. For larger tabular validation at the DataFrame level, **pandera** does the same thing for pandas / polars DataFrames and is covered in the MLOps module.

---

## Records: dataclass, NamedTuple, TypedDict

Data science code is full of small "record" types — a training config, a metric bundle, a hyperparameter combination, a parsed row. Python gives you several ways to express a record, and picking the right one for each case avoids both over-engineering (building a class hierarchy where a tuple would do) and under-engineering (nesting three levels of dicts where a typed object would have been clearer).

### `@dataclass`: the workhorse

`@dataclass` decorates a class definition and auto-generates `__init__`, `__repr__`, `__eq__`, and optionally a few other dunder methods from the class's annotated attributes:

```python
from dataclasses import dataclass

@dataclass
class TrainConfig:
    learning_rate: float
    batch_size: int
    epochs: int = 10
    optimizer: str = "adam"

cfg = TrainConfig(learning_rate=1e-3, batch_size=32)
print(cfg)  # TrainConfig(learning_rate=0.001, batch_size=32, epochs=10, optimizer='adam')
```

The annotations do double duty: they are read by type checkers *and* by the dataclass machinery to build the constructor. Defaults are specified the same way as normal function defaults; fields with no default must come before fields with one (same rule as function arguments).

There is one gotcha that bites everyone exactly once: **mutable defaults must use `field(default_factory=...)`, not a literal**. If you write `tags: list[str] = []`, every instance of the dataclass shares the same list, which is a nightmare bug. The dataclass machinery refuses to let you do this and raises an error at class-definition time, pointing you at the correct form:

```python
from dataclasses import dataclass, field

@dataclass
class RunLog:
    run_id: str
    tags: list[str] = field(default_factory=list)   # fresh list per instance
    metrics: dict[str, float] = field(default_factory=dict)
```

Other options to the `@dataclass` decorator are worth knowing:

- **`frozen=True`** makes instances immutable. Attempts to assign `cfg.learning_rate = ...` raise `FrozenInstanceError`. Frozen dataclasses are hashable, so you can use them as dict keys or set members — which is exactly what you want for a hyperparameter tuple that is memoizing experiments.

- **`slots=True`** (Python 3.10+) creates `__slots__`, which eliminates the per-instance `__dict__`, shrinks memory usage, and makes attribute access slightly faster. For any dataclass you plan to instantiate more than a few times — including every row of a dataset-like iteration — `slots=True` is essentially free upside.

- **`kw_only=True`** (Python 3.10+) forces all fields to be keyword-only arguments. This is very useful for configs, where positional arguments are unreadable at call sites (`TrainConfig(1e-3, 32, 10, "adam")` is a riddle; `TrainConfig(learning_rate=1e-3, batch_size=32, ...)` is documentation).

A fully loaded, modern config dataclass looks like this:

```python
@dataclass(frozen=True, slots=True, kw_only=True)
class TrainConfig:
    learning_rate: float
    batch_size: int
    epochs: int = 10
    optimizer: Literal["adam", "sgd", "adamw"] = "adam"
```

For post-construction logic (derived fields, validation), define `__post_init__`:

```python
@dataclass
class Range:
    low: float
    high: float

    def __post_init__(self) -> None:
        if self.low > self.high:
            raise ValueError(f"low={self.low} > high={self.high}")
```

To convert a dataclass to a plain dict (for JSON serialization, say), use `dataclasses.asdict(instance)`; `astuple` is the tuple equivalent. Note that `asdict` recurses into nested dataclasses, lists, and dicts, which is almost always what you want.

### `NamedTuple`: when you want a tuple

`typing.NamedTuple` is a typed, immutable record that is also a regular tuple. You can unpack it, index into it, and pass it to any code that expects a tuple:

```python
from typing import NamedTuple

class MetricBundle(NamedTuple):
    accuracy: float
    f1: float
    roc_auc: float

m = MetricBundle(accuracy=0.92, f1=0.88, roc_auc=0.95)
print(m.accuracy)        # attribute access
a, f, r = m              # tuple unpacking
m[0]                      # positional indexing (== m.accuracy)
```

`NamedTuple` and a frozen dataclass are nearly interchangeable for small records; the decision is about what you want the *identity* of the object to be. Use `NamedTuple` when the record is genuinely a tuple — an (x, y) coordinate, a confidence interval's (low, high), a function returning multiple values — and unpacking is natural. Use a frozen dataclass when the record is a struct with a dozen optional fields, because keyword arguments and attribute access scale better than positional tuples.

A subtle difference: `NamedTuple` instances compare equal to regular tuples with the same values (`MetricBundle(1, 2, 3) == (1, 2, 3)` is `True`), which is sometimes convenient and sometimes a footgun. Dataclasses only compare equal to other instances of the same class.

### `TypedDict`: describing existing dict shapes

Sometimes the data is already a dict — parsed from JSON, returned from a library, stored in a database column — and converting it to a class is more work than it is worth. `TypedDict` lets you annotate the expected keys and value types without changing the runtime representation:

```python
from typing import TypedDict

class UserRecord(TypedDict):
    user_id: int
    email: str
    signup_date: str
    is_premium: bool

def process(user: UserRecord) -> None:
    if user["is_premium"]:
        ...
```

At runtime, `UserRecord` instances are plain dicts; there is no class instantiation, no method lookup, no memory overhead. Static checkers verify that callers pass a dict with the right keys and types, and they flag `user["is_premion"]` as an error because it does not match any key.

Use `TypedDict` when:
- The data is already dict-shaped and you are adding types retroactively.
- You need JSON-native serialization with no conversion step.
- The record has a large number of optional fields (use `total=False` or, in 3.11+, mark individual fields with `NotRequired`).

Use a dataclass when:
- You want methods on the record.
- You want immutability (`frozen=True`) or slot-based memory savings.
- You are designing the record from scratch and have no reason to keep it as a dict.

### Choosing between them

| Need                                      | Reach for             |
|-------------------------------------------|-----------------------|
| Struct with methods, maybe immutable      | `@dataclass`          |
| Hyperparameter tuple, dict key, hashable  | `@dataclass(frozen=True, slots=True)` |
| Multiple return values from a function    | `NamedTuple`          |
| Already-dict data with fixed schema       | `TypedDict`           |
| Runtime-validated config from YAML/JSON   | `pydantic.BaseModel`  |
| Tabular rows at scale                     | pandas/polars DataFrame (covered in Module 03) |

The last row is the most important: none of these is the right answer for a million rows. Records are for hundreds or thousands of items where each one has real identity. For bulk data, columnar containers win on every axis.

---

## pathlib: filesystem paths done right

For years the standard way to work with filesystem paths in Python was `os.path` — a collection of string-manipulation functions that happened to know about path separators. `pathlib`, introduced in Python 3.4 and steadily improved since, replaces all of that with a `Path` object that is far more pleasant to use. There is essentially no reason to use `os.path` in new code; every modern library accepts `Path` objects directly.

The two mental shifts are that a `Path` is an object, not a string, and that you build paths by *joining* with the `/` operator rather than concatenating strings.

### Constructing and joining

```python
from pathlib import Path

root = Path("/data/experiments")
run_dir = root / "run_042" / "artifacts"
csv = run_dir / "metrics.csv"

print(csv)   # /data/experiments/run_042/artifacts/metrics.csv
```

The `/` operator dispatches to `Path.__truediv__`, which joins with the OS-appropriate separator. It also normalizes double slashes, handles absolute paths on the right-hand side (they replace the left), and does all the edge cases that string concatenation gets wrong. You can chain as many `/` joins as you want.

Common constructors and factories:

```python
Path.cwd()              # current working directory
Path.home()             # user's home directory
Path.home() / ".cache"  # common pattern for caching
Path(__file__).parent   # directory containing the current source file
```

The `Path(__file__).parent` idiom is the answer to "how do I reference a data file next to my script?". It makes the script robust to being run from any directory.

### Inspecting paths

A `Path` has a rich set of properties for pulling apart its components without string surgery:

```python
p = Path("/data/experiments/run_042/metrics.csv")

p.name       # 'metrics.csv'          — last component
p.stem       # 'metrics'              — name without suffix
p.suffix     # '.csv'                 — last suffix (includes the dot)
p.suffixes   # ['.csv']               — all suffixes, for 'model.tar.gz'
p.parent     # Path('/data/experiments/run_042')
p.parents[1] # Path('/data/experiments')  — grandparent
p.parts      # ('/', 'data', 'experiments', 'run_042', 'metrics.csv')
```

`with_suffix` and `with_name` produce new paths with one component replaced — invaluable for derived filenames:

```python
csv = Path("/data/metrics.csv")
parquet = csv.with_suffix(".parquet")         # /data/metrics.parquet
backup  = csv.with_name("metrics.csv.bak")    # /data/metrics.csv.bak
```

### Filesystem queries

Boolean queries on what the path actually points to:

```python
p.exists()       # does anything exist at this path?
p.is_file()      # is it a regular file?
p.is_dir()       # is it a directory?
p.is_symlink()   # is it a symlink?
```

For metadata, `p.stat()` returns the same `os.stat_result` as `os.stat`; use `.st_size` for file size in bytes, `.st_mtime` for modification time.

### Reading and writing

For small files, `pathlib` provides one-liner read and write methods:

```python
text = csv.read_text(encoding="utf-8")
csv.write_text("col1,col2\n1,2\n", encoding="utf-8")

blob = model_path.read_bytes()
model_path.write_bytes(pickled)
```

For larger files or line-by-line processing, the `open()` method returns a regular file object that you can use with a `with` block:

```python
with csv.open("r", encoding="utf-8") as f:
    header = f.readline()
    for line in f:
        ...
```

Always specify `encoding="utf-8"` explicitly when you care about portability. The default on Windows is the system locale encoding, which silently munges non-ASCII text; on Linux the default is usually UTF-8 but it can be changed by environment variables. Making the encoding explicit is three characters and saves a real category of bug.

### Creating directories, moving, deleting

```python
run_dir.mkdir(parents=True, exist_ok=True)   # mkdir -p
old.rename(new)                               # move or rename
old.unlink(missing_ok=True)                   # delete file, ok if absent
shutil.rmtree(run_dir)                        # delete directory tree (still needs shutil)
```

`parents=True` creates any missing intermediate directories, and `exist_ok=True` turns "directory already exists" from an error into a no-op. Those two flags together are the `mkdir -p` idiom and are almost always what you want in a data pipeline. `missing_ok=True` on `unlink` is new in Python 3.8 and makes "delete if present" a one-liner.

`pathlib` does not include a recursive delete; for that, `shutil.rmtree(path)` is still the standard function.

### Globbing

`glob` finds paths matching a wildcard pattern:

```python
for csv in Path("data").glob("*.csv"):
    ...

for csv in Path("data").rglob("*.csv"):   # recursive
    ...
```

`rglob` descends into subdirectories; it is the cleanest way to enumerate "every CSV under this tree". More elaborate patterns (`run_*/metrics_epoch_*.json`) work too. Both return a generator, so they are memory-friendly even for very large trees.

### Interop with string-expecting APIs

If you are handing a path to a library that predates `pathlib` and does not accept `Path` objects (rare nowadays, but possible), just cast to `str`: `str(path)`. Most of the scientific stack — NumPy, pandas, PyTorch, Pillow — already accepts `Path` directly, because they use `os.fspath()` internally, which knows how to convert.

---

## Environments and reproducibility

Data science has a reproducibility crisis and a lot of it traces back to one specific problem: the code ran in an environment that no longer exists. Six months later the same notebook imports the same libraries, pulls different versions, and produces a different answer — or fails altogether. Managing environments properly is not glamorous, but it is the difference between an analysis you can return to and one you cannot.

### What a project environment needs to specify

At minimum, a reproducible environment pins:
1. **The Python version** (3.11, 3.12, etc.) — semantics change between minor versions.
2. **The direct dependencies** with exact versions (`pandas==2.2.3`, not `pandas>=2.0`).
3. **The transitive dependencies**, because a direct dep's sub-dep can break you on a background upgrade.
4. **Platform-specific binary wheels**, where relevant, because `torch==2.3.1+cu121` on Linux and `torch==2.3.1` on macOS are different artifacts.

The last two usually require a **lock file** — a machine-written record of every package, its exact version, its hash, and its platform. A lock file is what makes "I ran `pip install -r requirements.txt` and got the same thing you got" a true statement.

### The tool landscape

There are four tools you are likely to see in the wild, and the right choice depends on the project:

- **`venv`** is the standard library's built-in virtual environment creator. It is the lowest common denominator: every Python installation has it, and it works with `pip install`. It does not produce lock files on its own; you generate those separately with `pip freeze`. Good for small scripts and one-off projects.

- **`uv`** is a modern, very fast Python package manager written in Rust. It installs packages an order of magnitude faster than `pip`, manages Python versions, produces lock files, and integrates with `pyproject.toml`. For most new projects in 2026 this is the default recommendation: it is fast, correct, and replaces `venv`, `pip`, `pip-tools`, and `pipx` with one tool.

- **`conda`** (and the faster drop-in `mamba`) is a package manager from the scientific Python community that pre-dates wheels and can install non-Python dependencies (CUDA toolkits, MKL, system libraries) as well as Python packages. It is still the right tool when you need a specific CUDA version bundled, or when a scientific package only distributes through conda-forge, but for pure-Python work `uv` has eclipsed it on both speed and ergonomics.

- **`poetry`** is an earlier-generation modern manager focused on publishing Python packages. It has a richer dependency-resolution model than `pip`, its own lock file, and good ergonomics for library authors. For application-style projects (your analysis repo), `uv` covers the same ground more quickly.

### `pyproject.toml`: the canonical project file

Since PEP 518 the standard way to describe a Python project is `pyproject.toml` at the repository root. A minimal file for an analysis project managed with `uv` looks like:

```toml
[project]
name = "churn-analysis"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = [
    "pandas==2.2.3",
    "numpy==1.26.4",
    "scikit-learn==1.5.0",
    "matplotlib==3.8.4",
]

[dependency-groups]
dev = [
    "mypy==1.10.0",
    "pytest==8.2.2",
    "ruff==0.4.8",
]

[tool.uv]
# uv-specific config
```

The corresponding lock file (`uv.lock`) is generated by `uv lock` and should be committed to the repository. `uv sync` then installs exactly the locked versions into a virtual environment. Cloning the repo and running `uv sync` gets you a bit-for-bit reproducible setup.

The same shape works with `poetry` (using `[tool.poetry]` sections and `poetry.lock`), and for pip-based workflows you can use `pip-tools` (`pip-compile`) to turn a list of direct dependencies into a pinned `requirements.txt` that acts like a lock file. The common theme is: **always generate and commit a lock file**.

### A minimum-viable reproducibility checklist

For any data-science project you expect to revisit:

1. Start with a fresh virtual environment (`uv venv` or `python -m venv .venv`).
2. Record your Python version in `pyproject.toml` or a `.python-version` file.
3. Install dependencies by name and generate a lock file immediately.
4. Commit `pyproject.toml` and the lock file to version control.
5. Never `pip install` something without adding it to `pyproject.toml`.
6. Set a seed at the top of any script or notebook that uses randomness (`np.random.seed`, `random.seed`, `torch.manual_seed`).
7. Log the commit hash of your code and the hash of your input data alongside any result you plan to trust six months from now.

None of this is exotic. It is just the cost of not having to apologize later.

---

## Profiling Python code

Before you optimize, measure. This is perhaps the most over-quoted piece of programming advice, and it is over-quoted because it is correct, because people violate it constantly, and because violating it means spending a week speeding up code that is not the bottleneck while the real bottleneck sits there untouched. Data science code is especially vulnerable to this: when a notebook takes ten minutes to run, the human instinct is to rewrite the loop you are currently looking at, not to find out which cell actually spends the time.

Python gives you several profilers operating at different levels of granularity. Learn them in order from coarse to fine: which line is slow, which function is slow inside that line, which statement is slow inside that function, and finally, where is the memory going.

### `timeit`: the micro-benchmark

`timeit` runs a small snippet of code many times and reports the best time. It is the right tool for comparing two ways of doing the same thing — "is list comprehension faster than `map` here?" — on short operations. The trap it avoids is that a single-run wall-clock measurement is noisy: Python's garbage collector, OS scheduling, and memory caching can swing results by 2–10x. `timeit` disables the GC and runs the snippet thousands of times.

From a script:

```python
import timeit

t = timeit.timeit("[x*x for x in range(1000)]", number=10_000)
print(f"{t:.3f} s for 10k iterations")
```

In a Jupyter notebook the cell-magic is much nicer:

```python
%%timeit
[x*x for x in range(1000)]
```

Jupyter auto-picks the number of loops and runs to get a stable measurement. The output looks like `95.3 µs ± 1.12 µs per loop (mean ± std. dev. of 7 runs, 10,000 loops each)`.

Two caveats. First, micro-benchmarks measure *micro* performance; the thing that is fast in a tight loop may not dominate a full pipeline. Second, setup cost matters: if your snippet includes importing a module, you are measuring the import, not the operation. Use the `setup=` argument (or `%%timeit -s "..."`) to move one-time setup out of the timed section.

### `cProfile`: function-level profiling

For a full script or notebook cell, `cProfile` records every function call, its total time (time inside the function, excluding sub-calls), its cumulative time (total time including sub-calls), and the number of calls. It is in the standard library.

From the command line:

```bash
python -m cProfile -o profile.out script.py
```

That writes a binary file; to read it, either open it in the `pstats` module or use a viewer. The text report is:

```python
import pstats
p = pstats.Stats("profile.out")
p.sort_stats("cumulative").print_stats(20)
```

Reading a `cProfile` report is a skill in itself. The two most useful columns are **cumtime** (cumulative time spent in this function and everything it calls — tells you *where* the time goes high-level) and **tottime** (time spent in this function's own body — tells you which function is actually doing the work). Sorting by `cumulative` and looking at the top 20 lines gives you a bird's-eye view: "we spent 18 of our 25 seconds inside `preprocess`, and 14 of those were inside `normalize_strings`". Then you dig into `normalize_strings` with a finer tool.

In Jupyter the magic `%%prun` profiles a cell inline and prints the top entries in the notebook:

```python
%%prun -l 20
run_full_pipeline()
```

For an interactive, visual view, **snakeviz** (`pip install snakeviz`) consumes `cProfile` output and serves a flame-graph-style view in the browser.

### `line_profiler`: line-level profiling

Once `cProfile` has pointed you at the slow function, `line_profiler` tells you which line inside it is the one you should care about. This is the single most useful tool when optimizing NumPy or pandas code, because two lines that look similar can differ by 100x in runtime.

Install `line_profiler` and decorate the function you want to profile:

```python
@profile   # provided at runtime by kernprof
def preprocess(df):
    df["x"] = np.log(df["a"])
    df["y"] = df["b"].apply(slow_python_function)  # suspect
    df["z"] = df["c"] * 2
    return df
```

Run with `kernprof -l -v script.py`. The `@profile` decorator is injected at runtime; you do not import it. The output annotates each line with its hit count, total time, and percent of function time:

```
Line #  Hits  Time   Per Hit  % Time  Line Contents
    ================================================
     3    1   4.2    4.2      0.1    df["x"] = np.log(df["a"])
     4    1   4812.  4812.    99.7   df["y"] = df["b"].apply(slow_python_function)
     5    1   6.1    6.1      0.1    df["z"] = df["c"] * 2
```

Here the answer is unambiguous: line 4 is 99.7% of the function. Now you know exactly what to fix.

In Jupyter, `%load_ext line_profiler` once, then `%lprun -f preprocess preprocess(df)` runs the profiler on `preprocess` while calling `preprocess(df)`.

### Memory profiling: `tracemalloc` and `memory_profiler`

When the problem is memory rather than time — a cell that fits comfortably in 16 GB until you bump the data 2x and suddenly it OOMs — you need a different tool.

`tracemalloc` is in the standard library and traces Python-level allocations:

```python
import tracemalloc

tracemalloc.start()
do_work()
snapshot = tracemalloc.take_snapshot()
for stat in snapshot.statistics("lineno")[:10]:
    print(stat)
```

It shows the top allocating lines. This is the right tool for "where are all these objects coming from?" questions.

`memory_profiler` (`pip install memory_profiler`) is the memory analog to `line_profiler`: decorate the function with `@profile` and run `python -m memory_profiler script.py`. Each line is annotated with memory usage and the delta it added:

```
Line #  Mem Usage  Increment   Line Contents
    =========================================
     3   120 MiB    0 MiB      df = load_data()
     4   220 MiB   +100 MiB    features = df.apply(expensive)   # leaked
     5   220 MiB    0 MiB      return features.mean()
```

For memory profiling of NumPy-heavy workloads, `tracemalloc` misses the large arrays because they are allocated by NumPy's C code, not through Python's allocator. For that, OS-level tools (`/usr/bin/time -v`, or `resource.getrusage(resource.RUSAGE_SELF).ru_maxrss`) give you peak RSS, which is the most honest number.

### `py-spy`: the sampling profiler

`cProfile` works by instrumenting every function call, which has overhead and changes the behavior of the thing you are measuring. **`py-spy`** is a **sampling profiler**: it runs as a separate process, peeks at the Python interpreter's stack many times a second, and aggregates what it sees. The result is near-zero overhead and the ability to profile a program that is already running.

```bash
# Profile a running script for 30 seconds, emit a flame graph
py-spy record -o profile.svg --pid 12345 --duration 30

# Or start a script under py-spy directly
py-spy record -o profile.svg -- python script.py

# Live top-style view of a running program
py-spy top --pid 12345
```

`py-spy` is the right tool for production systems where you cannot restart the process, for long-running training jobs where instrumented profiling would distort the timing, and for cases where you want a visual flame graph you can share with a teammate. It is probably the profiling tool you will use most once you have more than one collaborator on a codebase.

### Which profiler when

| You want to know                                  | Use                                  |
|---------------------------------------------------|--------------------------------------|
| Is `A` faster than `B` for this small operation?  | `timeit` / `%%timeit`                |
| Which function eats most of the total time?       | `cProfile` / `%%prun`                |
| Which line inside that function is the slow one?  | `line_profiler` / `%lprun`           |
| Where is the memory going?                        | `tracemalloc` / `memory_profiler`    |
| What is this already-running program doing?       | `py-spy top` / `py-spy record`       |
| I need peak resident set size as a number.        | `/usr/bin/time -v`, `getrusage`      |

The canonical workflow is: `cProfile` to find the hot function, `line_profiler` to find the hot line inside it, then fix that one line. Do not skip steps. A rewrite based on a guess about which line is slow is wasted effort as often as not.

---

## Logging instead of `print`

Every data-science codebase starts with `print` statements and every serious data-science codebase ends up replacing them with `logging`. The reason is that `print` has no levels, no timestamps, no way to silence it, and no way to route it anywhere but stdout. The standard library's `logging` module gives you all of that for roughly the same number of keystrokes.

The minimum viable setup at the top of a module:

```python
import logging

logger = logging.getLogger(__name__)
```

Then in the code:

```python
logger.debug("computed weights for %d rows", len(rows))
logger.info("starting epoch %d", epoch)
logger.warning("missing values in column 'age': %d rows", n_missing)
logger.error("failed to load %s", path, exc_info=True)
```

The `%s`/`%d` style (as opposed to f-strings) is intentional: the formatting is deferred and only happens if the log record is actually emitted, which matters when debug logging is turned off in production. `exc_info=True` on an error attaches the current exception traceback to the log record.

The five levels are `DEBUG < INFO < WARNING < ERROR < CRITICAL`. By default the root logger is set to `WARNING`, which is why `logger.info(...)` looks silent until you configure it. The simplest configuration, at the top of your entry point:

```python
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
```

This gives you timestamped, level-prefixed lines. For larger projects there is a full configuration system (dict config, handlers that write to files or syslog, per-logger level overrides), but the three lines above cover 80% of cases.

The important cultural rule is: **libraries do not configure logging; applications do.** A function in a library module should create a logger with `logging.getLogger(__name__)` and emit records to it, but it should not call `basicConfig`, because that would override whatever the calling application configured. Only the top-level script should configure logging — usually once, at startup, before doing anything else.

For structured logging (key-value pairs, JSON output, machine-readable context), libraries like `structlog` and `loguru` build on top of `logging` or replace it with a nicer API. They are worth knowing about but overkill for a single-person analysis project.

---

## What to remember & cheatsheet

### What to remember

- **Reach for built-ins first.** `enumerate`, `zip(..., strict=True)`, comprehensions, generator expressions, unpacking, and the walrus operator cover the vast majority of Python loops you will write. Any index-based `for i in range(len(xs))` loop should raise a small alarm.
- **`collections` and `itertools` are part of the language.** `defaultdict`, `Counter`, `deque` and `chain`, `groupby`, `islice`, `accumulate`, `product` remove the need to reinvent common iteration patterns.
- **Type hints are a contract at boundaries.** They matter most on the public signatures of modules that other code imports, on dataclasses, and on the boundary between untrusted data (config files, APIs) and the rest of the pipeline. Runtime validation for the untrusted side goes through Pydantic.
- **Pick the right record type.** `@dataclass` for most structs, `NamedTuple` for tuple-shaped return values, `TypedDict` for existing dict-shaped data, `pydantic.BaseModel` for validated inputs, and a DataFrame for anything with more than a few hundred rows.
- **Never write a literal mutable default in a dataclass.** `field(default_factory=list)`. The language will yell at you if you try, but only once you have already typed it.
- **Use `pathlib` everywhere.** `Path("a") / "b" / "c.csv"`, `path.read_text()`, `path.glob("*.csv")`, `path.mkdir(parents=True, exist_ok=True)`. `os.path` is a compatibility shim for code written before 2014.
- **Reproducibility requires a lock file.** Pin your Python version, pin your direct dependencies, and commit the lock file. `uv` is the current best default.
- **Profile before optimizing.** `cProfile` for functions, `line_profiler` for lines, `py-spy` for already-running processes, `tracemalloc` for memory. Guessing which line is slow is almost always wrong.
- **Log, do not print.** `logger = logging.getLogger(__name__)` at module level; `basicConfig` only at the top of applications.

### Cheatsheet

```python
# --- Iteration ---
for i, row in enumerate(rows, start=1): ...
for a, b in zip(xs, ys, strict=True): ...
squares = [x*x for x in xs]
total   = sum(x*x for x in xs)              # generator expression
head, *mid, tail = seq                       # starred unpacking
while (line := f.readline()): ...            # walrus

# --- collections ---
from collections import defaultdict, Counter, deque
groups  = defaultdict(list);  groups[k].append(v)
counts  = Counter(stream);    counts.most_common(5)
window  = deque(maxlen=10);   window.append(x)

# --- itertools ---
from itertools import chain, groupby, islice, accumulate, product
chain(a, b, c)
list(islice(stream, 5))
list(accumulate(xs))                         # running sum
for k, g in groupby(sorted(xs, key=key), key=key): list(g)
for lr, bs in product(LRS, BATCH_SIZES): ...

# --- functools ---
from functools import lru_cache, partial
@lru_cache(maxsize=1024)
def f(x): ...

# --- Types ---
def score(x: list[float], mode: Literal["mean","max"] = "mean") -> float: ...
Result = tuple[float, float] | None

# --- dataclass ---
from dataclasses import dataclass, field
@dataclass(frozen=True, slots=True, kw_only=True)
class Cfg:
    learning_rate: float
    batch_size: int = 32
    tags: list[str] = field(default_factory=list)

# --- pathlib ---
from pathlib import Path
root = Path.home() / "data"
(root / "runs").mkdir(parents=True, exist_ok=True)
for p in root.rglob("*.csv"): text = p.read_text(encoding="utf-8")
out = (root / "metrics.csv").with_suffix(".parquet")

# --- Profiling ---
# $ python -m cProfile -o p.out script.py
# $ kernprof -l -v script.py    # line_profiler
# $ py-spy record -o p.svg -- python script.py
# %%timeit / %%prun / %lprun -f fn fn(args)

# --- Logging ---
import logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
logger.info("loaded %d rows", len(df))
```

That is the scaffolding. The next module (`module02.md`) picks up from here and dives into NumPy — the library on top of which the rest of the scientific Python ecosystem is built, and the one whose memory model will repay every minute you spend on it.
