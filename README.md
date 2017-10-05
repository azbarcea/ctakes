
## Generic issues

### `build.xml` still exist but shouldn't be used

```
./ctakes-assertion/build.xml
./ctakes-ytex/scripts/data/build.xml
```

## ctakes-assertion

### Failed `main[]`

Execution of `org.apache.ctakes.assertion.cr.NegExCorpusReader:main` does nothing:
* File: `data/assertion/data/gold_standard/negex/Annotations-1-120-random.txt` doesn't exist

### `build.xml` exists, but ant is no longer used

### `build.xml` references outside of project file

```
<arg line="/Users/m081914/work/data/negextestset/rsAnnotations-1-120-random.txt"/>
```
