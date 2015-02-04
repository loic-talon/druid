---
layout: doc_page
---
# Transforming Dimension Values
The following JSON fields can be used in a query to operate on dimension values. 

## DimensionSpec

`DimensionSpec`s define how dimension values get transformed prior to aggregation.

### DefaultDimensionSpec

Returns dimension values as is and optionally renames the dimension.

```json
{ "type" : "default", "dimension" : <dimension>, "outputName": <output_name> }
```

### ExtractionDimensionSpec

Returns dimension values transformed using the given [DimExtractionFn](#toc_3)

```json
{
  "type" : "extraction",
  "dimension" : <dimension>,
  "outputName" :  <output_name>,
  "dimExtractionFn" : <dim_extraction_fn>
}
```

## <a id="toc_3"></a>DimExtractionFn

`DimExtractionFn`s define the transformation applied to each dimension value

### RegexDimExtractionFn

Returns the first group matched by the given regular expression. If there is no match it returns the dimension value as is.

```json
{ "type" : "regex", "expr", <regular_expression> }
```

### PartialDimExtractionFn

Returns the dimension value as is if there is a match, otherwise returns null.

```json
{ "type" : "partial", "expr", <regular_expression> }
```

### SearchQuerySpecDimExtractionFn

Returns the dimension value as is if the given [SearchQuerySpec](SearchQuerySpec.html) matches, otherwise returns null.

```json
{ "type" : "searchQuery", "query" : <search_query_spec> }
```

### TimeDimExtractionFn

Parses dimension values as timestamps using the given input format, and returns them formatted using the given output format. Time formats follow the [com.ibm.icu.text.SimpleDateFormat](http://icu-project.org/apiref/icu4j/com/ibm/icu/text/SimpleDateFormat.html) format

```json
{ "type" : "time", "timeFormat" : <input_format>, "resultFormat" : <output_format> }
```

### JavascriptDimExtractionFn

Returns the dimension value as transformed by the given JavaScript function.

Example

```json
{
  "type" : "javascript",
  "function" : "function(str) { return str.substr(0, 3); }"
}
```

### Namespaced extraction function
A namespaced extraction function is a simple key-value mapping where the key-value mappings are unique to a particular namespace.
This can be used for re-naming values in a datasource for certain queries. To use a particular namespace, simply set the appropriate namespace for the dimExtractionFn
```json
    "dimExtractionFn" : {
      "type":"namespace",
      "namespace":"some_namespace"
    }
```
Namespace updates can be set through adding a child in json form to the zookeeper path at`druid.zk.paths.namespacePath`. The json is of the following format:

#### URI namespace update
```json
{
  "namespace":{
    "type":"uri",
    "namespace":"some_namespace",
    "uri": "file:///some/file/or/other/uri",
    "isSmile":false
  },
  "updateMs":0
}
```

#### JDBC namespace update
The JDBC namespaces will pull from a database cache. If the `tsColumn` is set it must be able to accept comparisons in the format `'2015-01-01 00:00:00'`. For example, the following must be valid sql for the table `SELECT * FROM some_namespace_table WHERE timestamp_column >  '2015-01-01 00:00:00'`. If `tsColumn` is set, the caching service will attempt to only pull values that were written *after* the last sync. If `tsColumn` is not set, the entire table is pulled every time.

```json
{
  "namespace":{
    "namespace":"some_namespace",
    "connectorConfig":{
      "createTables":true,
      "connectURI":"jdbc:mysql://localhost:3306/druid",
      "user":"druid",
      "password":"diurd"
    },
    "table":"some_namespace_table",
    "keyColumn":"the_old_dim_value",
    "valueColumn":"the_new_dim_value",
    "tsColumn":"timestamp_column"
  },
  "updateMs":600000
}
```

#### Kafka namespace update
It is possible to plug into a kafka topic whose key is the old value and message is the desired new value (both in UTF-8). This requires the following extension: "io.druid.extensions:druid-dim-rename-kafka8"
```json
{
  "namespace":{
    "type":"kafka",
    "namespace":"testTopic",
    "kafkaTopic":"testTopic"
  }
}
```
