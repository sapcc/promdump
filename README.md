# Promdump
Dumps a prometheus query to a local file in a generic readable format for further data processing

## Minimal usage
```sh
promdump dump -u $PROM_URL '$PROM_QUERY' > query.json
```
Outputs the flattened query results as json to stdout that can be directly consumed by pandas via:
```python
import pandas as pd

df = pd.read_json("query.json", orient="records")
print(df)
```

## Arguments

### --backend/-b $BACKEND
Specifies the HTTP backend. Can be `curl` or `go`.

### --client-cert $CERT
Specifies the name of the client certificate to use.

### --format/-f $FORMAT
Specifies the serialization format. Can be `json` or `parquet`.

### --layout/-l $LAYOUT
Specifies the data layout.
- `raw` directly serializes the response of prometheus.
- `nested` creates "rows" of metric name, timestamp value and the label set as a nested element.
- `flat` flattens the label set into the upper structure.

### --compress/-c $COMPRESSION
Specifies the compression for the output. Can be `none` or `gzip`.

### --start/-s $START
Specifies the start timestamp for the query. Defaults to `now - 5m`.

### --end/-e $END
Specifies the end timestamp for the query. Defaults to `now`.

### --step/-S $STEP
Specifies the sample rate for the query. Defaults to `1m`.

## Note on MacOS with client-cert authentification
You need to enable that curl HTTP backend:
```sh
CURL_SSL_BACKEND=secure-transport promdump -b curl --client-cert $CERT_NAME dump -u $PROM_URL '$PROM_QUERY'
```
