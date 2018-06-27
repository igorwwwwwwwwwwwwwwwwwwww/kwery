# Kwery

## Usage

### Select

```
$ bin/kwery 'select name from users where id = 1'
{:name=>"Kathleen"}
```

### Explain

```
$ bin/kwery 'explain select name from users where id = 1'
{:explain=>
  [Kwery::Executor::Project,
   [Kwery::Executor::IndexScan, :users_idx_id, {:eq=>[1]}]]}
```

### Curl

```
$ curl -s 'http://localhost:9292/query' -d 'select id, name, active from users limit 2' | jq '.tups[]'
{
  "id": 1,
  "name": "Kathleen",
  "active": false
}
{
  "id": 2,
  "name": "Xantha",
  "active": true
}
```

## Options

* `--format=default|json|pretty` output format
* `--stats` display runtime stats
* `--notablescan` disallow table scans (index scans only)

### Debug

* `DEBUG_QUERY=true` display the query ast
* `DEBUG_PARSER=true` display a trace of the parser
