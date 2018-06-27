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
$ curl -s 'http://localhost:9292/insert/users' -d '[{"id":1,"name":"Kathleen","active":false},{"id":2,"name":"Xantha","active":true},{"id":3,"name":"Hope","active":true}]'
{
  "count": 3
}

$ curl -s 'http://localhost:9292/query' -d 'select id, name from users where active = true limit 1' | jq '.data[]'
{
  "id": 2,
  "name": "Xantha"
}
```

## Options

* `--format=default|json|pretty` output format
* `--stats` display runtime stats
* `--notablescan` disallow table scans (index scans only)

### Debug

* `DEBUG_QUERY=true` display the query ast
* `DEBUG_PARSER=true` display a trace of the parser
