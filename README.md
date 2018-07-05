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
$ curl -sS 'http://localhost:9292/insert/users' -d '[{"id":1,"name":"Kathleen","active":false},{"id":2,"name":"Xantha","active":true},{"id":3,"name":"Hope","active":true}]' | jq '.data[]'
{
  "count": 3
}

$ curl -sS 'http://localhost:9292/query' -d 'select id, name from users where active = true limit 1' | jq '.data[]'
{
  "id": 2,
  "name": "Xantha"
}
```

### Distributed

```
rm data/shard_*
supervisord

curl -sS 'http://localhost:7000/insert/users' -d '[{"id":1,"name":"Kathleen","active":false},{"id":2,"name":"Xantha","active":true},{"id":3,"name":"Hope","active":true},{"id":4,"name":"Hedley","active":false}]' | jq '.data[]'

curl -sS 'http://localhost:7000/query' -d 'explain select id, name from users where id = 1' | jq '.data[]'
curl -sS 'http://localhost:7000/query' -d 'explain select id, name from users where id = 4' | jq '.data[]'
curl -sS 'http://localhost:7000/query' -d 'explain select id, name from users' | jq '.data[]'

curl -sS 'http://localhost:7000/query' -d 'select id, name from users where id = 1' | jq '.data[]'
```

## Options

* `--format=default|json|pretty` output format
* `--stats` display runtime stats
* `--notablescan` disallow table scans (index scans only)

### Debug

* `DEBUG_QUERY=true` display the query ast
* `DEBUG_PARSER=true` display a trace of the parser
