# Kwery

Distributed. In-memory. [NewSQL](http://howfuckedismydatabase.com/nosql/).

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
$ curl -sS 'http://localhost:9292/query' -d 'insert into users (id, name, active) values (1, 'Kathleen', false), (2, 'Xantha', true), (3, 'Hope', true)' | jq '.data[]'
{
  "count": 3
}

$ curl -sS 'http://localhost:9292/query' -d 'select id, name from users where active = true limit 1' | jq '.data[]'
{
  "id": 2,
  "name": "Xantha"
}

$ curl -sS 'http://localhost:9292/query' -F query="copy users from stdin" -F data=@data/users.csv
```

### Distributed

```
# auto-restart during dev
# ag -l --ignore 'supervisord.pid' --ignore data | entr -r supervisord

rm data/shard_*
supervisord

curl -sS 'http://localhost:7000/query' -d 'insert into users (id, name, active) values (1, 'Kathleen', false), (2, 'Xantha', true), (3, 'Hope', true), (4, 'Hedley', false), (8, 'Quincy', true)' | jq '.data[]'

curl -sS 'http://localhost:7000/query' -d 'explain select id, name from users where id = 1' | jq '.data[]'
curl -sS 'http://localhost:7000/query' -d 'explain select id, name from users where id = 4' | jq '.data[]'
curl -sS 'http://localhost:7000/query' -d 'explain select id, name from users' | jq '.data[]'

curl -sS 'http://localhost:7000/query' -d 'select id, name from users where id = 1' | jq '.data[]'
```

### Snippets

convert csv to json:

```
ruby -rcsv -rjson -e 'puts CSV.table("data/users.csv").map(&:to_h).map(&:to_json)'
```

## Options

* `--format=default|json|pretty` output format
* `--stats` display runtime stats
* `--notablescan` disallow table scans (index scans only)

### Debug

* `DEBUG_QUERY=true` display the query ast
* `DEBUG_PARSER=true` display a trace of the parser
