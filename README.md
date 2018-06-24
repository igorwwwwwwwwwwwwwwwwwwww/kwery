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

## Options

* `--format=default|json|pretty` output format
* `--stats` display runtime stats
* `--notablescan` disallow table scans (index scans only)

### Debug

* `DEBUG_PARSER=true` display a trace of the parser
