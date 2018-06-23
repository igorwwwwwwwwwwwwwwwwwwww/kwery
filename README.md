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

* `DEBUG_STATS=true` display runtime stats
* `DEBUG_PARSER=true` display a trace of the parser
* `NOTABLESCAN=true` do not execute the query if no index can be used
