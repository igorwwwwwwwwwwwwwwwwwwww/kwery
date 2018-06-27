$: << 'lib'

require 'sinatra'
require 'kwery'
require 'json'

schema = Kwery::Schema.new

schema.import_csv(:users, 'data/users.csv', { id: :integer, active: :boolean })
schema.create_index(:users, :users_idx_id, [
  Kwery::Expr::IndexedExpr.new(Kwery::Expr::Column.new(:id), :asc),
])

schema.import_json(:clickhouse_users, 'data/clickhouse_users.json')
schema.create_index(:clickhouse_users, :clickhouse_users_idx_id, [
  Kwery::Expr::IndexedExpr.new(Kwery::Expr::Column.new(:id), :asc),
])
schema.create_index(:clickhouse_users, :clickhouse_users_idx_guid, [
  Kwery::Expr::IndexedExpr.new(Kwery::Expr::Column.new(:guid), :asc),
])
schema.create_index(:clickhouse_users, :clickhouse_users_idx_name, [
  Kwery::Expr::IndexedExpr.new(Kwery::Expr::Column.new(:name), :asc),
])

post '/query' do
  sql = request.body.read
  options = {}

  parser = Kwery::Parser.new(options)
  query = parser.parse(sql)

  plan = query.plan(schema)

  context = Kwery::Executor::Context.new(schema)
  tups = plan.call(context).to_a

  JSON.pretty_generate({
    tups: tups,
    stats: context.stats,
  }) + "\n"
end

run Sinatra::Application
