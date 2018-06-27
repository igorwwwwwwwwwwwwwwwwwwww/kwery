$: << 'lib'

require 'sinatra'
require 'kwery'
require 'json'

log = Kwery::Log.new('data/wal')
log.start_flush_thread

schema = Kwery::Schema.new(log: log)

schema.create_table(:users)
schema.create_index(:users, :users_idx_id, [
  Kwery::Expr::IndexedExpr.new(Kwery::Expr::Column.new(:id), :asc),
])

schema.recover

post '/insert/:table' do
  table = params[:table].to_sym
  data = JSON.parse(request.body.read, symbolize_names: true)

  count = schema.bulk_insert(table, data)

  JSON.pretty_generate({ count: count }) + "\n"
end

post '/query' do
  sql = request.body.read
  options = {}

  parser = Kwery::Parser.new(options)
  query = parser.parse(sql)

  plan = query.plan(schema)

  context = Kwery::Executor::Context.new(schema)
  tups = plan.call(context).to_a

  JSON.pretty_generate({
    data: tups,
    stats: context.stats,
  }) + "\n"
end

run Sinatra::Application
