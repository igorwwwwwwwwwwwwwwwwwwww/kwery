require 'sinatra'
require 'kwery'
require 'json'

# wait for primary to boot (useful when restarting via entr)
sleep ENV['BOOT_SLEEP']&.to_i if ENV['BOOT_SLEEP']

schema = Kwery::Schema.new

schema.create_table(:users)
schema.create_index(:users, :users_idx_id, [
  Kwery::Expr::IndexedExpr.new(Kwery::Expr::Column.new(:id), :asc),
])

Thread.new {
  recovery = Kwery::Replication::Client.new(primary: ENV['PRIMARY'])
  schema.recover(recovery)
}

parser = Kwery::Parser.new

set :protection, false
set :show_exceptions, false

get '/' do
  { name: ENV['SERVER_NAME'], replica: true, primary: ENV['PRIMARY'] }.to_json + "\n"
end

post '/insert/:table' do
  status 400
  return JSON.pretty_generate({
    error: 'no writes allowed against replica',
  }) + "\n"
end

post '/query' do
  sql = request.body.read

  query = parser.parse(sql)
  query.options[:partial] = true if env['HTTP_PARTIAL'] == 'true'

  unless Kwery::Query::Select === query
    status 400
    return JSON.pretty_generate({
      error: 'no writes allowed against replica',
    }) + "\n"
  end

  plan = query.plan(schema)

  context = Kwery::Executor::Context.new(schema)
  tups = plan.call(context).to_a

  JSON.pretty_generate({
    data: tups,
    stats: context.stats,
  }) + "\n"
end

error do |e|
  status 500
  JSON.pretty_generate({
    error: e,
    stack_first: e.backtrace.first,
    # stack: e.backtrace,
  }) + "\n"
end

error Kwery::Planner::UnsupportedQueryError do |e|
  status 400
  JSON.pretty_generate({
    error: e,
  }) + "\n"
end
