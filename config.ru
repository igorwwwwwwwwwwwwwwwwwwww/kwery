$: << 'lib'

require 'sinatra'
require 'kwery'
require 'json'
require 'socket'

log_file = ENV['JOURNAL_FILE'] || 'data/journal'

# wait for primary to boot (useful when restarting via entr)
sleep ENV['BOOT_SLEEP']&.to_i if ENV['BOOT_SLEEP']

if ENV['REPLICA'] == 'true'
  recovery = Kwery::Log::Recovery::Replication.new(primary: ENV['PRIMARY'])

  log = Kwery::Log.new(noop: true)
else
  recovery = Kwery::Log::Recovery::File.new(log_file: log_file)

  log = Kwery::Log.new(log_file: log_file)
  log.start_flush_thread
end

schema = Kwery::Schema.new(recovery: recovery, log: log)

schema.create_table(:users)
schema.create_index(:users, :users_idx_id, [
  Kwery::Expr::IndexedExpr.new(Kwery::Expr::Column.new(:id), :asc),
])

if ENV['REPLICA'] == 'true'
  Thread.new {
    schema.recover
  }
else
  schema.recover
end

unless ENV['REPLICA'] == 'true'
  server = Kwery::Replication::Server.new(log_file: log_file)
  server.listen
end

post '/insert/:table' do
  if ENV['REPLICA'] == 'true'
    status 500
    return JSON.pretty_generate({
      error: 'no writes allowed against replica',
    }) + "\n"
  end

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

  if ENV['REPLICA'] == 'true'
    unless Kwery::Query::Select === query
      status 500
      return JSON.pretty_generate({
        error: 'no writes allowed against replica',
      }) + "\n"
    end
  end

  plan = query.plan(schema)

  context = Kwery::Executor::Context.new(schema)
  tups = plan.call(context).to_a

  JSON.pretty_generate({
    data: tups,
    stats: context.stats,
  }) + "\n"
end

run Sinatra::Application
