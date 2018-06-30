$: << 'lib'

require 'sinatra'
require 'kwery'
require 'json'
require 'socket'

journal_file = ENV['JOURNAL_FILE'] || 'data/journal'

# wait for primary to boot (useful when restarting via entr)
sleep ENV['BOOT_SLEEP']&.to_i if ENV['BOOT_SLEEP']

journal = nil
unless ENV['REPLICA'] == 'true'
  journal = Kwery::Journal::Writer.new(journal_file: journal_file)
  journal.start_flush_thread
end

schema = Kwery::Schema.new(journal: journal)

schema.create_table(:users)
schema.create_index(:users, :users_idx_id, [
  Kwery::Expr::IndexedExpr.new(Kwery::Expr::Column.new(:id), :asc),
])

if ENV['REPLICA'] == 'true'
  Thread.new {
    recovery = Kwery::Replication::Client.new(primary: ENV['PRIMARY'])
    schema.recover(recovery)
  }
else
  recovery = Kwery::Journal::Recovery.new(journal_file: journal_file)
  schema.recover(recovery)
end

unless ENV['REPLICA'] == 'true'
  server = Kwery::Replication::Server.new(journal_file: journal_file)
  server.listen
end

post '/insert/:table' do
  if ENV['REPLICA'] == 'true'
    status 400
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
      status 400
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
