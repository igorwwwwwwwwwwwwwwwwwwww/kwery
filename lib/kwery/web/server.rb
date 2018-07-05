require 'sinatra'
require 'kwery'
require 'json'
require 'socket'

journal_file = ENV['JOURNAL_FILE'] || 'data/journal'

journal = Kwery::Journal::Writer.new(journal_file: journal_file)
journal.start_flush_thread

schema = Kwery::Schema.new(journal: journal)

schema.create_table(:users)
schema.create_index(:users, :users_idx_id, [
  Kwery::Expr::IndexedExpr.new(Kwery::Expr::Column.new(:id), :asc),
])

recovery = Kwery::Journal::Recovery.new(journal_file: journal_file)
schema.recover(recovery)

server = Kwery::Replication::Server.new(
  journal_file: journal_file,
  port: ENV['REPLICATION_PORT'],
)
server.listen

get '/' do
  { name: ENV['SERVER_NAME'] }.to_json + "\n"
end

# TODO: deprecate in favour of query?
post '/insert/:table' do
  table = params[:table].to_sym
  data = JSON.parse(request.body.read, symbolize_names: true)

  plan = Kwery::Executor::Insert.new(table, data)

  context = Kwery::Executor::Context.new(schema)
  tups = plan.call(context).to_a

  JSON.pretty_generate({
    data: tups,
  }) + "\n"
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