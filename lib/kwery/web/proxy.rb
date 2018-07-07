require 'sinatra'
require 'kwery'
require 'json'

backends = ENV['BACKENDS']
  .split(';')
  .map { |rs| rs.split(',') }

schema = Kwery::Schema.new
schema.define_shard(:users,
  key:      Kwery::Expr::Column.new(:id),
  shards:   16,
  backends: backends,
)

parser = Kwery::Parser.new

set :protection, false
set :show_exceptions, false

get '/' do
  { name: ENV['SERVER_NAME'], proxy: true, backends: backends }.to_json + "\n"
end

post '/insert/:table' do
  table = params[:table].to_sym
  data = JSON.parse(request.body.read, symbolize_names: true)

  data.each do |tup|
    tup[:_shard] = schema.shard_for_tup(table, tup)
  end

  backends = data.group_by do |tup|
    schema.primary_for_shard(table, tup[:_shard])
  end

  # TODO: deprecate RemoteInsert in favour of query rewriting
  plans = backends.map do |backend, tups|
    Kwery::Executor::RemoteInsert.new(
      backend,
      table,
      tups,
    )
  end

  plan = Kwery::Executor::Append.new(plans)

  context = Kwery::Executor::Context.new(schema)
  tups = plan.call(context).to_a

  JSON.pretty_generate({
    data: tups,
  }) + "\n"
end

# TODO: handle writes properly (select primary, disallow updates to shard key)
# TODO: service discovery?
# TODO: separate table per shard? (replicate only specific shard)
# TODO: resharding / shard moving and reassignment
# TODO: combine stats from remote calls
# TODO: support hash aggregate / group by
# TODO: distributed tracing (opencensus?)

post '/query' do
  sql = request.body.read

  query = parser.parse(sql)
  query.options[:remote] = true
  query.options[:sql]    = sql

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
