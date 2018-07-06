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

post '/query' do
  sql = request.body.read
  options = { remote: true, sql: sql }

  parser = Kwery::Parser.new(options)
  query = parser.parse(sql)

  plan = query.plan(schema)

  context = Kwery::Executor::Context.new(schema)
  tups = plan.call(context).to_a

  JSON.pretty_generate({
    data: tups,
    stats: context.stats,
  }) + "\n"

  # TODO: service discovery?
  # TODO: separate table per shard?
  # TODO: support IN query
  # TODO: handle writes properly (select primary, disallow updates to shard key)
  # TODO: resharding / shard moving and reassignment
  # TODO: combine stats from remote calls
end
