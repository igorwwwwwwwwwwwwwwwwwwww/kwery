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

# resharding
# * source: disable writes
# * target: disable reads (consensus)
# * source: copy to target
# * source: disable reads (consensus, atomic)
# * target: enable  reads (consensus, atomic)
# * target: enable  writes
# * source: delete data

# TODO: resharding / shard moving and reassignment
# TODO: reject writes destined for other shard
# TODO: support hash aggregate / group by
# TODO: distributed tracing (opencensus?)
# TODO: service discovery?
# TODO: separate table per shard? (replicate only specific shard)
# TODO: combine stats from remote calls

post '/query' do
  if env['CONTENT_TYPE'].start_with?('multipart/form-data;')
    sql   = params[:query]
    stdin = params[:data][:tempfile]
  else
    sql   = request.body.read
    stdin = nil
  end

  query = parser.parse(sql)
  query.options[:remote] = true
  query.options[:sql]    = sql

  plan = query.plan(schema)

  context = Kwery::Executor::Context.new(schema, stdin)
  tups = plan.call(context).to_a

  JSON.pretty_generate({
    data: tups,
    stats: context.stats,
  }) + "\n"
end

error do |e|
  status 500
  JSON.pretty_generate({
    error:       e,
    error_class: e.class,
    stack_first: e.backtrace.first,
    # stack: e.backtrace,
  }) + "\n"
end

error Kwery::Planner::UnsupportedQueryError do |e|
  status 400
  JSON.pretty_generate({
    error:       e,
    error_class: e.class,
  }) + "\n"
end
