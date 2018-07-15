require 'sinatra'
require 'kwery'
require 'json'

backends    = ENV['BACKENDS'].split(';').map { |rs| rs.split(',') }
assignments = ENV['ASSIGNMENTS'].split(';').map { |rs| rs.split(',').map(&:to_i) }

shards = Kwery::Shard::StateMap.new(backends)
shards.define_shard(:users,
  key:         Kwery::Expr::Column.new(:id),
  count:       16,
  assignments: assignments,
)

schema = Kwery::Schema.new(shards: shards)
parser = Kwery::Parser.new

set :protection, false
set :show_exceptions, false

get '/' do
  { name: ENV['SERVER_NAME'], proxy: true, backends: backends }.to_json + "\n"
end

# TODO: support hash aggregate / group by
# TODO: distributed tracing (opencensus?)
# TODO: service discovery?
# TODO: separate table per shard? (replicate only specific shard)
# TODO: combine stats from remote calls
# TODO: remote copy (split input into shards, just like remote insert)
# TODO: implement mysql (tmtm/ruby-mysql) or postgres (kivikakk/vhskit) protocol
# TODO: decide if proxy should be stateful, and if yes, it may need its own journal

post '/query' do
  if env['CONTENT_TYPE'].start_with?('multipart/form-data;')
    sql   = params[:query]
    stdin = params[:data][:tempfile]
  elsif params[:query] && params[:data]
    sql   = params[:query]
    stdin = StringIO.new(params[:data])
  else
    sql   = request.body.read
    stdin = nil
  end

  query = parser.parse(sql)
  query.options[:remote] = true
  query.options[:sql]    = sql

  plan = query.plan(schema)

  context = Kwery::Executor::Context.new(schema, stdin, shards)
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
