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
  query.options[:partial] = true if env['HTTP_PARTIAL'] == 'true'

  unless Kwery::Query::Select === query || query.options[:explain]
    status 400
    return JSON.pretty_generate({
      error: 'no writes allowed against replica',
    }) + "\n"
  end

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
