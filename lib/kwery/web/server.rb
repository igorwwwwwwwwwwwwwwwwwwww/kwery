require 'sinatra'
require 'kwery'
require 'json'

# TODO: break circular dependency between schema and raft journal
# TODO: move all of this raft code elsewhere
# TODO: figure out some form of on disk logging / journal
# TODO: get rid of all of the old replication code?
# TODO: does raft work properly with a single node? if so, we can make it required
# TODO: make goliath dep optional in raft gem
# TODO: consider fully evented server to play more nicely with all of the raft stuff

schema = nil
if ENV['RAFT_NODE'] && ENV['RAFT_NODES']
  require 'raft'

  raft_nodes   = ENV['RAFT_NODES'].split(',') - [ENV['RAFT_NODE']]
  raft_cluster = Raft::Cluster.new(*raft_nodes)
  raft_config  = Raft::Config.new(
    Kwery::Replication::Raft::RpcProvider.new,
    Kwery::Replication::Raft::AsyncProvider.new(
      ENV['RAFT_AWAIT_INTERVAL']&.to_f || 0.1
    ),
    ENV['RAFT_ELECTION_TIMEOUT']&.to_i || 15,
    ENV['RAFT_ELECTION_SPLAY']&.to_i || 15,
    ENV['RAFT_UPDATE_INTERVAL']&.to_i || 1,
    ENV['RAFT_HEARTBEAT_INTERVAL']&.to_i || 1,
  )

  raft_node = Raft::Node.new(ENV['RAFT_NODE'], raft_config, raft_cluster) do |command|
    schema.apply_tx(command)
  end

  Thread.new do
    while true
      raft_node.update
      sleep(raft_node.config.update_interval)
    end
  end
end

journal = Kwery::Journal::RaftWriter.new(raft_node)
schema = Kwery::Schema.new(journal: journal)

schema.create_table(:users)
schema.create_index(:users, :users_idx_id, [
  Kwery::Expr::IndexedExpr.new(Kwery::Expr::Column.new(:id), :asc),
])

parser = Kwery::Parser.new

set :protection, false
set :show_exceptions, false

get '/' do
  { name: ENV['SERVER_NAME'] }.to_json + "\n"
end

# TODO: move these raft endpoints out to a rack middleware or such

post '/raft/request_votes' do
  params = JSON.parse(request.body.read)

  if ENV['RAFT_DEBUG'] == 'true'
    STDOUT.write("\nnode #{raft_node.id} received request_vote from #{params['candidate_id']}, term #{params['term']}\n")
  end

  request = Raft::RequestVoteRequest.new(
      params['term'],
      params['candidate_id'],
      params['last_log_index'],
      params['last_log_term'])
  response = raft_node.handle_request_vote(request)
  [200, {}, { 'term' => response.term, 'vote_granted' => response.vote_granted }.to_json]
end

post '/raft/append_entries' do
  params = JSON.parse(request.body.read)

  if ENV['RAFT_DEBUG'] == 'true'
    STDOUT.write("\nnode #{raft_node.id} received append_entries from #{params['leader_id']}, term #{params['term']}\n")
  end

  entries = params['entries'].map {|entry| Raft::LogEntry.new(entry['term'], entry['index'], entry['command'])}
  request = Raft::AppendEntriesRequest.new(
      params['term'],
      params['leader_id'],
      params['prev_log_index'],
      params['prev_log_term'],
      entries,
      params['commit_index'])

  if ENV['RAFT_DEBUG'] == 'true'
    STDOUT.write("\nnode #{raft_node.id} received entries: #{request.entries}\n")
  end

  response = raft_node.handle_append_entries(request)

  if ENV['RAFT_DEBUG'] == 'true'
    STDOUT.write("\nnode #{raft_node.id} completed append_entries from #{params['leader_id']}, term #{params['term']} (#{response})\n")
  end

  [200, {}, { 'term' => response.term, 'success' => response.success }.to_json]
end

post '/raft/command' do
  params = JSON.parse(request.body.read)
  request = Raft::CommandRequest.new(params['command'])
  response = raft_node.handle_command(request)
  [response.success ? 200 : 409, {}, { 'success' => response.success }.to_json]
end

# TODO: mutex around parser and schema
# TODO: streaming output as newline-delimited json?
# TODO: sql parse errors persist in memory for some reason... why?

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
