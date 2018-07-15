require 'raft'
require 'typhoeus'
require 'json'

module Kwery
  module Replication
    module Raft
      def self.start_update_thread(node)
        Thread.new {
          while true
            node.update
            sleep node.config.update_interval
          end
        }
      end

      class Persistence
        def initialize(node, filename)
          @node = node
          @filename = filename
        end

        # TODO: figure out if there is an issue with log compaction/truncate here
        #       do we need to perform some sort of snapshotting/checkpointing?
        #       or can we maybe disable compaction for now?
        def load
          read_json = File.read(@filename)
          read_hash = JSON.parse(read_json, symbolize_names: true)

          persistent_state = @node.persistent_state
          persistent_state.current_term = read_hash[:current_term]
          persistent_state.voted_for    = read_hash[:voted_for]

          entries = read_hash[:log].map do |h|
            ::Raft::LogEntry.new(h[:term], h[:index], h[:command])
          end
          persistent_state.log = ::Raft::Log.new(entries)
        end

        # TODO: optimize for append only log writing
        #       maybe keep two separate files and update the state (non-log)
        #       one only when values change which should be infrequent enough
        #       if we have a stable leader.
        def flush
          sent_hash = HashMarshalling.object_to_hash(@node.persistent_state, %w(current_term voted_for))
          sent_hash['log'] = @node.persistent_state.log.map { |entry|
            HashMarshalling.object_to_hash(entry, %w(term index command))
          }
          sent_json = JSON.dump(sent_hash)
          File.write(@filename, sent_json)
        end
      end

      module HashMarshalling
        def self.hash_to_object(hash, klass)
          object = klass.new
          hash.each_pair do |k, v|
            object.send("#{k}=", v)
          end
          object
        end

        def self.object_to_hash(object, attrs)
          attrs.reduce({}) { |hash, attr|
            hash[attr] = object.send(attr); hash
          }
        end
      end

      class RpcProvider < ::Raft::RpcProvider
        def request_votes(request, cluster, &block)
          sent_hash = HashMarshalling.object_to_hash(request, %w(term candidate_id last_log_index last_log_term))
          sent_json = JSON.dump(sent_hash)

          reqs = cluster.node_ids
            .reject { |node_id| node_id == request.candidate_id }
            .map { |node_id|
              req = Typhoeus::Request.new(
                "#{node_id}/raft/request_votes",
                method: :post,
                body: sent_json,
              )
              req.on_complete do
                unless req.response.success?
                  warn "raft: request_vote failed request #{req.url} status=#{req.response.code} timeout=#{req.response.timed_out?} body=\"#{req.options[:body]}\""
                  next
                end
                received_hash = JSON.load(req.response.body)
                response = HashMarshalling.hash_to_object(received_hash, ::Raft::RequestVoteResponse)

                if ENV['RAFT_DEBUG'] == 'true'
                  STDOUT.write("\n\t#{node_id} responded #{response.vote_granted} to #{request.candidate_id}\n\n")
                end

                yield node_id, request, response
              end
              req
            }
          hydra = Typhoeus::Hydra.new
          reqs.each do |req|
            hydra.queue(req)
          end
          hydra.run
        end

        def append_entries(request, cluster, &block)
          reqs = cluster.node_ids
            .reject { |node_id| node_id == request.leader_id }
            .map { |node_id|
              create_append_entries_to_follower_request(request, node_id, &block)
            }
          hydra = Typhoeus::Hydra.new
          reqs.each do |req|
            hydra.queue(req)
          end
          hydra.run
        end

        def append_entries_to_follower(request, node_id, &block)
          req = create_append_entries_to_follower_request(request, node_id, &block)
          req.run
        end

        def create_append_entries_to_follower_request(request, node_id, &block)
          sent_hash = HashMarshalling.object_to_hash(request, %w(term leader_id prev_log_index prev_log_term entries commit_index))
          sent_hash['entries'] = sent_hash['entries'].map {|obj| HashMarshalling.object_to_hash(obj, %w(term index command))}
          sent_json = JSON.dump(sent_hash)
          raise "replicating to self!" if request.leader_id == node_id

          if ENV['RAFT_DEBUG'] == 'true'
            STDOUT.write("\nleader #{request.leader_id} replicating entries to #{node_id}: #{sent_hash}\n")#"\t#{caller[0..4].join("\n\t")}")
          end

          req = Typhoeus::Request.new(
            "#{node_id}/raft/append_entries",
            method: :post,
            body: sent_json,
          )
          req.on_complete do |response|
            if ENV['RAFT_DEBUG'] == 'true'
              STDOUT.write("\nleader #{request.leader_id} calling back to #{node_id} to append entries\n")
            end

            if response.code == 200
              received_hash = JSON.load(response.body)
              response = HashMarshalling.hash_to_object(received_hash, ::Raft::AppendEntriesResponse)
              yield node_id, response
            else
              warn "raft: append_entries failed for node '#{node_id}' with code #{response.code}"
            end
          end
          req
        end

        def command(request, node_id)
          sent_hash = HashMarshalling.object_to_hash(request, %w(command))
          sent_json = JSON.dump(sent_hash)

          req = Typhoeus::Request.new(
            "#{node_id}/raft/command",
            method: :post,
            body: sent_json,
          )
          req.run

          if req.response.code == 200
            received_hash = JSON.load(req.response.body)
            HashMarshalling.hash_to_object(received_hash, ::Raft::CommandResponse)
          else
            warn "raft: command failed for node '#{node_id}' with code #{req.response.code}"
            ::Raft::CommandResponse.new(false)
          end
        end
      end

      class AsyncProvider < ::Raft::AsyncProvider
        def initialize(await_interval)
          @await_interval = await_interval
        end

        def await
          # TODO: track wait time, perhaps propagate to context somehow
          until yield
            sleep @await_interval
          end
        end
      end
    end
  end
end
