require 'raft'
require 'typhoeus'
require 'json'

module Kwery
  module Replication
    module Raft
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
        def initialize(update_interval)
          @await_interval = await_interval
        end

        def await
          until yield
            sleep @await_interval
          end
        end
      end
    end
  end
end
