require 'set'
require 'zlib'

module Kwery
  module Shard
    def self.hashmod(val, n)
      Zlib::crc32(val.to_s) % n
    end

    class ShardLockedError < StandardError
    end

    class StateMap
      # each line is a replica set / raft group
      #
      # backends: [
      #   [http://localhost:8000, http://localhost:8001, http://localhost:8002],
      #   [http://localhost:8100, http://localhost:8101, http://localhost:8102],
      #   [http://localhost:8200, http://localhost:8201, http://localhost:8202],
      # ],
      #
      # TODO: introduce rs key prefix to make it more explicit,
      #       e.g. backends: { rs0: ..., rs1: ..., rs2: ... }
      def initialize(backends)
        @states = {}
        @leaders = {}
        @backends = backends
      end

      # each line corresponds to a replica set,
      # that is an index in the backends array
      #
      # assignments: [
      #   [ 0,  1,  2,  3,  4,  5],
      #   [ 6,  7,  8,  9, 10],
      #   [11, 12, 13, 14, 15],
      # ],
      #
      # TODO: validate that the assignments are actually valid
      def define_shard(table, key:, count:, assignments:)
        @states[table] = Kwery::Shard::State.new(
          key: key,
          count: count,
          assignments: assignments,
        )
      end

      def shard_for_value(table, val)
        Kwery::Shard.hashmod(val, @states[table].count)
      end

      def shard_for_tup(table, tup)
        val = @states[table].key.call(tup)
        shard_for_value(table, val)
      end

      def primary_for_rs(rs)
        @backends[rs].first
      end

      def rs_for_shard(table, shard)
        rs = @states[table].assignments_inverse[shard]
        @backends[rs]
      end

      # TODO: support some kind of interrupt so that a "config_reload_hint" response
      #       to a query can perform a faster poll
      # TODO: make intervals configurable
      def start_config_update_thread
        Thread.new {
          sleep 5
          loop do
            @backends.each do |rs|
              client = Kwery::Client::Batch.new
              results = client.raft_leader(rs)
              leader = results
                .sort_by { |res| res[:current_term] }
                .map { |res| res[:leader_id] }
                .last
              @leaders[rs] = leader
            end

            # some backends are unknown, poll more quickly
            if @leaders.values.any? { |v| v.nil? }
              sleep 5
            else
              sleep 60
            end
          end
        }
      end

      # TODO: select primary from raft leader
      #       perhaps even error out if we do not know
      #       a background process should infrequently poll each set for updates,
      #       in response to a query should trigger a poll
      def primary_for_shard(table, shard)
        rs = rs_for_shard(table, shard)
        @leaders[rs] or raise "do not know leader for raft group #{rs}"
      end

      # TODO: select primary from raft leader
      def primaries_for_shards(table, shards)
        shards
          .group_by { |shard| rs_for_shard(table, shard) }
          .map { |rs, shards| @leaders[rs] or raise "do not know leader for raft group #{rs}" }
      end

      def primaries_all(table)
        @backends.map(&:first)
      end

      # pick random backend from replica set
      def backend_for_shard(table, shard)
        rs_for_shard(table, shard).sample
      end

      def backends_for_shards(table, shards)
        shards
          .group_by { |shard| rs_for_shard(table, shard) }
          .map { |rs, shards| rs.sample }
      end

      def backends_all(table)
        @backends.map(&:sample)
      end

      def shards_locked?(table, shards)
        return true if shards.empty? && any_locked?(table)
        return true if shards.any? { |shard| locked?(table, shard) }
        false
      end

      def any_locked?(table)
        @states[table].any_locked?
      end

      def locked?(table, shard)
        @states[table].locked?(shard)
      end

      def lock(table, shard)
        @states[table].lock(shard)
      end

      def unlock(table, shard)
        @states[table].unlock(shard)
      end

      def key(table)
        @states[table].key
      end

      def count(table)
        @states[table].count
      end

      def reassign(table, shard, rs)
        @states[table].reassign(shard, rs)
      end
    end

    class State
      attr_accessor :count, :key, :assignments, :assignments_inverse

      # assignments_inverse is of the form
      # {shard => rs, ...}
      def initialize(key:, count:, assignments:)
        @locked = Set.new
        @key = key
        @count = count
        @assignments = assignments
        @assignments_inverse = assignments
          .each_with_index
          .flat_map { |shards, i|
            shards.map { |shard| [shard, i] }
          }
          .to_h
      end

      def any_locked?
        !@locked.empty?
      end

      def locked?(shard)
        @locked.include?(shard)
      end

      def lock(shard)
        @locked << shard
      end

      def unlock(shard)
        @locked.delete(shard)
      end

      def reassign(shard, rs)
        @assignments[rs].delete(shard)
        @assignments_inverse[shard] = rs
      end
    end
  end
end
