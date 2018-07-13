require 'set'

module Kwery
  module Shard
    def self.hashmod(val, n)
      Zlib::crc32(val.to_s) % n
    end

    class ShardLockedError < StandardError
    end

    class StateMap
      # each line is a "replica set"
      # the first backend is the primary,
      # the others are replicas.
      #
      # backends: [
      #   [http://localhost:8000, http://localhost:9000],
      #   [http://localhost:8001, http://localhost:9001],
      #   [http://localhost:8002, http://localhost:9002],
      # ],
      #
      # TODO: introduce rs key prefix to make it more explicit,
      #       e.g. backends: { rs0: ..., rs1: ..., rs2: ... }
      def initialize(backends)
        @states = {}
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

      def primary_for_shard(table, shard)
        rs = @states[table].assignments_inverse[shard]
        @backends[rs].first
      end

      def primaries_for_shards(table, shards)
        shards
          .group_by { |shard| rs_for_shard(table, shard) }
          .map { |rs, shards| rs.first }
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
