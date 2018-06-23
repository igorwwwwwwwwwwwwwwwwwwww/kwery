module Kwery
  class Schema
    def initialize(catalog)
      @catalog = catalog
      @state = {}

      catalog.tables.each do |table_name, t|
        @state[table_name] = []
      end
      catalog.indexes.each do |index_name, i|
        @state[index_name] = Kwery::Index.new
      end
    end

    def index_scan(index_name, sargs = {}, scan_order = :asc, context = nil)
      index = @state[index_name] or raise "no index of name #{index_name}"

      index.scan(sargs, scan_order, context).lazy
    end

    def table_scan(table_name)
      table = @state[table_name] or raise "no table of name #{table_name}"

      table.lazy
    end

    def fetch(table_name, tid)
      table = @state[table_name] or raise "no table of name #{table_name}"

      tup = table[tid]
      tup
    end

    def insert(table_name, tup)
      bulk_insert(table_name, [tup])
    end

    def bulk_insert(table_name, tups)
      tups.each do |tup|
        relation = @state[table_name]
        relation << tup
        tid = relation.size - 1
      end
    end

    def reindex(table_name, index_name)
      table = @state[table_name]
      index = @state[index_name]

      table.each_with_index do |tup, tid|
        key = @catalog.indexes[index_name]
          .indexed_exprs
          .map(&:expr)
          .map { |expr| expr.call(tup) }
        index.insert(key, tid)
      end
    end
  end
end
