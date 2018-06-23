module Kwery
  class Schema
    def initialize
      @tables = {}
      @indexes = {}
    end

    def index_scan(index_name, sargs = {}, scan_order = :asc, context = nil)
      index = @indexes[index_name] or raise "no index of name #{index_name}"

      index.scan(sargs, scan_order, context).lazy
    end

    def table_scan(table_name)
      table = @tables[table_name] or raise "no table of name #{table_name}"

      table.lazy
    end

    def fetch(table_name, tid)
      table = @tables[table_name] or raise "no table of name #{table_name}"

      tup = table[tid]
      tup
    end

    def create_table(table_name)
      @tables[table_name] = []
    end

    def insert(table_name, tup)
      bulk_insert(table_name, [tup])
    end

    def bulk_insert(table_name, tups)
      tups.each do |tup|
        table = @tables[table_name]
        table << tup
        tid = table.size - 1
      end
    end

    def create_index(table_name, index_name, indexed_exprs)
      index = Kwery::Index.new(
        table_name: table_name,
        indexed_exprs: indexed_exprs,
      )

      table = @tables[table_name]
      table.each_with_index do |tup, tid|
        index.insert_tup(tid, tup)
      end

      @indexes[index_name] = index
    end

    def indexes_for(table_name)
      @indexes.select { |k, idx| idx.table_name == table_name }
    end
  end
end
