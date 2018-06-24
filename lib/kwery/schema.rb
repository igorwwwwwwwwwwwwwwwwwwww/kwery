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
      raise "no table of name #{@tables[table_name]}" unless @tables[table_name]

      table = @tables[table_name]
      table.lazy
    end

    def fetch(table_name, tid)
      raise "no table of name #{@tables[table_name]}" unless @tables[table_name]

      table = @tables[table_name]

      tup = table[tid]
      tup
    end

    def create_table(table_name)
      @tables[table_name] = [] unless @tables[table_name]
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
      raise "no table of name #{@tables[table_name]}" unless @tables[table_name]

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

    def import_csv(table_name, filename, type_map)
      @importer_csv ||= Kwery::Importer::Csv.new(self)
      @importer_csv.load(table_name, filename, type_map)
    end

    def import_json(table_name, filename)
      @importer_json ||= Kwery::Importer::Json.new(self)
      @importer_json.load(table_name, filename)
    end
  end
end
