# TODO: this is a "god" object, things should be moved out
#       perhaps separate "storage" and "tx" modules

module Kwery
  class Schema
    def initialize(journal: nil, recovery: nil)
      @tables = {}
      @indexes = {}
      @journal = journal || Kwery::Journal::NoopWriter.new
    end

    def recover(recovery)
      recovery.recover.each do |tx|
        apply_tx(tx)
      end
    end

    def apply_tx(tx)
      op, payload = tx

      case op.to_sym
      when :insert
        table_name, tid, tup = payload

        table = @tables[table_name.to_sym]
        table[tid] = tup

        indexes = indexes_for(table_name.to_sym)
        indexes.each do |k, idx|
          idx.insert_tup(tid, tup)
        end
      when :update
        # TODO implement as delete + insert?

        table_name, tid, tup1, tup2 = payload

        indexes = indexes_for(table_name.to_sym)
        indexes.each do |k, idx|
          idx.delete_tup(tid, tup1)
          idx.insert_tup(tid, tup2)
        end

        table = @tables[table_name.to_sym]
        table[tid] = tup2
      when :delete
        table_name, tid, tup = payload

        indexes = indexes_for(table_name.to_sym)
        indexes.each do |k, idx|
          idx.delete_tup(tid, tup)
        end

        table = @tables[table_name.to_sym]
        table[tid] = nil
      else
        raise "apply_tx: unsupported op #{op}"
      end
    end

    def index_scan(index_name, sargs = {}, scan_order = :asc, context = nil)
      raise "no index of name #{index_name}" unless @indexes[index_name]

      index = @indexes[index_name]
      index.scan(sargs, scan_order, context).lazy
    end

    def table_scan(table_name)
      raise "no table of name #{@tables[table_name]}" unless @tables[table_name]

      # TODO: tup.dup to protect against change-by-reference?
      #       we probably rely on change by ref currently, so
      #       we'd likely need to rewrite things to be mvcc-like

      table = @tables[table_name]
      table.lazy.reject { |tup| tup.nil? }
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
      count = 0
      indexes = indexes_for(table_name)
      table = @tables[table_name]

      tups.each do |tup|
        tid = table.size
        tup[:_tid] = tid

        table << tup

        indexes.each do |k, idx|
          idx.insert_tup(tid, tup)
        end

        @journal.append(:insert, [table_name, tid, tup])

        count += 1
      end

      count
    end

    # TODO: batchify?
    def update(table_name, tup, update)
      tid = tup[:_tid]

      tup1 = tup.dup
      update.call(tup)
      tup2 = tup.dup

      indexes = indexes_for(table_name)
      indexes.each do |k, idx|
        idx.delete_tup(tid, tup1)
        idx.insert_tup(tid, tup2)
      end

      @journal.append(:update, [table_name, tid, tup1, tup2])
    end

    # TODO: batchify?
    def delete(table_name, tup)
      indexes = indexes_for(table_name)

      tid = tup[:_tid]

      indexes.each do |k, idx|
        idx.delete_tup(tid, tup)
      end

      # TODO: reclaim deleted tuple slots

      table = @tables[table_name]
      table[tid] = nil

      @journal.append(:delete, [table_name, tid, tup])
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

    def index(index_name)
      @indexes[index_name]
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
