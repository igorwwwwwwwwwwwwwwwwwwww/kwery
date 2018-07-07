require 'binary_search_tree'
require 'set'

module Kwery
  class Index
    attr_accessor :table_name
    attr_accessor :indexed_exprs

    def initialize(table_name:, indexed_exprs:, comparator: nil)
      @table_name = table_name
      @indexed_exprs = indexed_exprs
      @bst = BinarySearchTree.new(comparator: comparator)
    end

    def scan(sargs = {}, scan_order = :asc, context = nil)
      @bst.scan_leaf(@bst.root, sargs, scan_order, context)
    end

    def insert_tup(tid, tup)
      key = tup_key(tup)
      return if key.all? { |k| k.nil? }

      vals = @bst.find(key)&.value
      unless vals
        vals = Set.new
        @bst.insert(key, vals)
      end
      vals << tid
    end

    def delete_tup(tid, tup)
      key = tup_key(tup)
      return if key.all? { |k| k.nil? }

      vals = @bst.find(key)&.value
      vals.delete(tid) if vals
    end

    def tup_key(tup)
      @indexed_exprs
        .map(&:expr)
        .map { |expr| expr.call(tup) }
    end
  end
end
