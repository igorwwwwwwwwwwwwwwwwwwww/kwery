require 'binary_search_tree'
require 'csv'

module Kwery
  class Tree
    def initialize
      @bst = BinarySearchTree.new
    end

    def insert(k, v)
      @bst.insert(k, v)
    end

    def scan(order = :asc, &block)
      if order == :asc
        scan_leaf_asc(@bst.root, &block)
      else
        scan_leaf_desc(@bst.root, &block)
      end
    end

    private

    def scan_leaf_asc(leaf, &block)
      return if leaf.nil?
      scan_leaf_asc(leaf.left, &block)
      block.call([leaf.key, leaf.value])
      scan_leaf_asc(leaf.right, &block)
    end

    def scan_leaf_desc(leaf, &block)
      return if leaf.nil?
      scan_leaf_desc(leaf.right, &block)
      block.call([leaf.key, leaf.value])
      scan_leaf_desc(leaf.left, &block)
    end
  end

  class Schema
    def initialize
      @fields = []
    end

    def column(name, type)
      @fields << { name: name, type: type }
    end

    def tuple(row)
      tup = {}
      @fields.each do |field|
        name = field[:name]
        type = field[:type]
        tup[name] = apply_type(row[name], type)
      end
      tup
    end

    def apply_type(v, type)
      return nil if v.nil?
      case type
      when :integer
        Integer(v)
      when :string
        v
      when :boolean
        v.downcase == 'true' ? true : false
      else
        raise "unknown type #{type}"
      end
    end
  end

  module Plan
    class IndexScan
      def initialize(table, index, direction = :asc)
        @table = table
        @index = index
        @direction = direction
      end

      def each
        @index.scan(@direction) do |_, tid|
          yield @table[tid]
        end
      end
    end
  end
end

schema = Kwery::Schema.new
schema.column :id, :integer
schema.column :name, :string
schema.column :active, :boolean

table = []
index = Kwery::Tree.new

csv = CSV.table('users.csv', converters: nil)
csv.each do |row|
  tup = schema.tuple(row)
  table << tup
  index.insert(tup[:id], table.size-1)
end

plan = Kwery::Plan::IndexScan.new(
  table,
  index,
  :desc
)

plan.each do |tup|
  puts tup
end

# Limit(10)
#   Filter('active = true')
#     IndexScan('users', 'id', nil, nil, 'desc', ['name', 'active'])
