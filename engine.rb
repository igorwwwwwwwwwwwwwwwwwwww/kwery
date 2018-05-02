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

    def scan(order = :asc)
      if order == :asc
        scan_leaf_asc(@bst.root)
      else
        scan_leaf_desc(@bst.root)
      end
    end

    private

    def scan_leaf_asc(leaf)
      return [] if leaf.nil?
      Enumerator.new do |y|
        scan_leaf_asc(leaf.left).each do |v|
          y << v
        end
        y << leaf.value
        scan_leaf_asc(leaf.right).each do |v|
          y << v
        end
      end
    end

    def scan_leaf_desc(leaf)
      Enumerator.new do |y|
        if leaf.right
          scan_leaf_desc(leaf.right).each do |v|
            y << v
          end
        end
        y << leaf.value
        if leaf.left
          scan_leaf_desc(leaf.left).each do |v|
            y << v
          end
        end
      end
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
      include Enumerable

      def initialize(table, index, order = :asc)
        @table = table
        @index = index
        @order = order
      end

      def call
        @index.scan(@order).lazy.map {|tid|
          tup = @table[tid]
          tup
        }
      end
    end

    class Filter
      include Enumerable

      def initialize(pred, plan)
        @pred = pred
        @plan = plan
      end

      def call
        @plan.call.select(&@pred)
      end
    end

    class Limit
      def initialize(limit, plan)
        @limit = limit
        @plan = plan
      end

      def call
        @plan.call.take(@limit)
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

plan = Kwery::Plan::Limit.new(10,
  Kwery::Plan::Filter.new(lambda { |tup| tup[:active] },
    Kwery::Plan::IndexScan.new(table, index, :desc)
  )
)

plan.call.each do |tup|
  puts tup
end
