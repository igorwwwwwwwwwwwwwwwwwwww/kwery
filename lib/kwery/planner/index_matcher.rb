# here is a rough outline for how all of this could be made
# somewhat more robust:
#
# * parse eq and range constraints from query (range constraints grouped
#   by column expr)
# * match eq against indexes       => fully satisfied indexes are candidates
# * match ranges against indexes   => fully satisfied indexes are candidates
# * match order_by against indexes => fully satisfied indexes are candidates
# * prefix match eq against indexes
#   * for each prefix, match (prefix || order_by)              => candidate
#   * for each prefix, and each range constraint
#     column expr, match (prefix || column expr)               => candidate
# * rank candidates by (indexed_exprs.size +
#   5 * has_order_by), pick the highest one
# * figure out sargs for the index
# * figure out which query conditions have not been satisfied,
#   add Filter and Sort nodes if needed

module Kwery
  class Planner
    class IndexMatcher
      def initialize(catalog, query)
        @catalog = catalog
        @query = query
      end

      def match
        candidates = []

        eq_exprs = @query.where
          .select { |expr| Kwery::Expr::Eq === expr }
          .select { |expr| Kwery::Expr::Literal === expr.right }
          .map { |expr| [expr.left, expr.right.value] }
          .to_h

        index_exprs = @catalog.tables[@query.from].indexes
          .map { |k| [k, @catalog.indexes[k]] }
          .map { |k, idx| [k, idx.indexed_exprs.map(&:expr)] }

        index_exprs.each do |index_name, exprs|
          if match?(exprs, eq_exprs)
            sargs = make_sargs(index_name, exprs, eq_exprs)
            candidates << Candidate.new(
              index_name: index_name,
              sargs: sargs,
            )
          end
        end

        candidates
      end

      private

      def match?(index_exprs, eq_exprs)
        index_exprs.all? { |expr| eq_exprs.keys.include?(expr) }
      end

      def make_sargs(index_name, index_exprs, eq_exprs)
        { eq: index_exprs.map { |expr| eq_exprs[expr] } }
      end

      class Candidate
        attr_accessor :index_name, :sargs

        def initialize(index_name:, sargs:)
          @index_name = index_name
          @sargs = sargs
        end
      end
    end
  end
end
