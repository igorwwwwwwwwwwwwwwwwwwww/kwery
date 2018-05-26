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

require 'set'

module Kwery
  class Planner
    class IndexMatcher
      NEQ_OPERATORS = [
        Kwery::Expr::Gt,
        Kwery::Expr::Gte,
        Kwery::Expr::Lt,
        Kwery::Expr::Lte,
      ]

      def initialize(catalog, query)
        @catalog = catalog
        @query = query
      end

      def match
        candidates = []

        index_exprs = @catalog.tables[@query.from].indexes
          .map { |k| [k, @catalog.indexes[k]] }
          .map { |k, idx| [k, idx.indexed_exprs] }

        index_exprs.each do |index_name, indexed_exprs|
          index_candidates = matches(index_name, indexed_exprs)
          candidates.concat(index_candidates)
        end

        candidates
      end

      private

      def matches(index_name, indexed_exprs)
        candidates = []

        # * match eq against indexes       => fully satisfied indexes are candidates
        if indexed_exprs.map(&:expr).all? { |expr| eq_exprs.keys.include?(expr) }
          sargs = {
            eq: indexed_exprs
                  .map(&:expr)
                  .map { |expr| eq_exprs[expr] }
          }
          candidates << Candidate.new(
            index_name: index_name,
            sargs: sargs,
            recheck: recheck?(indexed_exprs),
          )
        end

        # * match order_by against indexes => fully satisfied indexes are candidates
        if indexed_exprs == @query.order_by
          candidates << Candidate.new(
            index_name: index_name,
            sargs: {},
            sorted: true,
            recheck: recheck?(indexed_exprs),
          )
        end

        # * match ranges against indexes   => fully satisfied indexes are candidates
        if neq_exprs.size == 1 && indexed_exprs.size == 1
          expr = indexed_exprs.map(&:expr).first

          neq_exprs.each do |neq_expr, neq_conds|
            if expr == neq_expr
              sargs = neq_conds
                .map { |op, value| [op.sarg_key, [value]] }
                .to_h
              candidates << Candidate.new(
                index_name: index_name,
                sargs: sargs,
                recheck: recheck?(indexed_exprs),
              )
            end
          end
        end

        # * prefix match eq against indexes
        prefixes_for(indexed_exprs).each do |prefix|
          #   * for each prefix, match (prefix || order_by)              => candidate
          if prefix.map(&:expr).all? { |expr| eq_exprs.keys.include?(expr) }
            if prefix.dup.concat(@query.order_by) == indexed_exprs
              sargs_prefix = prefix
                .map(&:expr)
                .map { |expr| eq_exprs[expr] }
              sargs = {
                eq: sargs_prefix
              }
              candidates << Candidate.new(
                index_name: index_name,
                sargs: sargs,
                sorted: true,
                recheck: recheck?(indexed_exprs),
              )
            end
          end

          #   * for each prefix, and each range constraint
          #     column expr, match (prefix || column expr)               => candidate
          neq_exprs.each do |neq_expr, neq_conds|
            if prefix.map(&:expr).dup.concat([neq_expr]) == indexed_exprs.map(&:expr)
              sargs_prefix = prefix
                .map(&:expr)
                .map { |expr| eq_exprs[expr] }

              sargs = neq_conds
                .map { |op, value| [op.sarg_key, sargs_prefix.dup.concat([value])] }
                .to_h

              sargs[:eq] = sargs_prefix

              candidates << Candidate.new(
                index_name: index_name,
                sargs: sargs,
                recheck: recheck?(indexed_exprs),
              )
            end
          end
        end

        candidates
      end

      def eq_exprs
        @eq_exprs ||= @query.where
          .select { |expr| Kwery::Expr::Eq === expr }
          .select { |expr| Kwery::Expr::Literal === expr.right }
          .map { |expr| [expr.left, expr.right.value] }
          .to_h
      end

      def neq_exprs
        @neq_exprs ||= @query.where
          .select { |expr| NEQ_OPERATORS.include?(expr.class) }
          .select { |expr| Kwery::Expr::Literal === expr.right }
          .map { |expr| [expr.left, expr.class, expr.right.value] }
          .group_by { |args| args[0] }
          .map { |k, argses| [k, argses.map { |args| [args[1], args[2]] }.to_h] }
          .to_h
      end

      def prefixes_for(indexed_exprs)
        1.upto(indexed_exprs.size).map do |i|
          indexed_exprs.slice(0, i)
        end
      end

      # assume we are matching against the whole index
      # in the future we should check which fields of
      # the index are actually used by the sargs
      def recheck?(indexed_exprs)
        query_exprs = eq_exprs.keys.to_set + neq_exprs.keys.to_set
        remaining_exprs = query_exprs - indexed_exprs.map(&:expr)
        remaining_exprs.size > 0
      end

      class Candidate
        attr_accessor :index_name, :sargs, :sorted, :recheck

        def initialize(index_name:, sargs:, sorted: false, recheck: false)
          @index_name = index_name
          @sargs = sargs
          @sorted = sorted
          @recheck = recheck
        end
      end
    end
  end
end
