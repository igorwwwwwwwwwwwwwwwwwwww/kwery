module Kwery
  module Executor
    class Aggregate
      class IndexOnlyScanCount
        def init
          0
        end

        def reduce(state, tup)
          state + tup[:_count]
        end

        def combine(states)
          states.reduce(:+)
        end

        def render(state)
          state
        end
      end

      class Count < Struct.new(:exprs)
        def init
          0
        end

        def reduce(state, tup)
          state + 1
        end

        def combine(states)
          states.reduce(:+)
        end

        def render(state)
          state
        end
      end

      class Sum < Struct.new(:exprs)
        def init
          0
        end

        def reduce(state, tup)
          val = exprs[0].call(tup)
          raise "sum: invalid expr #{exprs[0]} on called on tup #{tup}" unless val
          state + val
        end

        def combine(states)
          states.reduce(:+)
        end

        def render(state)
          state
        end
      end

      class Max < Struct.new(:exprs)
        def init
          0
        end

        def reduce(state, tup)
          val = exprs[0].call(tup)
          raise "max: invalid expr #{exprs[0]} on called on tup #{tup}" unless val
          [state, val].max
        end

        def combine(states)
          states.max
        end

        def render(state)
          state
        end
      end

      class Avg < Struct.new(:exprs)
        def init
          {count: 0, sum: 0}
        end

        def reduce(state, tup)
          val = exprs[0].call(tup)
          raise "avg: invalid expr #{exprs[0]} on called on tup #{tup}" unless val
          {
            count: state[:count] + 1,
            sum:   state[:sum] + val,
          }
        end

        def combine(states)
          {
            count: states.map { |state| state[:count] }.reduce(:+),
            sum:   states.map { |state| state[:sum] }.reduce(:+),
          }
        end

        def render(state)
          if state[:count] > 0
            state[:sum] / state[:count]
          else
            0
          end
        end
      end
    end
  end
end
