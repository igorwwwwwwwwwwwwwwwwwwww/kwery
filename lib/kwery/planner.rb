require 'set'

# this is the query planner, sometimes also called "optimizer"

module Kwery
  class Planner
    class UnsupportedQueryError < StandardError
    end

    def initialize(schema, query)
      @schema = schema
      @query = query
    end

    def call
      plan = remote_query || select_query || insert_query || update_query || delete_query
      plan = explain(plan) if @query.options[:explain]
      plan
    end

    private

    def remote_query
      @remote_planner ||= Kwery::Planner::Remote.new(@schema, @query)
      @remote_planner.call
    end

    def select_query
      @select_planner ||= Kwery::Planner::Select.new(@schema, @query)
      @select_planner.call
    end

    def insert_query
      @insert_planner ||= Kwery::Planner::Insert.new(@schema, @query)
      @insert_planner.call
    end

    def update_query
      @update_planner ||= Kwery::Planner::Update.new(@schema, @query)
      @update_planner.call
    end

    def delete_query
      @delete_planner ||= Kwery::Planner::Delete.new(@schema, @query)
      @delete_planner.call
    end

    def explain(plan)
      plan = Kwery::Executor::Explain.new(plan)
      plan
    end
  end
end
