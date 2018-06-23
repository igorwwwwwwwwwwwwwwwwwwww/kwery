require 'kwery'

RSpec.describe Kwery::Executor::Aggregate do
  catalog = Kwery::Catalog.new

  catalog.table :users, Kwery::Catalog::Table.new

  schema = catalog.new_schema
  schema.bulk_insert(:users, [
    {id: 1,  name: "Kathleen",  active: false},
    {id: 2,  name: "Xantha",    active: true},
    {id: 3,  name: "Hope",      active: true},
    {id: 4,  name: "Hedley",    active: false},
    {id: 5,  name: "Reese",     active: true},
    {id: 6,  name: "Emi",       active: true},
    {id: 7,  name: "Herrod",    active: true},
    {id: 8,  name: "Quincy",    active: true},
    {id: 9,  name: "Uta",       active: false},
    {id: 10, name: "Anastasia", active: false},
  ])

  it "counts all records" do
    reduce = lambda { |sum, tup| sum + 1 }
    render = lambda { |state| {count: state} }

    plan = Kwery::Executor::TableScan.new(:users)
    plan = Kwery::Executor::Aggregate.new(0, reduce, render, plan)

    context = Kwery::Executor::Context.new(schema)
    result = plan.call(context)

    expect(result.to_a).to eq([
      {count: 10},
    ])

    expect(context.stats).to eq({
      table_tuples_scanned: 10,
    })
  end

  it "counts records grouped by active" do
    group_by = lambda { |tup| tup[:active] }
    reduce = lambda { |sum, tup| sum + 1 }
    render = lambda { |k, v| {active: k, count: v} }

    plan = Kwery::Executor::TableScan.new(:users)
    plan = Kwery::Executor::HashAggregate.new(0, group_by, reduce, render, plan)

    context = Kwery::Executor::Context.new(schema)
    result = plan.call(context)

    expect(result.to_a).to eq([
      {active: false, count: 4},
      {active: true,  count: 6},
    ])

    expect(context.stats).to eq({
      table_tuples_scanned: 10,
    })
  end
end
