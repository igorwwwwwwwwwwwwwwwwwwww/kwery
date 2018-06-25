require 'kwery'

RSpec.describe Kwery::Executor::Aggregate do
  schema = Kwery::Schema.new
  schema.create_table(:users)

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
    agg = Kwery::Executor::AggCount.new

    plan = Kwery::Executor::TableScan.new(:users)
    plan = Kwery::Executor::Aggregate.new(:_0, agg, plan)

    context = Kwery::Executor::Context.new(schema)
    result = plan.call(context)

    expect(result.to_a).to eq([
      {_0: 10},
    ])

    expect(context.stats).to eq({
      table_tuples_scanned: 10,
    })
  end

  it "counts records grouped by active" do
    agg = Kwery::Executor::AggCount.new
    group_by = lambda { |tup| tup[:active] }

    plan = Kwery::Executor::TableScan.new(:users)
    plan = Kwery::Executor::HashAggregate.new(:_0, agg, :active, group_by, plan)

    context = Kwery::Executor::Context.new(schema)
    result = plan.call(context)

    expect(result.to_a).to eq([
      {active: false, _0: 4},
      {active: true,  _0: 6},
    ])

    expect(context.stats).to eq({
      table_tuples_scanned: 10,
    })
  end

  it "counts records grouped by without group key" do
    agg = Kwery::Executor::AggCount.new
    group_by = lambda { |tup| tup[:active] }

    plan = Kwery::Executor::TableScan.new(:users)
    plan = Kwery::Executor::HashAggregate.new(:_0, agg, nil, group_by, plan)

    context = Kwery::Executor::Context.new(schema)
    result = plan.call(context)

    expect(result.to_a).to eq([
      {_0: 4},
      {_0: 6},
    ])

    expect(context.stats).to eq({
      table_tuples_scanned: 10,
    })
  end
end
