require 'kwery'

RSpec.describe Kwery::Executor::Aggregate do
  schema = Kwery::Schema.new
  schema.create_table(:users)

  schema.bulk_insert(:users, [
    {id: 1,  name: "Kathleen",  team: "a", active: false},
    {id: 2,  name: "Xantha",    team: "a", active: true},
    {id: 3,  name: "Hope",      team: "a", active: true},
    {id: 4,  name: "Hedley",    team: "a", active: false},
    {id: 5,  name: "Reese",     team: "b", active: true},
    {id: 6,  name: "Emi",       team: "b", active: true},
    {id: 7,  name: "Herrod",    team: "c", active: true},
    {id: 8,  name: "Quincy",    team: "c", active: true},
    {id: 9,  name: "Uta",       team: "c", active: false},
    {id: 10, name: "Anastasia", team: "d", active: false},
  ])

  it "counts all records" do
    agg = Kwery::Executor::Aggregate::Count.new

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
    agg = Kwery::Executor::Aggregate::Count.new
    group_by = lambda { |tup| [tup[:active]] }

    plan = Kwery::Executor::TableScan.new(:users)
    plan = Kwery::Executor::HashAggregate.new(:_0, agg, group_by, [:active], plan)

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

  it "counts records grouped by without projection" do
    agg = Kwery::Executor::Aggregate::Count.new
    group_by = lambda { |tup| [tup[:active]] }

    plan = Kwery::Executor::TableScan.new(:users)
    plan = Kwery::Executor::HashAggregate.new(:_0, agg, group_by, [], plan)

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

  it "counts records grouped by team" do
    agg = Kwery::Executor::Aggregate::Count.new
    group_by = lambda { |tup| [tup[:team]] }

    plan = Kwery::Executor::TableScan.new(:users)
    plan = Kwery::Executor::HashAggregate.new(:_0, agg, group_by, [:team], plan)

    context = Kwery::Executor::Context.new(schema)
    result = plan.call(context)

    expect(result.to_a).to eq([
      {team: "a", _0: 4},
      {team: "b", _0: 2},
      {team: "c", _0: 3},
      {team: "d", _0: 1},
    ])

    expect(context.stats).to eq({
      table_tuples_scanned: 10,
    })
  end

  it "counts records grouped by team, active" do
    agg = Kwery::Executor::Aggregate::Count.new
    group_by = lambda { |tup| [tup[:team], tup[:active]] }

    plan = Kwery::Executor::TableScan.new(:users)
    plan = Kwery::Executor::HashAggregate.new(:_0, agg, group_by, [:team, :active], plan)

    context = Kwery::Executor::Context.new(schema)
    result = plan.call(context)

    expect(result.to_a).to eq([
      {team: "a", active: false, _0: 2},
      {team: "a", active: true,  _0: 2},
      {team: "b", active: true,  _0: 2},
      {team: "c", active: true,  _0: 2},
      {team: "c", active: false, _0: 1},
      {team: "d", active: false,  _0: 1},
    ])

    expect(context.stats).to eq({
      table_tuples_scanned: 10,
    })
  end

  it "counts records grouped with single projection" do
    agg = Kwery::Executor::Aggregate::Count.new
    group_by = lambda { |tup| [tup[:team], tup[:active]] }

    plan = Kwery::Executor::TableScan.new(:users)
    plan = Kwery::Executor::HashAggregate.new(:_0, agg, group_by, [nil, :active], plan)

    context = Kwery::Executor::Context.new(schema)
    result = plan.call(context)

    expect(result.to_a).to eq([
      {active: false, _0: 2},
      {active: true,  _0: 2},
      {active: true,  _0: 2},
      {active: true,  _0: 2},
      {active: false, _0: 1},
      {active: false,  _0: 1},
    ])

    expect(context.stats).to eq({
      table_tuples_scanned: 10,
    })
  end
end
