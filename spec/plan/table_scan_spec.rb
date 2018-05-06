require 'kwery'

RSpec.describe Kwery::Plan::TableScan do
  catalog = Kwery::Catalog.new

  catalog.table :users, Kwery::Catalog::Table.new(
    columns: {
      id:     Kwery::Catalog::Column.new(:integer),
      name:   Kwery::Catalog::Column.new(:string),
      active: Kwery::Catalog::Column.new(:boolean),
    },
  )

  schema = {}
  schema[:users] = [
    {id: 1,  name: "Kathleen"},
    {id: 2,  name: "Xantha"},
    {id: 3,  name: "Hope"},
    {id: 4,  name: "Hedley"},
    {id: 5,  name: "Reese"},
    {id: 6,  name: "Emi"},
    {id: 7,  name: "Herrod"},
    {id: 8,  name: "Quincy"},
    {id: 9,  name: "Uta"},
    {id: 10, name: "Anastasia"}
  ]

  it "scans the whole table" do
    plan = Kwery::Plan::TableScan.new(:users)

    context = Kwery::Plan::Context.new(schema)
    result = plan.call(context)

    expect(result.to_a).to eq([
      {id: 1,  name: "Kathleen"},
      {id: 2,  name: "Xantha"},
      {id: 3,  name: "Hope"},
      {id: 4,  name: "Hedley"},
      {id: 5,  name: "Reese"},
      {id: 6,  name: "Emi"},
      {id: 7,  name: "Herrod"},
      {id: 8,  name: "Quincy"},
      {id: 9,  name: "Uta"},
      {id: 10, name: "Anastasia"},
    ])

    expect(context.stats).to eq({
      table_tuples_scanned: 10,
    })
  end

  it "scans only as much as needed by limit" do
    plan = Kwery::Plan::TableScan.new(:users)
    plan = Kwery::Plan::Limit.new(5, plan)

    context = Kwery::Plan::Context.new(schema)
    result = plan.call(context)

    expect(result.to_a).to eq([
      {id: 1,  name: "Kathleen"},
      {id: 2,  name: "Xantha"},
      {id: 3,  name: "Hope"},
      {id: 4,  name: "Hedley"},
      {id: 5,  name: "Reese"},
    ])

    expect(context.stats).to eq({
      table_tuples_scanned: 5,
    })
  end

  it "filters properly" do
    pred = lambda { |tup| tup[:id] == 8 }

    plan = Kwery::Plan::TableScan.new(:users)
    plan = Kwery::Plan::Filter.new(pred, plan)

    context = Kwery::Plan::Context.new(schema)
    result = plan.call(context)

    expect(result.to_a).to eq([
      {id: 8,  name: "Quincy"},
    ])

    expect(context.stats).to eq({
      table_tuples_scanned: 10,
    })
  end

  it "filters lazily" do
    pred = lambda { |tup| tup[:id] == 8 }

    plan = Kwery::Plan::TableScan.new(:users)
    plan = Kwery::Plan::Filter.new(pred, plan)
    plan = Kwery::Plan::Limit.new(1, plan)

    context = Kwery::Plan::Context.new(schema)
    result = plan.call(context)

    expect(result.to_a).to eq([
      {id: 8,  name: "Quincy"},
    ])

    expect(context.stats).to eq({
      table_tuples_scanned: 8,
    })
  end

  it "sorts the result set" do
    comp = lambda { |tup_a, tup_b| tup_a[:name] <=> tup_b[:name] }

    plan = Kwery::Plan::TableScan.new(:users)
    plan = Kwery::Plan::Sort.new(comp, plan)

    context = Kwery::Plan::Context.new(schema)
    result = plan.call(context)

    expect(result.to_a).to eq([
      {id: 10, name: "Anastasia"},
      {id: 6,  name: "Emi"},
      {id: 4,  name: "Hedley"},
      {id: 7,  name: "Herrod"},
      {id: 3,  name: "Hope"},
      {id: 1,  name: "Kathleen"},
      {id: 8,  name: "Quincy"},
      {id: 5,  name: "Reese"},
      {id: 9,  name: "Uta"},
      {id: 2,  name: "Xantha"}
    ])

    expect(context.stats).to eq({
      table_tuples_scanned: 10,
    })
  end

  it "scans everything while sorting despite limit" do
    comp = lambda { |tup_a, tup_b| tup_a[:name] <=> tup_b[:name] }

    plan = Kwery::Plan::TableScan.new(:users)
    plan = Kwery::Plan::Sort.new(comp, plan)
    plan = Kwery::Plan::Limit.new(2, plan)

    context = Kwery::Plan::Context.new(schema)
    result = plan.call(context)

    expect(result.to_a).to eq([
      {id: 10, name: "Anastasia"},
      {id: 6,  name: "Emi"},
    ])

    expect(context.stats).to eq({
      table_tuples_scanned: 10,
    })
  end

  it "projects selected values" do
    proj = lambda { |tup| tup[:name] }

    plan = Kwery::Plan::TableScan.new(:users)
    plan = Kwery::Plan::Project.new(proj, plan)
    plan = Kwery::Plan::Limit.new(2, plan)

    context = Kwery::Plan::Context.new(schema)
    result = plan.call(context)

    expect(result.to_a).to eq([
      "Kathleen",
      "Xantha",
    ])

    expect(context.stats).to eq({
      table_tuples_scanned: 2,
    })
  end
end
