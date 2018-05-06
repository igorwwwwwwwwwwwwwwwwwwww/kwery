require 'kwery'

RSpec.describe Kwery::Plan::IndexScan do
  catalog = Kwery::Catalog.new

  catalog.table :users, Kwery::Catalog::Table.new(
    columns: {
      id:     Kwery::Catalog::Column.new(:integer),
      name:   Kwery::Catalog::Column.new(:string),
    },
    indexes: [:users_idx_name],
  )
  catalog.index :users_idx_name, Kwery::Catalog::Index.new(:users, [
    Kwery::Catalog::IndexedExpr.new(Kwery::Expr::Column.new(:name), :asc),
  ])

  schema = catalog.new_schema
  schema.bulk_insert(:users, [
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
  ])
  schema.reindex(:users, :users_idx_name)

  it "scans the whole index in sorted order" do
    plan = Kwery::Plan::IndexScan.new(:users, :users_idx_name)

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
      {id: 2,  name: "Xantha"},
    ])

    expect(context.stats).to eq({
      index_tuples_scanned: 10,
    })
  end

  it "scans only as much as needed by limit" do
    plan = Kwery::Plan::IndexScan.new(:users, :users_idx_name)
    plan = Kwery::Plan::Limit.new(5, plan)

    context = Kwery::Plan::Context.new(schema)
    result = plan.call(context)

    expect(result.to_a).to eq([
      {id: 10, name: "Anastasia"},
      {id: 6,  name: "Emi"},
      {id: 4,  name: "Hedley"},
      {id: 7,  name: "Herrod"},
      {id: 3,  name: "Hope"},
    ])

    expect(context.stats).to eq({
      index_tuples_scanned: 5,
    })
  end

  it "filters properly" do
    pred = lambda { |tup| tup[:id] == 8 }

    plan = Kwery::Plan::IndexScan.new(:users, :users_idx_name)
    plan = Kwery::Plan::Filter.new(pred, plan)

    context = Kwery::Plan::Context.new(schema)
    result = plan.call(context)

    expect(result.to_a).to eq([
      {id: 8,  name: "Quincy"},
    ])

    expect(context.stats).to eq({
      index_tuples_scanned: 10,
    })
  end

  it "filters lazily" do
    pred = lambda { |tup| tup[:id] == 8 }

    plan = Kwery::Plan::IndexScan.new(:users, :users_idx_name)
    plan = Kwery::Plan::Filter.new(pred, plan)
    plan = Kwery::Plan::Limit.new(1, plan)

    context = Kwery::Plan::Context.new(schema)
    result = plan.call(context)

    expect(result.to_a).to eq([
      {id: 8,  name: "Quincy"},
    ])

    expect(context.stats).to eq({
      index_tuples_scanned: 7,
    })
  end

  it "sorts the result set" do
    comp = lambda { |tup_a, tup_b| tup_a[:name] <=> tup_b[:name] }

    plan = Kwery::Plan::IndexScan.new(:users, :users_idx_name)
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
      {id: 2,  name: "Xantha"},
    ])

    expect(context.stats).to eq({
      index_tuples_scanned: 10,
    })
  end

  it "scans everything while sorting despite limit" do
    comp = lambda { |tup_a, tup_b| tup_a[:name] <=> tup_b[:name] }

    plan = Kwery::Plan::IndexScan.new(:users, :users_idx_name)
    plan = Kwery::Plan::Sort.new(comp, plan)
    plan = Kwery::Plan::Limit.new(2, plan)

    context = Kwery::Plan::Context.new(schema)
    result = plan.call(context)

    expect(result.to_a).to eq([
      {id: 10, name: "Anastasia"},
      {id: 6,  name: "Emi"},
    ])

    expect(context.stats).to eq({
      index_tuples_scanned: 10,
    })
  end

  it "projects selected values" do
    proj = lambda { |tup| tup[:name] }

    plan = Kwery::Plan::IndexScan.new(:users, :users_idx_name)
    plan = Kwery::Plan::Project.new(proj, plan)
    plan = Kwery::Plan::Limit.new(2, plan)

    context = Kwery::Plan::Context.new(schema)
    result = plan.call(context)

    expect(result.to_a).to eq([
      "Anastasia",
      "Emi",
    ])

    expect(context.stats).to eq({
      index_tuples_scanned: 2,
    })
  end

  it "performs a direct lookup with match search args" do
    sargs = {
      eq: ["Quincy"],
    }
    plan = Kwery::Plan::IndexScan.new(:users, :users_idx_name, :asc, sargs)

    context = Kwery::Plan::Context.new(schema)
    result = plan.call(context)

    expect(result.to_a).to eq([
      {id: 8,  name: "Quincy"},
    ])

    expect(context.stats).to eq({
      index_tuples_scanned: 1,
    })
  end
end