require 'kwery'

RSpec.describe Kwery::Executor::IndexScan do
  schema = Kwery::Schema.new
  schema.create_table(:users)

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
    {id: 10, name: "Anastasia"},
  ])

  schema.create_index(:users, :users_idx_name, [
    Kwery::Index::IndexedExpr.new(Kwery::Expr::Column.new(:name)),
  ])
  schema.create_index(:users, :users_idx_name_id, [
    Kwery::Index::IndexedExpr.new(Kwery::Expr::Column.new(:name)),
    Kwery::Index::IndexedExpr.new(Kwery::Expr::Column.new(:id)),
  ])

  it "scans the whole index in sorted order" do
    plan = Kwery::Executor::IndexScan.new(:users, :users_idx_name)

    context = Kwery::Executor::Context.new(schema)
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
      index_comparisons: 10,
    })
  end

  it "scans only as much as needed by limit" do
    plan = Kwery::Executor::IndexScan.new(:users, :users_idx_name)
    plan = Kwery::Executor::Limit.new(5, plan)

    context = Kwery::Executor::Context.new(schema)
    result = plan.call(context)

    expect(result.to_a).to eq([
      {id: 10, name: "Anastasia"},
      {id: 6,  name: "Emi"},
      {id: 4,  name: "Hedley"},
      {id: 7,  name: "Herrod"},
      {id: 3,  name: "Hope"},
    ])

    # theory: this is 6 because the iterator advances to the next
    # item before limit effectively limits it
    expect(context.stats).to eq({
      index_tuples_scanned: 5,
      index_comparisons: 6,
    })
  end

  it "filters properly" do
    pred = lambda { |tup| tup[:id] == 8 }

    plan = Kwery::Executor::IndexScan.new(:users, :users_idx_name)
    plan = Kwery::Executor::Filter.new(pred, plan)

    context = Kwery::Executor::Context.new(schema)
    result = plan.call(context)

    expect(result.to_a).to eq([
      {id: 8,  name: "Quincy"},
    ])

    expect(context.stats).to eq({
      index_tuples_scanned: 10,
      index_comparisons: 10,
    })
  end

  it "filters lazily" do
    pred = lambda { |tup| tup[:id] == 8 }

    plan = Kwery::Executor::IndexScan.new(:users, :users_idx_name)
    plan = Kwery::Executor::Filter.new(pred, plan)
    plan = Kwery::Executor::Limit.new(1, plan)

    context = Kwery::Executor::Context.new(schema)
    result = plan.call(context)

    expect(result.to_a).to eq([
      {id: 8,  name: "Quincy"},
    ])

    expect(context.stats).to eq({
      index_tuples_scanned: 7,
      index_comparisons: 8,
    })
  end

  it "sorts the result set" do
    comp = lambda { |tup_a, tup_b| tup_a[:name] <=> tup_b[:name] }

    plan = Kwery::Executor::IndexScan.new(:users, :users_idx_name)
    plan = Kwery::Executor::Sort.new(comp, plan)

    context = Kwery::Executor::Context.new(schema)
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
      index_comparisons: 10,
    })
  end

  it "scans everything while sorting despite limit" do
    comp = lambda { |tup_a, tup_b| tup_a[:name] <=> tup_b[:name] }

    plan = Kwery::Executor::IndexScan.new(:users, :users_idx_name)
    plan = Kwery::Executor::Sort.new(comp, plan)
    plan = Kwery::Executor::Limit.new(2, plan)

    context = Kwery::Executor::Context.new(schema)
    result = plan.call(context)

    expect(result.to_a).to eq([
      {id: 10, name: "Anastasia"},
      {id: 6,  name: "Emi"},
    ])

    expect(context.stats).to eq({
      index_tuples_scanned: 10,
      index_comparisons: 10,
    })
  end

  it "projects selected values" do
    proj = lambda { |tup| tup[:name] }

    plan = Kwery::Executor::IndexScan.new(:users, :users_idx_name)
    plan = Kwery::Executor::Project.new(proj, plan)
    plan = Kwery::Executor::Limit.new(2, plan)

    context = Kwery::Executor::Context.new(schema)
    result = plan.call(context)

    expect(result.to_a).to eq([
      "Anastasia",
      "Emi",
    ])

    # not sure why we have 4 comparisons here
    expect(context.stats).to eq({
      index_tuples_scanned: 2,
      index_comparisons: 4,
    })
  end

  it "performs a direct lookup with eq sarg" do
    sargs = {
      eq: ["Quincy"],
    }
    plan = Kwery::Executor::IndexScan.new(:users, :users_idx_name, sargs, :asc)

    context = Kwery::Executor::Context.new(schema)
    result = plan.call(context)

    expect(result.to_a).to eq([
      {id: 8,  name: "Quincy"},
    ])

    expect(context.stats).to eq({
      index_tuples_scanned: 1,
      index_comparisons: 3,
    })
  end

  it "handles non-matching eq sarg" do
    sargs = {
      eq: ["Quixote"],
    }
    plan = Kwery::Executor::IndexScan.new(:users, :users_idx_name, sargs, :asc)

    context = Kwery::Executor::Context.new(schema)
    result = plan.call(context)

    expect(result.to_a).to eq([])

    expect(context.stats).to eq({
      index_comparisons: 3,
    })
  end

  it "performs a range scan with gt sarg" do
    sargs = {
      gt: ["Kathleen"],
    }
    plan = Kwery::Executor::IndexScan.new(:users, :users_idx_name, sargs, :asc)

    context = Kwery::Executor::Context.new(schema)
    result = plan.call(context)

    expect(result.to_a).to eq([
      {id: 8,  name: "Quincy"},
      {id: 5,  name: "Reese"},
      {id: 9,  name: "Uta"},
      {id: 2,  name: "Xantha"},
    ])

    expect(context.stats).to eq({
      index_tuples_scanned: 4,
      index_comparisons: 5,
    })
  end

  it "handles non-matching gt sarg" do
    sargs = {
      gt: ["Yves"],
    }
    plan = Kwery::Executor::IndexScan.new(:users, :users_idx_name, sargs, :asc)

    context = Kwery::Executor::Context.new(schema)
    result = plan.call(context)

    expect(result.to_a).to eq([])
    expect(context.stats).to eq({
      index_comparisons: 3,
    })
  end

  it "performs a range scan with gte sarg" do
    sargs = {
      gte: ["Quincy"],
    }
    plan = Kwery::Executor::IndexScan.new(:users, :users_idx_name, sargs, :asc)

    context = Kwery::Executor::Context.new(schema)
    result = plan.call(context)

    expect(result.to_a).to eq([
      {id: 8,  name: "Quincy"},
      {id: 5,  name: "Reese"},
      {id: 9,  name: "Uta"},
      {id: 2,  name: "Xantha"},
    ])

    expect(context.stats).to eq({
      index_tuples_scanned: 4,
      index_comparisons: 5,
    })
  end

  it "performs a range scan with lt sarg" do
    sargs = {
      lt: ["Herrod"],
    }
    plan = Kwery::Executor::IndexScan.new(:users, :users_idx_name, sargs, :asc)

    context = Kwery::Executor::Context.new(schema)
    result = plan.call(context)

    expect(result.to_a).to eq([
      {id: 10, name: "Anastasia"},
      {id: 6,  name: "Emi"},
      {id: 4,  name: "Hedley"},
    ])

    expect(context.stats).to eq({
      index_tuples_scanned: 3,
      index_comparisons: 6,
    })
  end

  it "performs a range scan with lte sarg" do
    sargs = {
      lte: ["Herrod"],
    }
    plan = Kwery::Executor::IndexScan.new(:users, :users_idx_name, sargs, :asc)

    context = Kwery::Executor::Context.new(schema)
    result = plan.call(context)

    expect(result.to_a).to eq([
      {id: 10, name: "Anastasia"},
      {id: 6,  name: "Emi"},
      {id: 4,  name: "Hedley"},
      {id: 7,  name: "Herrod"},
    ])

    expect(context.stats).to eq({
      index_tuples_scanned: 4,
      index_comparisons: 6,
    })
  end

  it "scans the whole index backwards" do
    plan = Kwery::Executor::IndexScan.new(:users, :users_idx_name, {}, :desc)

    context = Kwery::Executor::Context.new(schema)
    result = plan.call(context)

    expect(result.to_a).to eq([
      {id: 2,  name: "Xantha"},
      {id: 9,  name: "Uta"},
      {id: 5,  name: "Reese"},
      {id: 8,  name: "Quincy"},
      {id: 1,  name: "Kathleen"},
      {id: 3,  name: "Hope"},
      {id: 7,  name: "Herrod"},
      {id: 4,  name: "Hedley"},
      {id: 6,  name: "Emi"},
      {id: 10, name: "Anastasia"},
    ])

    expect(context.stats).to eq({
      index_tuples_scanned: 10,
      index_comparisons: 10,
    })
  end

  it "performs a backwards range scan with gt sarg" do
    sargs = {
      gt: ["Kathleen"],
    }
    plan = Kwery::Executor::IndexScan.new(:users, :users_idx_name, sargs, :desc)

    context = Kwery::Executor::Context.new(schema)
    result = plan.call(context)

    expect(result.to_a).to eq([
      {id: 2,  name: "Xantha"},
      {id: 9,  name: "Uta"},
      {id: 5,  name: "Reese"},
      {id: 8,  name: "Quincy"},
    ])

    expect(context.stats).to eq({
      index_tuples_scanned: 4,
      index_comparisons: 5,
    })
  end

  it "performs a range scan with gt and lt sargs" do
    sargs = {
      gt: ["Herrod"],
      lt: ["Quincy"],
    }
    plan = Kwery::Executor::IndexScan.new(:users, :users_idx_name, sargs, :asc)

    context = Kwery::Executor::Context.new(schema)
    result = plan.call(context)

    expect(result.to_a).to eq([
      {id: 3,  name: "Hope"},
      {id: 1,  name: "Kathleen"},
    ])

    expect(context.stats).to eq({
      index_tuples_scanned: 2,
      index_comparisons: 6,
    })
  end

  it "performs a range scan with contradictory gt and lt sargs" do
    sargs = {
      gt: ["Quincy"],
      lt: ["Herrod"],
    }
    plan = Kwery::Executor::IndexScan.new(:users, :users_idx_name, sargs, :asc)

    context = Kwery::Executor::Context.new(schema)
    result = plan.call(context)

    expect(result.to_a).to eq([])
    expect(context.stats).to eq({
      index_comparisons: 1,
    })
  end

  it "performs direct lookups with in sarg" do
    sargs = {
      in: [["Hedley"], ["Hope"], ["Kathleen"], ["Xantha"]],
    }
    plan = Kwery::Executor::IndexScan.new(:users, :users_idx_name, sargs, :asc)

    context = Kwery::Executor::Context.new(schema)
    result = plan.call(context)

    expect(result.to_a).to eq([
      {id: 4,  name: "Hedley"},
      {id: 3,  name: "Hope"},
      {id: 1,  name: "Kathleen"},
      {id: 2,  name: "Xantha"},
    ])

    expect(context.stats).to eq({
      index_comparisons: 9,
      index_tuples_scanned: 4,
    })
  end

  it "performs a prefix match with eq sarg" do
    sargs = {
      eq: ["Hope"],
    }
    plan = Kwery::Executor::IndexScan.new(:users, :users_idx_name_id, sargs, :asc)

    context = Kwery::Executor::Context.new(schema)
    result = plan.call(context)

    expect(result.to_a).to eq([
      {id: 3,  name: "Hope"},
    ])

    expect(context.stats).to eq({
      index_comparisons: 3,
      index_tuples_scanned: 1,
    })
  end

  it "performs a prefix match with gt and lt sargs" do
    sargs = {
      gt: ["Herrod"],
      lt: ["Quincy"],
    }
    plan = Kwery::Executor::IndexScan.new(:users, :users_idx_name_id, sargs, :asc)

    context = Kwery::Executor::Context.new(schema)
    result = plan.call(context)

    expect(result.to_a).to eq([
      {id: 3,  name: "Hope"},
      {id: 1,  name: "Kathleen"},
    ])

    expect(context.stats).to eq({
      index_tuples_scanned: 2,
      index_comparisons: 6,
    })
  end

  it "performs a compound prefix match" do
    sargs = {
      eq: ["Hope"],
      gt: ["Hope", 2],
    }
    plan = Kwery::Executor::IndexScan.new(:users, :users_idx_name_id, sargs, :asc)

    context = Kwery::Executor::Context.new(schema)
    result = plan.call(context)

    expect(result.to_a).to eq([
      {id: 3,  name: "Hope"},
    ])

    expect(context.stats).to eq({
      index_tuples_scanned: 1,
      index_comparisons: 4,
    })
  end
end
