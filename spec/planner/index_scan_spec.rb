require 'kwery'

RSpec.describe Kwery::Planner do
  it "performs an index scan for order by" do
    schema = Kwery::Schema.new
    schema.create_table(:users)
    schema.create_index(:users, :users_idx_id, [
      Kwery::Expr::IndexedExpr.new(Kwery::Expr::Column.new(:id), :asc),
    ])

    query = Kwery::Query.new(
      select: {
        id: Kwery::Expr::Column.new(:id)
      },
      from: :users,
      order_by: [
        Kwery::Expr::IndexedExpr.new(Kwery::Expr::Column.new(:id), :asc),
      ],
    )

    plan = query.plan(schema)

    expect(plan.explain).to eq(
      [Kwery::Executor::Project,
        [Kwery::Executor::IndexScan, :users_idx_id, {}]]
    )
  end

  it "performs an index scan for where" do
    schema = Kwery::Schema.new
    schema.create_table(:users)
    schema.create_index(:users, :users_idx_id, [
      Kwery::Expr::IndexedExpr.new(Kwery::Expr::Column.new(:id), :asc),
    ])

    query = Kwery::Query.new(
      select: {
        id: Kwery::Expr::Column.new(:id)
      },
      from: :users,
      where: [
        Kwery::Expr::Eq.new(Kwery::Expr::Column.new(:id), Kwery::Expr::Literal.new(10)),
      ],
    )

    plan = query.plan(schema)

    expect(plan.explain).to eq(
      [Kwery::Executor::Project,
        [Kwery::Executor::IndexScan, :users_idx_id,
          {eq: [10]}]]
    )
  end

  it "matches an indexed expression" do
    schema = Kwery::Schema.new
    schema.create_table(:users)
    schema.create_index(:users, :users_idx_upper_name, [
      Kwery::Expr::IndexedExpr.new(Kwery::Expr::FnCall.new(:upper, [Kwery::Expr::Column.new(:name)]), :asc),
    ])

    query = Kwery::Query.new(
      select: {
        id: Kwery::Expr::Column.new(:id)
      },
      from: :users,
      where: [
        Kwery::Expr::Eq.new(Kwery::Expr::FnCall.new(:upper, [Kwery::Expr::Column.new(:name)]), Kwery::Expr::Literal.new('CARA')),
      ],
    )

    plan = query.plan(schema)

    expect(plan.explain).to eq(
      [Kwery::Executor::Project,
        [Kwery::Executor::IndexScan, :users_idx_upper_name,
          {eq: ['CARA']}]]
    )
  end

  it "matches a compound index" do
    schema = Kwery::Schema.new
    schema.create_table(:users)
    schema.create_index(:users, :users_idx_name_active, [
      Kwery::Expr::IndexedExpr.new(Kwery::Expr::Column.new(:name), :asc),
      Kwery::Expr::IndexedExpr.new(Kwery::Expr::Column.new(:active), :asc),
    ])

    query = Kwery::Query.new(
      select: {
        id: Kwery::Expr::Column.new(:id)
      },
      from: :users,
      where: [
        Kwery::Expr::Eq.new(Kwery::Expr::Column.new(:name), Kwery::Expr::Literal.new('Cara')),
        Kwery::Expr::Eq.new(Kwery::Expr::Column.new(:active), Kwery::Expr::Literal.new(true)),
      ],
    )

    plan = query.plan(schema)

    expect(plan.explain).to eq(
      [Kwery::Executor::Project,
        [Kwery::Executor::IndexScan, :users_idx_name_active,
          {eq: ['Cara', true]}]]
    )
  end

  it "matches a compound index with reverse field order" do
    schema = Kwery::Schema.new
    schema.create_table(:users)
    schema.create_index(:users, :users_idx_active_name, [
      Kwery::Expr::IndexedExpr.new(Kwery::Expr::Column.new(:active), :asc),
      Kwery::Expr::IndexedExpr.new(Kwery::Expr::Column.new(:name), :asc),
    ])

    query = Kwery::Query.new(
      select: {
        id: Kwery::Expr::Column.new(:id)
      },
      from: :users,
      where: [
        Kwery::Expr::Eq.new(Kwery::Expr::Column.new(:name), Kwery::Expr::Literal.new('Cara')),
        Kwery::Expr::Eq.new(Kwery::Expr::Column.new(:active), Kwery::Expr::Literal.new(true)),
      ],
    )

    plan = query.plan(schema)

    expect(plan.explain).to eq(
      [Kwery::Executor::Project,
        [Kwery::Executor::IndexScan, :users_idx_active_name,
          {eq: [true, 'Cara']}]]
    )
  end

  xit "matches an index prefix" do
    schema = Kwery::Schema.new
    schema.create_table(:users)
    schema.create_index(:users, :users_idx_name_active, [
      Kwery::Expr::IndexedExpr.new(Kwery::Expr::Column.new(:name), :asc),
      Kwery::Expr::IndexedExpr.new(Kwery::Expr::Column.new(:active), :asc),
    ])

    query = Kwery::Query.new(
      select: {
        id: Kwery::Expr::Column.new(:id)
      },
      from: :users,
      where: [
        Kwery::Expr::Eq.new(Kwery::Expr::Column.new(:name), Kwery::Expr::Literal.new('Cara')),
      ],
    )

    plan = query.plan(schema)

    expect(plan.explain).to eq(
      [Kwery::Executor::Project,
        [Kwery::Executor::IndexScan, :users_idx_name_active,
          {eq: ['Cara']}]]
    )
  end

  it "matches a > constraint" do
    schema = Kwery::Schema.new
    schema.create_table(:users)
    schema.create_index(:users, :users_idx_id, [
      Kwery::Expr::IndexedExpr.new(Kwery::Expr::Column.new(:id), :asc),
    ])

    query = Kwery::Query.new(
      select: {
        id: Kwery::Expr::Column.new(:id)
      },
      from: :users,
      where: [
        Kwery::Expr::Gt.new(Kwery::Expr::Column.new(:id), Kwery::Expr::Literal.new(10)),
      ],
    )

    plan = query.plan(schema)

    expect(plan.explain).to eq(
      [Kwery::Executor::Project,
        [Kwery::Executor::IndexScan, :users_idx_id,
          {gt: [10]}]]
    )
  end

  it "matches a > constraint with an index prefix" do
    schema = Kwery::Schema.new
    schema.create_table(:users)
    schema.create_index(:users, :users_idx_active_id, [
      Kwery::Expr::IndexedExpr.new(Kwery::Expr::Column.new(:active), :asc),
      Kwery::Expr::IndexedExpr.new(Kwery::Expr::Column.new(:id), :asc),
    ])

    query = Kwery::Query.new(
      select: {
        id: Kwery::Expr::Column.new(:id)
      },
      from: :users,
      where: [
        Kwery::Expr::Eq.new(Kwery::Expr::Column.new(:active), Kwery::Expr::Literal.new(true)),
        Kwery::Expr::Gt.new(Kwery::Expr::Column.new(:id), Kwery::Expr::Literal.new(10)),
      ],
    )

    plan = query.plan(schema)

    expect(plan.explain).to eq(
      [Kwery::Executor::Project,
        [Kwery::Executor::IndexScan, :users_idx_active_id,
          {eq: [true], gt: [true, 10]}]]
    )
  end

  it "matches a >= constraint" do
    schema = Kwery::Schema.new
    schema.create_table(:users)
    schema.create_index(:users, :users_idx_id, [
      Kwery::Expr::IndexedExpr.new(Kwery::Expr::Column.new(:id), :asc),
    ])

    query = Kwery::Query.new(
      select: {
        id: Kwery::Expr::Column.new(:id)
      },
      from: :users,
      where: [
        Kwery::Expr::Gte.new(Kwery::Expr::Column.new(:id), Kwery::Expr::Literal.new(10)),
      ],
    )

    plan = query.plan(schema)

    expect(plan.explain).to eq(
      [Kwery::Executor::Project,
        [Kwery::Executor::IndexScan, :users_idx_id,
          {gte: [10]}]]
    )
  end

  it "matches a >= constraint with an index prefix" do
    schema = Kwery::Schema.new
    schema.create_table(:users)
    schema.create_index(:users, :users_idx_active_id, [
      Kwery::Expr::IndexedExpr.new(Kwery::Expr::Column.new(:active), :asc),
      Kwery::Expr::IndexedExpr.new(Kwery::Expr::Column.new(:id), :asc),
    ])

    query = Kwery::Query.new(
      select: {
        id: Kwery::Expr::Column.new(:id)
      },
      from: :users,
      where: [
        Kwery::Expr::Eq.new(Kwery::Expr::Column.new(:active), Kwery::Expr::Literal.new(true)),
        Kwery::Expr::Gte.new(Kwery::Expr::Column.new(:id), Kwery::Expr::Literal.new(10)),
      ],
    )

    plan = query.plan(schema)

    expect(plan.explain).to eq(
      [Kwery::Executor::Project,
        [Kwery::Executor::IndexScan, :users_idx_active_id,
          {eq: [true], gte: [true, 10]}]]
    )
  end

  it "matches a < constraint" do
    schema = Kwery::Schema.new
    schema.create_table(:users)
    schema.create_index(:users, :users_idx_id, [
      Kwery::Expr::IndexedExpr.new(Kwery::Expr::Column.new(:id), :asc),
    ])

    query = Kwery::Query.new(
      select: {
        id: Kwery::Expr::Column.new(:id)
      },
      from: :users,
      where: [
        Kwery::Expr::Lt.new(Kwery::Expr::Column.new(:id), Kwery::Expr::Literal.new(10)),
      ],
    )

    plan = query.plan(schema)

    expect(plan.explain).to eq(
      [Kwery::Executor::Project,
        [Kwery::Executor::IndexScan, :users_idx_id,
          {lt: [10]}]]
    )
  end

  it "matches a between constraint" do
    schema = Kwery::Schema.new
    schema.create_table(:users)
    schema.create_index(:users, :users_idx_id, [
      Kwery::Expr::IndexedExpr.new(Kwery::Expr::Column.new(:id), :asc),
    ])

    query = Kwery::Query.new(
      select: {
        id: Kwery::Expr::Column.new(:id)
      },
      from: :users,
      where: [
        Kwery::Expr::Gt.new(Kwery::Expr::Column.new(:id), Kwery::Expr::Literal.new(10)),
        Kwery::Expr::Lt.new(Kwery::Expr::Column.new(:id), Kwery::Expr::Literal.new(15)),
      ],
    )

    plan = query.plan(schema)

    expect(plan.explain).to eq(
      [Kwery::Executor::Project,
        [Kwery::Executor::IndexScan, :users_idx_id,
          {gt: [10], lt: [15]}]]
    )
  end

  it "matches order by and an eq constraint prefix" do
    schema = Kwery::Schema.new
    schema.create_table(:users)
    schema.create_index(:users, :users_idx_name_id, [
      Kwery::Expr::IndexedExpr.new(Kwery::Expr::Column.new(:name), :asc),
      Kwery::Expr::IndexedExpr.new(Kwery::Expr::Column.new(:id), :asc),
    ])

    query = Kwery::Query.new(
      select: {
        id: Kwery::Expr::Column.new(:id)
      },
      from: :users,
      where: [
        Kwery::Expr::Eq.new(Kwery::Expr::Column.new(:name), Kwery::Expr::Literal.new('Cara')),
      ],
      order_by: [
        Kwery::Expr::IndexedExpr.new(Kwery::Expr::Column.new(:id), :asc),
      ],
    )

    plan = query.plan(schema)

    expect(plan.explain).to eq(
      [Kwery::Executor::Project,
        [Kwery::Executor::IndexScan, :users_idx_name_id,
          {eq: ['Cara']}]]
    )
  end

  it "matches eq constraint prefix with gt" do
    schema = Kwery::Schema.new
    schema.create_table(:users)
    schema.create_index(:users, :users_idx_name_id, [
      Kwery::Expr::IndexedExpr.new(Kwery::Expr::Column.new(:name), :asc),
      Kwery::Expr::IndexedExpr.new(Kwery::Expr::Column.new(:id), :asc),
    ])

    query = Kwery::Query.new(
      select: {
        id: Kwery::Expr::Column.new(:id)
      },
      from: :users,
      where: [
        Kwery::Expr::Eq.new(Kwery::Expr::Column.new(:name), Kwery::Expr::Literal.new('Cara')),
        Kwery::Expr::Gt.new(Kwery::Expr::Column.new(:id), Kwery::Expr::Literal.new(10)),
      ],
    )

    plan = query.plan(schema)

    expect(plan.explain).to eq(
      [Kwery::Executor::Project,
        [Kwery::Executor::IndexScan, :users_idx_name_id,
          {eq: ['Cara'], gt: ['Cara', 10]}]]
    )
  end

  it "performs an extra sort if index is only used for matching" do
    schema = Kwery::Schema.new
    schema.create_table(:users)
    schema.create_index(:users, :users_idx_id, [
      Kwery::Expr::IndexedExpr.new(Kwery::Expr::Column.new(:id), :asc),
    ])

    query = Kwery::Query.new(
      select: {
        id: Kwery::Expr::Column.new(:id)
      },
      from: :users,
      where: [
        Kwery::Expr::Eq.new(Kwery::Expr::Column.new(:id), Kwery::Expr::Literal.new(10)),
      ],
      order_by: [
        Kwery::Expr::IndexedExpr.new(Kwery::Expr::Column.new(:name), :asc),
      ],
    )

    plan = query.plan(schema)

    expect(plan.explain).to eq(
      [Kwery::Executor::Project,
        [Kwery::Executor::Sort,
          [Kwery::Executor::IndexScan, :users_idx_id, {eq: [10]}]]]
    )
  end

  it "performs an extra where if index is only used for sorting" do
    schema = Kwery::Schema.new
    schema.create_table(:users)
    schema.create_index(:users, :users_idx_id, [
      Kwery::Expr::IndexedExpr.new(Kwery::Expr::Column.new(:id), :asc),
    ])

    query = Kwery::Query.new(
      select: {
        id: Kwery::Expr::Column.new(:id)
      },
      from: :users,
      where: [
        Kwery::Expr::Eq.new(Kwery::Expr::Column.new(:name), Kwery::Expr::Literal.new('Cara')),
      ],
      order_by: [
        Kwery::Expr::IndexedExpr.new(Kwery::Expr::Column.new(:id), :asc),
      ],
    )

    plan = query.plan(schema)

    expect(plan.explain).to eq(
      [Kwery::Executor::Project,
        [Kwery::Executor::Filter,
          [Kwery::Executor::IndexScan, :users_idx_id, {}]]]
    )
  end
end
